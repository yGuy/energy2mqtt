
use evalexpr::{ContextWithMutableVariables, DefaultNumericTypes, HashMapContext};
use log::{debug, error, info, warn};
use rmodbus::{client::ModbusRequest, ModbusProto};
use crate::{metering_modbus::{HubConnectionState, ModbusDevice, ModbusError, registers, set_device_parms::write_register, utils::{self, round_number}}, mqtt::{PublishData, Transmission}};
use serde::Serialize;
use tokio::{net::TcpStream, sync::mpsc::Sender, time::timeout};
use std::collections::HashMap;
use std::time::Duration;
use crate::{metering_modbus::registers::Register, models::DeviceProtocol, MeteringData};


/// Establish a connection to a Modbus hub with timeout
pub(crate) async fn connect_to_hub(
    socket_addr: &str,
    connection_timeout: Duration,
) -> Result<TcpStream, ModbusError> {
    match timeout(connection_timeout, TcpStream::connect(socket_addr)).await {
        Ok(Ok(stream)) => {
            let _ = stream.set_nodelay(true);
            Ok(stream)
        }
        Ok(Err(e)) => Err(ModbusError::ConnectionFailed(format!(
            "Failed to connect to {}: {}", socket_addr, e
        ))),
        Err(_) => Err(ModbusError::ConnectionTimeout(connection_timeout.as_secs())),
    }
}

/// Read all devices on a hub using persistent connection
/// Connection is kept alive across read cycles; only reconnects on failure
/// This is important for RTU over TCP where many converters only support one connection
pub async fn read_hub_devices(
    socket_addr: &str,
    devices: &mut [ModbusDevice],
    hub_name: &str,
    proto: ModbusProto,
    hub_sender: &Sender<Transmission>,
    conn_state: &mut HubConnectionState,
) {
    if !conn_state.retry_ready() {
        return;
    }

    // Collect indices of devices that need to be read this cycle
    let devices_to_read: Vec<usize> = devices.iter()
        .enumerate()
        .filter(|(_, d)| d.cur_waits >= d.waits_till_read)
        .map(|(i, _)| i)
        .collect();

    if devices_to_read.is_empty() {
        return;
    }

    // Ensure we have a connection (reuse existing or establish new)
    if conn_state.stream.is_none() {
        match connect_to_hub(socket_addr, conn_state.connection_timeout).await {
            Ok(s) => {
                info!("Hub {}: Connection established", hub_name);
                conn_state.stream = Some(s);
            }
            Err(e) => {
                error!("Hub {}: Failed to establish connection: {:?}", hub_name, e);
                conn_state.record_failure();
                return;
            }
        }
    }

    // Read all devices using the persistent connection
    for idx in devices_to_read {
        let device = &mut devices[idx];

        if let Some(default_list) = &device.default {
            for default in default_list {
                /* We need to set some registers based on the Defaults */
                for default in &default.defaults {
                    match default.function {
                        crate::config::defaults::DefaultFunction::Set => {
                            if default.register.is_none() {
                                error!("A Set default was found without a register, which is wrong");
                                continue;
                            }

                            let rname = default.register.clone().unwrap();
                            for reg in &device.registers {
                                if let Register::Modbus(mreg) = reg {
                                    if mreg.name == rname {
                                        info!("Setting default for {rname} to {}", default.value);
                                        write_register(device, proto, conn_state,
                                                reg, utils::get_data_vec(reg, &default.value)).await;
                                    }
                                }
                            }
                        },
                        crate::config::defaults::DefaultFunction::Sleep => {
                            /* We need to sleep for some time */
                            let timeout: u64 = match default.value.parse() {
                                Ok(t) => t,
                                Err(_) => 1000,
                            };

                            tokio::time::sleep(Duration::from_millis(timeout)).await;
                        },
                    }
                }
            }
            /* Unset the default, we are done here */
            device.default = None;

        }

        debug!("Hub {} Device {} start reading", hub_name, device.config.name);

        // Get mutable reference to stream
        let Some(stream) = conn_state.stream.as_mut() else {
            conn_state.record_failure();
            return;
        };

        match read_device_registers(
            stream,
            device,
            hub_name,
            proto,
            hub_sender,
            conn_state.read_timeout
        ).await {
            Ok(_) => {
                debug!("Hub {} Device {} done reading", hub_name, device.config.name);
                device.cur_waits = 0;
            }
            Err(e) => {
                error!("Hub {} Device {} read failed: {:?}", hub_name, device.config.name, e);
                conn_state.record_failure();

                // A malformed/truncated response may leave unread bytes behind.
                // Drop this stream and retry the failed device on a new connection.
                let delay = conn_state.next_retry.unwrap().saturating_duration_since(tokio::time::Instant::now());
                warn!("Hub {}: retrying on a fresh connection in at least {:.1}s (failure {})",
                    hub_name, delay.as_secs_f64(), conn_state.consecutive_failures);
                return;
            }
        }
    }
    conn_state.record_success();
    // Connection is NOT dropped here - it persists for the next cycle
}

/* Helper defition for the RAW export */
#[derive(Serialize)]
struct E2MRegister {
    //r#type: u32,
    address: i32,
    data: Vec<u8>
}

#[derive(Serialize)]
struct E2MRawData {
    hub: String,
    device: String,
    registers: Vec<E2MRegister>,
}

/// Read registers from a single device using an existing connection
pub async fn read_device_registers(
    stream: &mut TcpStream,
    device: &mut ModbusDevice,
    hub_name: &str,
    proto: ModbusProto,
    hub_sender: &Sender<Transmission>,
    read_timeout: Duration,
) -> Result<(), ModbusError> {
    let mut meter_data = MeteringData::new().unwrap();
    meter_data.meter_name = device.config.name.clone();
    meter_data.protocol = DeviceProtocol::ModbusTCP;
    meter_data.id = crate::get_id("modbus".to_string(), &device.config.name);
    meter_data.transmission_time = crate::get_unix_ts();
    meter_data.metered_time = meter_data.transmission_time;

    let mut context = HashMapContext::<DefaultNumericTypes>::new();
    // Store SunSpec scale factors for later application
    let mut scale_factors: HashMap<String, i16> = HashMap::new();

    /* We want to send the raw data to mqtt */
    let mut raw_data = E2MRawData {
        hub: hub_name.to_string(),
        device: device.config.name.clone(),
        registers: Vec::new()
    };

    for reg in &device.registers {
        let reg = match reg {
            Register::Template(_) => continue,
            Register::Modbus(modbus_register) => modbus_register,
        };

        if reg.ignore {
            continue;
        }

        debug!("Hub {} Device {} Register {} start reading", hub_name, device.config.name, reg.name);

        let mut mreq = ModbusRequest::new(device.config.slave_id, proto);
        mreq.tr_id = super::transport::next_transaction_id();
        let mut request = Vec::new();

        match reg.input_type {
            registers::ModbusRegisterType::Holding => {
                mreq.generate_get_holdings(reg.register, reg.length, &mut request).map_err(|e| ModbusError::ProtocolError(format!("Invalid register {}: {e:?}", reg.name)))?;
            }
            registers::ModbusRegisterType::Input => {
                mreq.generate_get_inputs(reg.register, reg.length, &mut request).map_err(|e| ModbusError::ProtocolError(format!("Invalid register {}: {e:?}", reg.name)))?;
            }
            registers::ModbusRegisterType::Coil => {
                mreq.generate_get_coils(reg.register, reg.length, &mut request).map_err(|e| ModbusError::ProtocolError(format!("Invalid register {}: {e:?}", reg.name)))?;
            },
            registers::ModbusRegisterType::Discrete => {
                mreq.generate_get_discretes(reg.register, reg.length, &mut request).map_err(|e| ModbusError::ProtocolError(format!("Invalid register {}: {e:?}", reg.name)))?
            },
        }


        let response = super::transport::exchange(stream, &mreq, &request, read_timeout).await?;

        // Process the response - use f64 to handle all numeric types
        let parsed_value: Result<f64, String>;
        let mut string_value: Option<String> = None;
        let mut integer_value: Option<u64> = None;

        raw_data.registers.push( E2MRegister { address: reg.register as i32, data: response.clone() });

        // Outside callers may want to get raw data without populating our internal messages
        // with useless information. That can happen if someone wants to get debug messages from
        // a battery.
        if reg.name.starts_with("_") {
            continue;
        }

        match reg.format {
            registers::ModbusRegisterFormat::Bool | registers::ModbusRegisterFormat::Coil => {
                let mut data = Vec::new();
                match mreq.parse_bool(&response, &mut data) {
                    Ok(()) => {
                        parsed_value = Ok(match data[0] {
                            true => 1,
                            false => 0
                        } as f64);
                    }
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    },
                }
            },
            registers::ModbusRegisterFormat::Int32
            | registers::ModbusRegisterFormat::UInt32
            | registers::ModbusRegisterFormat::UInt64
            | registers::ModbusRegisterFormat::Float32 => {
                let mut data = Vec::new();
                parsed_value = mreq
                    .parse_u16(&response, &mut data)
                    .map_err(|e| format!("{:?}", e))
                    .and_then(|()| {
                        let length = if reg.format == registers::ModbusRegisterFormat::UInt64 {
                            4
                        } else {
                            2
                        };
                        let bits = utils::decode_register_words(&data, length, reg.word_order)?;
                        Ok(match reg.format {
                            registers::ModbusRegisterFormat::Int32 => (bits as i32) as f64,
                            registers::ModbusRegisterFormat::Float32 => f32::from_bits(bits as u32) as f64,
                            registers::ModbusRegisterFormat::UInt64 => {
                                integer_value = Some(bits);
                                bits as f64
                            },
                            _ => bits as f64,
                        })
                    });
            },
            registers::ModbusRegisterFormat::Int16 => {
                let mut data = Vec::new();
                match mreq.parse_i16(&response, &mut data) {
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    }
                    Ok(()) => {
                        parsed_value = Ok(data[0] as f64);
                    }
                }
            },
            registers::ModbusRegisterFormat::UInt16 => {
                let mut data = Vec::new();
                match mreq.parse_u16(&response, &mut data) {
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    }
                    Ok(()) => {
                        parsed_value = Ok(data[0] as f64);
                    }
                }
            },
            registers::ModbusRegisterFormat::HexString => {
                let mut data = Vec::new();
                match mreq.parse_u16(&response, &mut data) {
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    }
                    Ok(()) => {
                        // Convert u16 registers to a hex string
                        let mut data_u8 = Vec::new();
                        for reg in data {
                            data_u8.push((reg >> 8) as u8 & 0xFF);
                            data_u8.push(reg as u8 & 0xFF);
                        }

                        string_value = Some(hex::encode(data_u8));
                        parsed_value = Ok(0.0); // Placeholder, we use string_value
                    }
                }
            },
            registers::ModbusRegisterFormat::String => {
                let mut data = Vec::new();
                 match mreq.parse_u16(&response, &mut data) {
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    }
                    Ok(()) => {
                        // Convert u16 registers to string (2 chars per register, big-endian)
                        let mut chars = Vec::new();
                        for word in &data {
                            let hi = ((*word >> 8) & 0xFF) as u8;
                            let lo = (*word & 0xFF) as u8;
                            if hi != 0 { chars.push(hi); }
                            if lo != 0 { chars.push(lo); }
                        }
                        string_value = Some(String::from_utf8_lossy(&chars).trim_end_matches('\0').trim().to_string());
                        parsed_value = Ok(0.0); // Placeholder, we use string_value
                    }
                }
            },
            registers::ModbusRegisterFormat::SunSSF => {
                // SunSpec scale factor: int16 representing power of 10 exponent
                let mut data = Vec::new();
                match mreq.parse_i16(&response, &mut data) {
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    }
                    Ok(()) => {
                        let sf = data[0];
                        debug!("Hub {} Device {}: Decoded scale factor {} = {}",
                               hub_name, device.config.name, reg.name, sf);
                        parsed_value = Ok(sf as f64);
                    }
                }
            },
        }

        if let Err(e) = &parsed_value {
            return Err(ModbusError::ProtocolError(format!(
                "Register {}: {}", reg.name, e
            )));
        }

        // Handle string values separately
        if let Some(s) = string_value {
            meter_data.metered_values.insert(reg.name.clone(), serde_json::Value::from(s.clone()));
            let _ = context.set_value(reg.name.clone(), evalexpr::Value::String(s));
            continue;
        }

        let raw_value = parsed_value.unwrap();
        let invalid = !raw_value.is_finite() || reg.invalid_values.iter().any(|invalid| {
            if let Some(integer) = integer_value {
                invalid.as_u64() == Some(integer)
            } else {
                invalid.as_f64() == Some(raw_value)
            }
        });
        // Invalid scale factors must not be cached or used as exponents.
        if reg.format == registers::ModbusRegisterFormat::SunSSF {
            if !invalid && (-10.0..=10.0).contains(&raw_value) {
                scale_factors.insert(reg.name.clone(), raw_value as i16);
            }
            continue;
        }
        if invalid {
            debug!("Hub {} Device {}: register {} reports an invalid/unimplemented value",
                hub_name, device.config.name, reg.name);
            meter_data.metered_values.insert(reg.name.clone(), serde_json::Value::Null);
            continue;
        }

        // Apply scale factor: either from referenced SunSSF register or from static scaler
        let scaled_value = if let Some(ref sf_name) = reg.scale_factor {
            if let Some(&sf) = scale_factors.get(sf_name) {
                // SunSpec scale factor: value * 10^sf
                raw_value * 10_f64.powi(sf as i32)
            } else {
                debug!("Hub {} Device {}: scale factor {} unavailable for {}",
                      hub_name, device.config.name, sf_name, reg.name);
                meter_data.metered_values.insert(reg.name.clone(), serde_json::Value::Null);
                continue;
            }
        } else {
            // Use static scaler
            raw_value * reg.scaler as f64
        };

        {

            let v = round_number(scaled_value, reg.precision);
            if !v.is_finite() {
                meter_data.metered_values.insert(reg.name.clone(), serde_json::Value::Null);
                continue;
            }
            // Keep unscaled 64-bit counters exact in JSON, including above 2^53.
            // Expressions still use the existing floating-point context.
            let mut value = match integer_value {
                Some(integer)
                    if reg.scale_factor.is_none()
                        && reg.scaler == 1.0
                        && reg.mappings.is_empty() =>
                {
                    serde_json::Value::from(integer)
                },
                _ => serde_json::Value::from(v),
            };

            let mut found = false;
            for mapping in reg.mappings.iter() {
                info!("mapping {:?} with {:?}", mapping.data, v);
                if mapping.data == format!("{:?}", v) {
                    value = mapping.mapping.clone();
                    found = true;
                    break;
                }
            }

            if !found {
                for mapping in reg.mappings.iter() {
                    if mapping.data == "_" {
                        value = mapping.mapping.clone();
                        break;
                    }
                }
            }

            meter_data.metered_values.insert(reg.name.clone(), value);
            let _ = context.set_value(reg.name.clone(), evalexpr::Value::Float(v as f64));
        }
    }

    // Calculate template registers if needed
    for reg in &device.registers {
        let reg = match reg {
            Register::Template(template_register) => template_register,
            Register::Modbus(_) => continue,
        };

        let value = match evalexpr::eval_float_with_context(&reg.value, &context) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to evaluate: {e:?}");
                meter_data.metered_values.insert(reg.name.clone(), serde_json::Value::Null);
                continue;
            },
        };

        let value = round_number(value, reg.precision);
        meter_data.metered_values.insert(reg.name.clone(), serde_json::Value::from(value));
        let _ = context.set_value(reg.name.clone(), evalexpr::Value::Float(value as f64));
    }

    let _ = hub_sender.send(Transmission::Metering(meter_data)).await;

    /* Now publish the raw data. In that mode we work as transparent bridge for the data to flow */
    let p = PublishData {
        topic: format!("energy2mqtt/raw/modbus/{}", hub_name),
        payload: serde_json::to_string(&raw_data).unwrap_or("{}".to_string()),
        qos: 1,
        retain: false,
    };

    let _ = hub_sender.send(Transmission::Publish(p)).await;

    Ok(())
}
