use std::{collections::HashMap, time::Duration};
use log::{debug, error, info};
use rmodbus::{ModbusProto, client::ModbusRequest};
use tokio::net::TcpStream;

use crate::metering_modbus::{HubConnectionState, ModbusDevice, ModbusError, read_device_parms, registers::{ModbusRegisterType, Register}};

pub async fn set(
    socket_addr: &str,
    hub_name: &str,
    registers: &HashMap<String, Vec<u8>>,
    device: &ModbusDevice,
    proto: ModbusProto,
    conn_state: &mut HubConnectionState,
){
    if !conn_state.retry_ready() { return; }
    // Ensure we have a connection (reuse existing or establish new)
    if conn_state.stream.is_none() {
        match read_device_parms::connect_to_hub(socket_addr, conn_state.connection_timeout).await {
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

    // Get mutable reference to stream
    let stream = conn_state.stream.as_mut().unwrap();

    for (str_addr, value) in registers {
        
        let address: u16 = str_addr.parse().unwrap_or(0);

        for needle in &device.registers {
            
            if let super::registers::Register::Modbus(r) = needle {
                debug!("Searching with register ... {}", r.register);
                if r.register != address {
                    continue;
                }

                debug!("Got register to write ... {address}");

                let mut mreq = ModbusRequest::new(device.config.slave_id, proto);
                mreq.tr_id = super::transport::next_transaction_id();
                let value_u16 = match value.len() {
                    1 => value[0] as u16,
                    2 => u16::from_be_bytes([value[0], value[1]]),
                    _ => {
                        error!("Wrong number of bytes to write");
                        continue;
                    }
                };

                let mut request = Vec::new();

                if let Err(e) = match r.input_type {
                    ModbusRegisterType::Holding => mreq.generate_set_holding(address, value_u16, &mut request),
                    ModbusRegisterType::Coil => mreq.generate_set_coil(address, value[0], &mut request),
                    ModbusRegisterType::Input => {
                        error!("Trying to set input register on hub {} device {} address {address}", hub_name, device.config.name);
                        continue;
                    }
                    ModbusRegisterType::Discrete => {
                        error!("Trying to set discrete register on hub {} device {} address {address}", hub_name, device.config.name);
                        continue;
                    },
                } {
                    error!("Can not build request for {address} on {}: {e:?}", device.config.name);
                    continue;
                }

                /* We run one or multiple times */
                for _  in 0..device.parameters.write_retries {
                    /* Wait if needed */
                    tokio::time::sleep(Duration::from_millis(device.parameters.write_wait_before)).await;
                    match write_single_register(stream, &mreq, &request, conn_state.read_timeout).await {
                        Ok(_) => {
                            info!("Written register {} on Device {} of Hub {}", address, device.config.name, hub_name);
                            tokio::time::sleep(Duration::from_millis(device.parameters.write_wait_after)).await;
                            break;
                        },
                        Err(e) => {
                            error!("Writing register {} on Device {} of Hub {} failed: {e:?}", address, device.config.name, hub_name);
                            conn_state.record_failure();
                            return;
                        },
                    }
                }
            }
        }
    }
}

pub async fn write_register(device: &ModbusDevice, proto: ModbusProto,  conn_state: &mut HubConnectionState, register: &Register, value: Vec<u8> ) {
    if !conn_state.retry_ready() { return; }
    if let Register::Modbus(r) = register {

        let address = r.register;

        let mut mreq = ModbusRequest::new(device.config.slave_id, proto);
        mreq.tr_id = super::transport::next_transaction_id();
        let value_u16 = match value.len() {
            1 => value[0] as u16,
            2 => u16::from_be_bytes([value[0], value[1]]),
            _ => {
                error!("Wrong number of bytes to write");
                return;
            }
        };

        let mut request = Vec::new();

        if let Err(e) = match r.input_type {
            ModbusRegisterType::Holding => mreq.generate_set_holding(address, value_u16, &mut request),
            ModbusRegisterType::Coil => mreq.generate_set_coil(address, value[0], &mut request),
            ModbusRegisterType::Input => {
                error!("Trying to set input register on device {} address {address}", device.config.name);
                return;
            }
            ModbusRegisterType::Discrete => {
                error!("Trying to set discrete register on device {} address {address}", device.config.name);
                return;
            }
        } {
            error!("Can not build request for {address} on {}: {e:?}", device.config.name);
            return;
        }

        if conn_state.stream.is_none() {
            error!("Write before connection");
            return;
        }
        
        /* Get our stream to write to */
        let stream = conn_state.stream.as_mut().unwrap();

        /* We run one or multiple times */
        for _  in 0..device.parameters.write_retries {
            /* Wait if needed */
            tokio::time::sleep(Duration::from_millis(device.parameters.write_wait_before)).await;
            match write_single_register(stream, &mreq, &request, conn_state.read_timeout).await {
                Ok(_) => {
                    info!("Written register {} on Device {}", address, device.config.name);
                    tokio::time::sleep(Duration::from_millis(device.parameters.write_wait_after)).await;
                    break;
                },
                Err(e) => {
                    error!("Writing register {} on Device {} failed: {e:?}", address, device.config.name);
                    conn_state.record_failure();
                    return;
                },
            }
        }
    }
}

async fn write_single_register(
    stream: &mut TcpStream,
    request: &ModbusRequest,
    bytes: &[u8],
    read_timeout: Duration,
) -> Result<(), ModbusError> {
    super::transport::exchange(stream, request, bytes, read_timeout).await?;
    Ok(())
}
