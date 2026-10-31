use super::read_device_parms::read_device_registers;
use super::registers::{self, ModbusRegisterFormat, Register, WordOrder};
use super::utils::decode_register_words;
use super::ModbusDevice;
use crate::config::ModbusDeviceConfig;
use crate::mqtt::Transmission;
use rmodbus::ModbusProto;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

#[test]
fn word_order_preserves_modbus_byte_order() {
    assert_eq!(
        decode_register_words(&[0x1234, 0x5678], 2, WordOrder::HighFirst).unwrap(),
        0x12345678
    );
    assert_eq!(
        decode_register_words(&[0x5678, 0x1234], 2, WordOrder::LowFirst).unwrap(),
        0x12345678
    );
    assert_eq!(
        decode_register_words(&[0x7788, 0x5566, 0x3344, 0x1122], 4, WordOrder::LowFirst).unwrap(),
        0x1122334455667788
    );
    assert_eq!(
        decode_register_words(&[0xffff; 4], 4, WordOrder::HighFirst).unwrap(),
        u64::MAX
    );
    assert!(decode_register_words(&[1], 2, WordOrder::LowFirst).is_err());
    assert!(decode_register_words(&[1; 4], 5, WordOrder::HighFirst).is_err());
}

#[test]
fn definitions_have_ordered_signed_scale_factors_and_valid_lengths() {
    for (model, count) in [("inverter", 40), ("meter", 79), ("battery", 24)] {
        let (registers, manufacturer, _, _, _) =
            registers::get_registers(&format!("solaredge/{model}"));
        assert_eq!(manufacturer, "SolarEdge");
        assert_eq!(registers.len(), count);
        let mut scales = Vec::new();
        let mut names = Vec::new();
        for register in registers {
            let Register::Modbus(register) = register else {
                panic!("Unexpected template")
            };
            assert!(!names.contains(&register.name));
            names.push(register.name.clone());
            assert!(register.register as u32 + register.length as u32 <= 65536);
            assert_eq!(register.platform, "sensor");
            if register.format == ModbusRegisterFormat::SunSSF {
                assert_eq!(register.length, 1);
                assert!(!register.name.starts_with('_'));
                scales.push(register.name);
            } else if let Some(scale) = register.scale_factor {
                assert!(
                    scales.contains(&scale),
                    "Scale factor must precede its measurement"
                );
            }
        }
    }
}

fn string(words: &mut BTreeMap<u16, u16>, address: u16, value: &str) {
    let mut bytes = value.as_bytes().to_vec();
    if bytes.len() % 2 != 0 {
        bytes.push(0);
    }
    for (index, pair) in bytes.chunks_exact(2).enumerate() {
        words.insert(
            address + index as u16,
            u16::from_be_bytes([pair[0], pair[1]]),
        );
    }
}

fn low_float(words: &mut BTreeMap<u16, u16>, address: u16, value: f32) {
    let bits = value.to_bits();
    words.insert(address, bits as u16);
    words.insert(address + 1, (bits >> 16) as u16);
}

// Exercise the real YAML loader, Modbus request/response parser, scaling and
// published MeteringData against a local TCP server. No inverter or broker needed.
async fn read_fixture(
    model: &str,
    words: BTreeMap<u16, u16>,
) -> serde_json::Map<String, serde_json::Value> {
    let (registers, _, _, _, parameters) = registers::get_registers(&format!("solaredge/{model}"));
    read_register_fixture(model, registers, parameters, words).await
}

async fn read_register_fixture(
    model: &str,
    registers: Vec<Register>,
    parameters: registers::ModbusDeviceParameters,
    words: BTreeMap<u16, u16>,
) -> serde_json::Map<String, serde_json::Value> {
    assert!(!registers.is_empty());
    let requests: Vec<_> = registers
        .iter()
        .filter_map(|register| match register {
            Register::Modbus(r) => Some((r.register, r.length)),
            _ => None,
        })
        .collect();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for (address, length) in requests {
            let mut request = [0; 12];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(request[6], 1, "All three devices use unit 1");
            assert_eq!(request[7], 3, "Only holding-register reads are allowed");
            assert_eq!(u16::from_be_bytes([request[8], request[9]]), address);
            assert_eq!(u16::from_be_bytes([request[10], request[11]]), length);
            let mut response = request[..4].to_vec();
            response.extend_from_slice(&(3 + length * 2).to_be_bytes());
            response.extend_from_slice(&[1, 3, (length * 2) as u8]);
            for index in 0..length {
                response.extend_from_slice(
                    &words
                        .get(&(address + index))
                        .copied()
                        .unwrap_or(0)
                        .to_be_bytes(),
                );
            }
            stream.write_all(&response).await.unwrap();
        }
    });
    let mut device = ModbusDevice {
        config: ModbusDeviceConfig {
            name: format!("test_{model}"),
            meter: format!("solaredge/{model}"),
            slave_id: 1,
            read_interval: 10,
            defaults: None,
        },
        waits_till_read: 1,
        cur_waits: 0,
        registers,
        default: None,
        parameters,
    };
    let (sender, mut receiver) = mpsc::channel(4);
    let mut stream = TcpStream::connect(address).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        read_device_registers(
            &mut stream,
            &mut device,
            "test",
            ModbusProto::TcpUdp,
            &sender,
            Duration::from_secs(2),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap();
    let Some(Transmission::Metering(data)) = receiver.recv().await else {
        panic!("Missing metering data")
    };
    assert_eq!(data.meter_name, format!("test_{model}"));
    assert!(!data.metered_values.keys().any(|key| key.ends_with("_sf")));
    data.metered_values
}

#[tokio::test]
async fn inverter_uses_zero_based_addresses_and_signed_scales() {
    let mut words = BTreeMap::from([
        (40069, 103),
        (40075, (-2_i16) as u16),
        (40082, (-1_i16) as u16),
        (40086, (-2_i16) as u16),
        (40092, (-2_i16) as u16),
        (40095, (-1_i16) as u16),
        (40106, (-1_i16) as u16),
        (40071, 1234),
        (40079, 2301),
        (40083, (-500_i16) as u16),
        (40085, 5000),
        (40091, 9850),
        (40093, 0x1234),
        (40094, 0x5678),
        (40103, 451),
        (40107, 4),
    ]);
    string(&mut words, 40004, "SolarEdge");
    string(&mut words, 40052, "INVERTER123");
    let values = read_fixture("inverter", words).await;
    assert_eq!(values.len(), 28);
    assert_eq!(values["manufacturer"], "SolarEdge");
    assert_eq!(values["serial_number"], "INVERTER123");
    assert_eq!(values["type"], 103.0);
    assert_eq!(values["ac_current"], 12.34);
    assert_eq!(values["ac_voltage_p1"], 230.1);
    assert_eq!(values["ac_power"], -500.0);
    assert_eq!(values["ac_frequency"], 50.0);
    assert_eq!(values["ac_power_factor"], 98.5);
    assert_eq!(values["ac_total_energy_produced"], 30541989.6);
    assert_eq!(values["heat_sink_temperature"], 45.1);
    assert_eq!(values["status"], 4.0);
}

#[tokio::test]
async fn meter_one_reads_signed_power_and_both_energy_directions() {
    let mut words = BTreeMap::from([
        (40188, 203),
        (40194, (-2_i16) as u16),
        (40203, (-1_i16) as u16),
        (40205, (-2_i16) as u16),
        (40191, (-125_i16) as u16),
        (40196, 2301),
        (40204, 5000),
        (40206, (-1234_i16) as u16),
        (40226, 1),
        (40227, 0),
        (40234, 2),
        (40235, 1),
        (40294, 4),
    ]);
    string(&mut words, 40123, "SolarEdge");
    string(&mut words, 40171, "WRONG-SERIAL");
    let values = read_fixture("meter", words).await;
    assert_eq!(values.len(), 69);
    assert_eq!(values["manufacturer"], "SolarEdge");
    assert_eq!(values["serial_number"], "WRONG-SERIAL");
    assert_eq!(values["type"], 203.0);
    assert_eq!(values["ac_current_p1"], -1.25);
    assert_eq!(values["ac_voltage_p1"], 230.1);
    assert_eq!(values["ac_frequency"], 50.0);
    assert_eq!(values["ac_power"], -1234.0);
    assert_eq!(values["exported_energy"], 65536.0);
    assert_eq!(values["imported_energy"], 131073.0);
    assert_eq!(values["events"], 4.0);
}

#[tokio::test]
async fn battery_one_reads_low_word_first_floats_and_exact_64_bit_counters() {
    let mut words = BTreeMap::from([
        // 2^53 + 17, which cannot be represented exactly as an f64.
        (57718, 17),
        (57719, 0),
        (57720, 0),
        (57721, 0x20),
        (57722, 0x5678),
        (57723, 0x1234),
        (57724, 0),
        (57725, 0),
        (57734, 4),
        (57735, 0),
        (57736, 0x5678),
        (57737, 0x1234),
        (57738, 42),
    ]);
    string(&mut words, 57600, "Battery Vendor");
    string(&mut words, 57648, "BATTERY123");
    low_float(&mut words, 57666, 10000.0);
    low_float(&mut words, 57708, 25.5);
    low_float(&mut words, 57712, 400.25);
    low_float(&mut words, 57714, -3.125);
    low_float(&mut words, 57716, -1250.0);
    low_float(&mut words, 57726, 9500.0);
    low_float(&mut words, 57728, 4750.0);
    low_float(&mut words, 57730, 95.0);
    low_float(&mut words, 57732, 50.0);
    let values = read_fixture("battery", words).await;
    assert_eq!(values.len(), 24);
    assert_eq!(values["manufacturer"], "Battery Vendor");
    assert_eq!(values["serial_number"], "BATTERY123");
    assert_eq!(values["rated_capacity"], 10000.0);
    assert_eq!(values["avg_temperature"], 25.5);
    assert_eq!(values["voltage"], 400.25);
    assert_eq!(values["current"], -3.125);
    assert_eq!(values["power"], -1250.0);
    assert_eq!(
        values["lifetime_exported_energy"].as_u64(),
        Some((1_u64 << 53) + 17)
    );
    assert_eq!(
        values["lifetime_imported_energy"].as_u64(),
        Some(0x12345678)
    );
    assert_eq!(values["capacity"], 9500.0);
    assert_eq!(values["charge"], 4750.0);
    assert_eq!(values["capacity_percent"], 95.0);
    assert_eq!(values["charge_percent"], 50.0);
    assert_eq!(values["status"], 4.0);
    assert_eq!(values["vendor_status"], 0x12345678_u32 as f64);
    assert_eq!(values["last_event"], 42.0);
}

#[tokio::test]
async fn existing_numeric_formats_default_to_high_word_first() {
    let registers: Vec<registers::ModbusRegister> = serde_yml::from_str(r#"
- {name: signed, input_type: Holding, register: 10, length: 2, format: Int32}
- {name: unsigned, input_type: Holding, register: 12, length: 2, format: UInt32}
- {name: float, input_type: Holding, register: 14, length: 2, format: Float32}
- {name: signed_low, input_type: Holding, register: 16, length: 2, format: Int32, word_order: LowFirst}
"#).unwrap();
    let bits = (-12.5_f32).to_bits();
    let words = BTreeMap::from([
        (10, 0xfffe),
        (11, 0xffff),
        (12, 0x1234),
        (13, 0x5678),
        (14, (bits >> 16) as u16),
        (15, bits as u16),
        (16, 0xffff),
        (17, 0xfffe),
    ]);
    let values = read_register_fixture(
        "existing",
        registers.into_iter().map(Register::Modbus).collect(),
        Default::default(),
        words,
    )
    .await;
    assert_eq!(values["signed"], -65537.0);
    assert_eq!(values["unsigned"], 0x12345678_u32 as f64);
    assert_eq!(values["float"], -12.5);
    assert_eq!(values["signed_low"], -65537.0);
}

#[tokio::test]
async fn scale_factors_are_not_discovered_as_home_assistant_sensors() {
    use crate::mqtt::home_assistant::HaSensor;
    for (model, count) in [("inverter", 28), ("meter", 69), ("battery", 24)] {
        let (registers, manufacturer, description, _, _) =
            registers::get_registers(&format!("solaredge/{model}"));
        let mut discovery = HaSensor::new(
            "ModbusTCP".into(),
            "stable_name".into(),
            Some(manufacturer),
            Some(description),
        );
        let (commands, _) = mpsc::channel(1);
        let (transmissions, _) = mpsc::channel(256);
        for register in registers {
            super::ha_config::get_cmp_from_reg(
                register,
                &mut discovery,
                &commands,
                &transmissions,
                &"solaredge".into(),
                &"stable_name".into(),
            )
            .await;
        }
        assert_eq!(discovery.get_entity_discoveries().len(), count);
    }
}

#[test]
fn example_config_shares_one_connection_and_polls_every_ten_seconds() {
    let config: crate::config::Config =
        serde_yml::from_str(include_str!("../../examples/solaredge.yaml")).unwrap();
    assert_eq!(config.modbus.hubs.len(), 1);
    let hub = &config.modbus.hubs[0];
    assert_eq!(hub.port, 502);
    assert!(hub.proto == crate::config::ModbusProtoConfig::TCP);
    assert_eq!(hub.devices.len(), 3);
    for device in &hub.devices {
        assert_eq!(device.slave_id, 1);
        assert_eq!(device.read_interval, 10);
    }
}
