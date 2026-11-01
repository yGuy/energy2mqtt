use std::time::Duration;

use rmodbus::{client::ModbusRequest, ModbusProto};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use super::{
    read_device_parms::read_hub_devices, registers, transport, HubConnectionState, ModbusDevice,
    ModbusError,
};
use crate::config::{ModbusDeviceConfig, ModbusHubConfig};
use crate::mqtt::Transmission;

fn request(proto: ModbusProto) -> (ModbusRequest, Vec<u8>) {
    let mut request = ModbusRequest::new(1, proto);
    request.tr_id = transport::next_transaction_id();
    let mut bytes = Vec::new();
    request.generate_get_holdings(40093, 2, &mut bytes).unwrap();
    (request, bytes)
}

fn response(request: &[u8], words: &[u16]) -> Vec<u8> {
    let mut frame = request[..4].to_vec();
    frame.extend_from_slice(&(3 + words.len() as u16 * 2).to_be_bytes());
    frame.extend_from_slice(&[1, 3, (words.len() * 2) as u8]);
    for word in words {
        frame.extend_from_slice(&word.to_be_bytes());
    }
    frame
}

async fn exchange_fixture(
    proto: ModbusProto,
    request: ModbusRequest,
    bytes: Vec<u8>,
    reply: Vec<u8>,
    fragment: bool,
) -> Result<Vec<u8>, ModbusError> {
    assert_eq!(proto, request.proto);
    let (mut client, mut server) = tokio::io::duplex(1024);
    let count = bytes.len();
    let task = tokio::spawn(async move {
        let mut input = vec![0; count];
        server.read_exact(&mut input).await.unwrap();
        if fragment {
            for byte in reply {
                server.write_all(&[byte]).await.unwrap();
                tokio::task::yield_now().await;
            }
        } else {
            server.write_all(&reply).await.unwrap();
        }
    });
    let result = transport::exchange(&mut client, &request, &bytes, Duration::from_secs(1)).await;
    task.await.unwrap();
    result
}

#[tokio::test]
async fn fragmented_mbap_header_and_body_are_reassembled() {
    let (req, bytes) = request(ModbusProto::TcpUdp);
    let frame = response(&bytes, &[0x1234, 0x5678]);
    let received = exchange_fixture(req.proto, req, bytes, frame.clone(), true)
        .await
        .unwrap();
    assert_eq!(received, frame);
}

#[tokio::test]
async fn wrong_transaction_and_short_register_responses_are_rejected() {
    for wrong_transaction in [true, false] {
        let (req, bytes) = request(ModbusProto::TcpUdp);
        let mut reply = response(&bytes, if wrong_transaction { &[1, 2] } else { &[1] });
        if wrong_transaction {
            reply[0] ^= 0xff;
        }
        assert!(matches!(
            exchange_fixture(req.proto, req, bytes, reply, false).await,
            Err(ModbusError::ProtocolError(_))
        ));
    }
}

#[tokio::test]
async fn exceptions_and_oversized_frames_are_rejected() {
    for reply_type in [0, 1, 2] {
        let (req, bytes) = request(ModbusProto::TcpUdp);
        let mut reply = bytes[..4].to_vec();
        match reply_type {
            0 => reply.extend_from_slice(&[0, 3, 1, 0x83, 2]),
            1 => reply.extend_from_slice(&[0xff, 0xff]),
            _ => reply.extend_from_slice(&[0, 7, 1, 4, 4, 0, 1, 0, 2]),
        }
        assert!(matches!(
            exchange_fixture(req.proto, req, bytes, reply, false).await,
            Err(ModbusError::ProtocolError(_))
        ));
    }
}

#[tokio::test]
async fn rtu_frames_are_reassembled_and_crc_checked() {
    for corrupt in [false, true] {
        let (req, bytes) = request(ModbusProto::Rtu);
        let mut reply = vec![1, 3, 4, 0x12, 0x34, 0x56, 0x78];
        let crc: u16 = 0x0781; // Modbus CRC for 01 03 04 12 34 56 78
        reply.extend_from_slice(&crc.to_le_bytes());
        if corrupt {
            reply[8] ^= 1;
        }
        let result = exchange_fixture(req.proto, req, bytes, reply.clone(), true).await;
        if corrupt {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap(), reply);
        }
    }
}

#[tokio::test]
async fn incomplete_response_times_out_even_if_the_socket_remains_open() {
    let (req, bytes) = request(ModbusProto::TcpUdp);
    let frame = response(&bytes, &[1, 2]);
    let (mut client, mut server) = tokio::io::duplex(1024);
    let task = tokio::spawn(async move {
        let mut input = [0; 12];
        server.read_exact(&mut input).await.unwrap();
        server.write_all(&frame[..10]).await.unwrap();
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    let result = transport::exchange(&mut client, &req, &bytes, Duration::from_millis(20)).await;
    assert!(matches!(result, Err(ModbusError::ReadTimeout(_))));
    task.abort();
}

fn config() -> ModbusHubConfig {
    serde_yml::from_str("name: test\nhost: 127.0.0.1\nport: 502\nproto: TCP\nretry_backoff_initial_seconds: 1\nretry_backoff_max_seconds: 4\nrestart_after_seconds: 10\n").unwrap()
}

#[tokio::test(start_paused = true)]
async fn backoff_caps_and_restart_timer_resets_only_on_success() {
    let mut state = HubConnectionState::new(&config());
    for seconds in [1, 2, 4, 4] {
        state.record_failure();
        assert!(!state.retry_ready());
        assert_eq!(
            state
                .next_retry
                .unwrap()
                .duration_since(tokio::time::Instant::now()),
            Duration::from_secs(seconds)
        );
        tokio::time::advance(Duration::from_secs(seconds)).await;
        assert!(state.retry_ready());
    }
    assert!(state.restart_due());
    state.record_success();
    assert_eq!(state.consecutive_failures, 0);
    assert!(!state.restart_due());
    assert!(state.retry_ready());
    state.record_failure();
    assert_eq!(
        state
            .next_retry
            .unwrap()
            .duration_since(tokio::time::Instant::now()),
        Duration::from_secs(1)
    );
    let mut disabled = config();
    disabled.restart_after_seconds = 0;
    let mut state = HubConnectionState::new(&disabled);
    state.record_failure();
    tokio::time::advance(Duration::from_secs(100000)).await;
    assert!(!state.restart_due());
}

#[tokio::test]
async fn timed_out_poll_discards_socket_and_recovers_on_a_new_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (mut broken, _) = listener.accept().await.unwrap();
        let mut first = [0; 12];
        broken.read_exact(&mut first).await.unwrap();
        let frame = response(&first, &[0x1234, 0x5678]);
        broken.write_all(&frame[..11]).await.unwrap();
        // Timeout must close the connection, rather than sending another request
        // into the middle of this unfinished frame.
        let mut probe = [0; 12];
        assert_eq!(broken.read(&mut probe).await.unwrap(), 0);
        // Opening a new socket is not recovery if its reply is still bad.
        let (mut still_bad, _) = listener.accept().await.unwrap();
        let mut second = [0; 12];
        still_bad.read_exact(&mut second).await.unwrap();
        still_bad.write_all(&response(&second, &[7])).await.unwrap();
        let (mut recovered, _) = listener.accept().await.unwrap();
        let mut next = [0; 12];
        recovered.read_exact(&mut next).await.unwrap();
        assert_ne!(first[..2], next[..2]);
        recovered
            .write_all(&response(&next, &[0x1234, 0x5678]))
            .await
            .unwrap();
    });
    let register: registers::ModbusRegister = serde_yml::from_str(
        "name: energy\ninput_type: Holding\nregister: 40093\nlength: 2\nformat: UInt32\n",
    )
    .unwrap();
    let mut devices = vec![ModbusDevice {
        config: ModbusDeviceConfig {
            name: "test".into(),
            meter: "test".into(),
            slave_id: 1,
            read_interval: 1,
            defaults: None,
        },
        waits_till_read: 1,
        cur_waits: 1,
        registers: vec![registers::Register::Modbus(register)],
        default: None,
        parameters: Default::default(),
    }];
    let mut state = HubConnectionState::new(&config());
    state.read_timeout = Duration::from_millis(30);
    let (sender, mut receiver) = mpsc::channel(4);
    read_hub_devices(
        &address,
        &mut devices,
        "test",
        ModbusProto::TcpUdp,
        &sender,
        &mut state,
    )
    .await;
    assert!(state.stream.is_none());
    assert_eq!(state.consecutive_failures, 1);
    assert!(
        receiver.try_recv().is_err(),
        "Failed device snapshots must not be published"
    );
    assert_eq!(devices[0].cur_waits, 1, "The failed device must remain due");
    read_hub_devices(
        &address,
        &mut devices,
        "test",
        ModbusProto::TcpUdp,
        &sender,
        &mut state,
    )
    .await;
    assert_eq!(
        state.consecutive_failures, 1,
        "Backoff must prevent another connection attempt"
    );
    let first_failure = state.first_failure;
    state.next_retry = Some(tokio::time::Instant::now());
    read_hub_devices(
        &address,
        &mut devices,
        "test",
        ModbusProto::TcpUdp,
        &sender,
        &mut state,
    )
    .await;
    assert_eq!(
        state.consecutive_failures, 2,
        "A TCP connect must not reset failures"
    );
    assert_eq!(state.first_failure, first_failure);
    assert!(state.stream.is_none());
    assert!(receiver.try_recv().is_err());
    state.next_retry = Some(tokio::time::Instant::now());
    read_hub_devices(
        &address,
        &mut devices,
        "test",
        ModbusProto::TcpUdp,
        &sender,
        &mut state,
    )
    .await;
    assert_eq!(state.consecutive_failures, 0);
    assert!(state.first_failure.is_none());
    let Some(Transmission::Metering(data)) = receiver.recv().await else {
        panic!("Missing recovered poll")
    };
    assert_eq!(data.metered_values["energy"], 0x12345678_u32 as f64);
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

/// Opt-in hardware probe: uses only holding-register reads and collects the
/// production reader's output locally. No MQTT client, HA changes or writes.
#[tokio::test]
#[ignore = "requires ENERGY2MQTT_LIVE_CONFIG and an idle SolarEdge Modbus connection"]
async fn live_solaredge_read_only_probe() {
    let path = std::env::var("ENERGY2MQTT_LIVE_CONFIG").expect("Set ENERGY2MQTT_LIVE_CONFIG");
    let contents = std::fs::read_to_string(path).expect("Cannot read live configuration");
    let cfg: crate::config::Config =
        serde_yml::from_str(&contents).expect("Invalid live configuration");
    let hub = cfg
        .modbus
        .hubs
        .iter()
        .find(|h| h.name == "solaredge")
        .expect("SolarEdge hub missing");
    assert!(hub.proto == crate::config::ModbusProtoConfig::TCP);
    let mut devices = Vec::new();
    for device in &hub.devices {
        assert!(
            device.defaults.as_ref().map_or(true, |v| v.is_empty()),
            "Live probe refuses write defaults"
        );
        assert!(device.meter.starts_with("solaredge/"));
        let (registers, manufacturer, _, _, parameters) = registers::get_registers(&device.meter);
        assert_eq!(manufacturer, "SolarEdge");
        assert!(!registers.is_empty());
        assert!(registers.iter().all(|r| matches!(r, registers::Register::Modbus(r) if r.input_type == registers::ModbusRegisterType::Holding)));
        devices.push(ModbusDevice {
            config: device.clone(),
            waits_till_read: 1,
            cur_waits: 1,
            registers,
            default: None,
            parameters,
        });
    }
    let address = format!("{}:{}", hub.host, hub.port);
    let mut state = HubConnectionState::new(hub);
    let (sender, mut receiver) = mpsc::channel(32);
    for cycle in 1..=3 {
        for device in &mut devices {
            device.cur_waits = 1;
        }
        tokio::time::timeout(
            Duration::from_secs(90),
            read_hub_devices(
                &address,
                &mut devices,
                &hub.name,
                ModbusProto::TcpUdp,
                &sender,
                &mut state,
            ),
        )
        .await
        .expect("Live poll timed out");
        assert_eq!(
            state.consecutive_failures, 0,
            "Live poll failed; no values published to MQTT"
        );
        let mut count = 0;
        while let Ok(message) = receiver.try_recv() {
            if let Transmission::Metering(data) = message {
                count += 1;
                let summary: serde_json::Map<String, serde_json::Value> = data
                    .metered_values
                    .iter()
                    .filter(|(key, _)| {
                        ["ac_power", "power", "charge_percent", "status"].contains(&key.as_str())
                    })
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                println!(
                    "read-only cycle {cycle}: {}: {} fields, {} unavailable; {}",
                    data.meter_name,
                    data.metered_values.len(),
                    data.metered_values.values().filter(|v| v.is_null()).count(),
                    serde_json::Value::Object(summary)
                );
            }
        }
        assert_eq!(count, devices.len());
    }
}
