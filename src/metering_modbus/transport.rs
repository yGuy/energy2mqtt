//! A TCP read is not a Modbus frame. Consume exactly one bounded response and
//! reject mismatched replies before any register value can be published.
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use rmodbus::{client::ModbusRequest, ModbusProto};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;

use super::ModbusError;

static TRANSACTION_ID: AtomicU16 = AtomicU16::new(1);

pub fn next_transaction_id() -> u16 {
    TRANSACTION_ID.fetch_add(1, Ordering::Relaxed)
}

fn io_error(error: std::io::Error) -> ModbusError {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        ModbusError::ConnectionClosed
    } else {
        ModbusError::IoError(error)
    }
}

async fn read_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    proto: ModbusProto,
) -> Result<Vec<u8>, ModbusError> {
    let (mut frame, length) = match proto {
        ModbusProto::TcpUdp => {
            let mut header = [0; 6];
            stream.read_exact(&mut header).await.map_err(io_error)?;
            let length = u16::from_be_bytes([header[4], header[5]]) as usize;
            if header[2..4] != [0, 0] || !(3..=254).contains(&length) {
                return Err(ModbusError::ProtocolError(
                    "Invalid MBAP header or length".into(),
                ));
            }
            (header.to_vec(), 6 + length)
        }
        ModbusProto::Rtu => {
            let mut header = [0; 3];
            stream.read_exact(&mut header).await.map_err(io_error)?;
            let length = match header[1] {
                0x80..=0xff => 5,
                1..=4 => 5 + header[2] as usize,
                5 | 6 | 15 | 16 => 8,
                _ => {
                    return Err(ModbusError::ProtocolError(
                        "Unsupported RTU function".into(),
                    ))
                }
            };
            if length > 256 {
                return Err(ModbusError::ProtocolError("Oversized RTU frame".into()));
            }
            (header.to_vec(), length)
        }
        ModbusProto::Ascii => {
            return Err(ModbusError::ProtocolError(
                "ASCII framing is not supported".into(),
            ));
        }
    };
    let header_length = frame.len();
    frame.resize(length, 0);
    stream
        .read_exact(&mut frame[header_length..])
        .await
        .map_err(io_error)?;
    Ok(frame)
}

pub async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    request: &ModbusRequest,
    bytes: &[u8],
    read_timeout: Duration,
) -> Result<Vec<u8>, ModbusError> {
    timeout(read_timeout, stream.write_all(bytes))
        .await
        .map_err(|_| ModbusError::WriteTimeout(read_timeout.as_secs()))?
        .map_err(|e| ModbusError::WriteFailed(e.to_string()))?;

    // One deadline covers the entire response, including fragmented headers.
    let frame = timeout(read_timeout, read_frame(stream, request.proto))
        .await
        .map_err(|_| ModbusError::ReadTimeout(read_timeout.as_secs()))??;

    request
        .parse_ok(&frame)
        .map_err(|e| ModbusError::ProtocolError(format!("Invalid response: {e:?}")))?;

    let offset = if request.proto == ModbusProto::TcpUdp {
        6
    } else {
        0
    };
    let function = bytes[offset + 1];
    if (1..=4).contains(&function) {
        let expected = if function <= 2 {
            usize::from(request.count).div_ceil(8)
        } else {
            usize::from(request.count) * 2
        };
        if usize::from(frame[offset + 2]) != expected {
            return Err(ModbusError::ProtocolError(format!(
                "Register {}: expected {expected} data bytes, received {}",
                request.reg,
                frame[offset + 2]
            )));
        }
        let trailer = if request.proto == ModbusProto::Rtu {
            2
        } else {
            0
        };
        if frame.len() != offset + 3 + expected + trailer {
            return Err(ModbusError::ProtocolError(
                "Response byte count does not match frame length".into(),
            ));
        }
    } else if frame.len() < offset + 6 || frame[offset..offset + 6] != bytes[offset..offset + 6] {
        return Err(ModbusError::ProtocolError(
            "Write response does not echo request".into(),
        ));
    }
    Ok(frame)
}
