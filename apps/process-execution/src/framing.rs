use std::io;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

pub async fn read(reader: &mut (impl AsyncBufRead + Unpin)) -> io::Result<Vec<u8>> {
    let mut frame = Vec::new();
    reader
        .take((MAX_FRAME_BYTES + 1) as u64)
        .read_until(b'\n', &mut frame)
        .await?;
    validate(&frame)?;
    Ok(frame)
}

pub fn validate(frame: &[u8]) -> io::Result<()> {
    if frame.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "no RPC frame received",
        ));
    }
    if frame.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RPC frame exceeds 8 MiB",
        ));
    }
    Ok(())
}

pub async fn write(writer: &mut (impl AsyncWrite + Unpin), frame: &[u8]) -> io::Result<()> {
    if frame.len() >= MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RPC frame exceeds 8 MiB",
        ));
    }
    writer.write_all(frame).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reads_one_frame_at_a_time() {
        let mut input = b"{\"one\":1}\n{\"two\":2}\n".as_slice();
        assert_eq!(read(&mut input).await.unwrap(), b"{\"one\":1}\n");
        assert_eq!(read(&mut input).await.unwrap(), b"{\"two\":2}\n");
        assert_eq!(
            read(&mut input).await.unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[tokio::test]
    async fn rejects_oversized_input_without_reading_the_rest() {
        let bytes = vec![b'x'; MAX_FRAME_BYTES + 100];
        let mut input = bytes.as_slice();
        assert_eq!(
            read(&mut input).await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(input.len(), 99);
    }
}
