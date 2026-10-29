//! Helpers for HTTP servers used by gateway integration tests.

use std::io;

/// Reads one request head and the complete body named by its `Content-Length` header.
pub(crate) fn read_request(stream: &mut impl io::Read) -> io::Result<(String, Vec<u8>)> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    let header_end = loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before the request head arrived",
            ));
        }
        request.extend_from_slice(chunk.get(..read).unwrap_or_default());
        if let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break end.saturating_add(4);
        }
    };
    let header = String::from_utf8_lossy(request.get(..header_end.saturating_sub(4)).unwrap_or_default());
    let content_length = content_length(&header)?;
    let buffered_body = request.get(header_end..).unwrap_or_default();
    let buffered_length = buffered_body.len().min(content_length);
    let mut body = Vec::with_capacity(content_length);
    body.extend_from_slice(buffered_body.get(..buffered_length).unwrap_or_default());
    body.resize(content_length, 0);
    stream.read_exact(body.get_mut(buffered_length..).unwrap_or_default())?;
    Ok((header.into_owned(), body))
}

/// Returns the request body's declared length, or zero when the header is absent.
fn content_length(header: &str) -> io::Result<usize> {
    let content_length = header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then_some(value.trim())
        })
        .map(str::parse::<usize>)
        .transpose()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
        .unwrap_or(0);
    Ok(content_length)
}
