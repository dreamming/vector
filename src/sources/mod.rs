use std::io;
use std::io::Read;

use bytes::{BufMut, BytesMut};
use memchr::memchr;

pub mod console;

pub struct ReaderSource<T> {
    inner: T,
    buf: BytesMut,
}

impl<T: Read> ReaderSource<T> {
    pub fn new(inner: T) -> Self {
        let buf = BytesMut::new();
        Self {
            inner,
            buf,
        }
    }

    pub fn pull(&mut self) -> io::Result<BytesMut> {
        loop {
            if let Some(pos) = memchr(b'\n', &self.buf) {
                let mut line = self.buf.split_to(pos + 1);
                line.split_off(pos);
                return Ok(line);
            } else {
                let n = self.fill_buf()?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "reader closed with no newline",
                    ));
                }
            }
        }
    }

    fn fill_buf(&mut self) -> io::Result<usize> {
        self.buf.reserve(1024 * 10);
        unsafe {
            let n = self.inner.read(self.buf.bytes_mut())?;
            self.buf.advance_mut(n);
            Ok(n)
        }
    }
}