use std::thread;

use super::ReaderSource;
use transport::Log;

pub struct Source {
    log: Log,
}

impl Source {
    pub fn new(log: Log) -> Self {
        Source { log }
    }

    pub fn run(mut self) -> thread::JoinHandle<u64> {
        thread::spawn(move || {
            let mut offset = 0;
            let reader = ::std::io::stdin();
            let buffer = reader.lock();
            let mut source = ReaderSource::new(buffer);
            while let Ok(msg) = source.pull() {
                self.log.append(&[&msg]).expect("failed to append input");
                offset += 1;
            }
            offset
        })
    }
}

#[cfg(test)]
mod test {
    use super::ReaderSource;
    use std::io::{self, Cursor};

    #[test]
    fn reader_source_works() {
        let src = Cursor::new("hello world\n".repeat(10));
        let mut rdr = ReaderSource::new(src);
        for _ in 0..10 {
            assert_eq!(rdr.pull().unwrap(), "hello world");
        }
    }

    #[test]
    fn reader_source_works_for_really_long_lines() {
        let line = "a".repeat(1024 * 100);
        let src = Cursor::new(format!("{}\n", line));
        let mut rdr = ReaderSource::new(src);
        assert_eq!(rdr.pull().unwrap(), line);
    }

    #[test]
    fn reader_source_returns_eof_if_no_newline() {
        let line = "a".repeat(1024 * 100);
        let src = Cursor::new(format!("{}", line));
        let mut rdr = ReaderSource::new(src);
        assert_eq!(rdr.pull().unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }
}
