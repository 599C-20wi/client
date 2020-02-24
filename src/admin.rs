use std::io::{BufRead, BufReader};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;

const ADMIN_PORT: u16 = 3001;

pub fn start() {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", ADMIN_PORT)).unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(error) => {
                error!("failed to accept incoming connection: {}", error);
            }
        }
    }
}

fn handle_client(stream: TcpStream) {
    let mut reader = BufReader::new(&stream);
    let mut buffer = Vec::new();
    'read: while match reader.read_until(b'\n', &mut buffer) {
        Ok(size) => {
            trace!("read {} bytes", size);
            if size == 0 {
                break 'read;
            }

            // Read all data except newline character.
            let value = std::str::from_utf8(&buffer[..size - 1]).unwrap();
            let value: i32 = value.parse().expect("Expected to read an int");
            println!("read int from stream: {}", value);
            true
        }
        Err(error) => {
            error!("shutting down stream: {}", error);
            stream.shutdown(Shutdown::Both).unwrap();
            false
        }
    } {}
}
