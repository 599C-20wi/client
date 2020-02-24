use std::io::{BufRead, BufReader};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;

const ADMIN_PORT: u16 = 3001;
pub static mut DISTR_WEIGHTS: [u16; 2] = [50, 50];

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
            let raw_value = std::str::from_utf8(&buffer[..size - 1]).unwrap();
            let new_anger_weight: u16 = raw_value.parse().expect("Expected to read an int");
            if new_anger_weight > 100 {
                error!(
                    "recieved invalid anger weight, expected 0 to 100: {}",
                    new_anger_weight
                );
                continue 'read;
            }
            info!("read anger weight: {}", new_anger_weight);
            unsafe {
                DISTR_WEIGHTS[0] = new_anger_weight;
                DISTR_WEIGHTS[1] = 100 - new_anger_weight;
            }
            true
        }
        Err(error) => {
            error!("shutting down stream: {}", error);
            stream.shutdown(Shutdown::Both).unwrap();
            false
        }
    } {}
}
