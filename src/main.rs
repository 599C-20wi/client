#[macro_use]
extern crate log;

use std::fs::File;
use std::net::{TcpStream};
use std::io::{Read, Write};

use task::message::{Request, Response};
use task::face::{Expression};

const BUFFER_SIZE: usize = 256;

fn get_task_addr() -> String {
    String::from("54.183.196.119:3333")
}

// Reads the given file into buffer.
fn read_file(filename: &str, buffer: &mut Vec<u8>) -> Result<usize, std::io::Error> {
    let mut file = File::open(filename).expect("error opening file");
    file.read_to_end(buffer)
}

fn main() {
    simple_logger::init().unwrap();

    let mut buffer = Vec::new();
    read_file("src/resource/rainier.jpeg", &mut buffer).expect("failed to read file");

    match TcpStream::connect(get_task_addr()) {
        Ok(mut stream) => {
            let message = Request{
                expression: Expression::Anger,
                image: buffer,
            };
            let serialized = message.serialize();
            stream.write(serialized.as_bytes()).unwrap();

            let mut buffer = [0 as u8; BUFFER_SIZE];
            match stream.read(&mut buffer) {
                Ok(size) => {
                    if size == 0 {
                        // Server died.
                        return;
                    }

                    let response = match Response::deserialize(&buffer[..size]) {
                        Ok(message) => message,
                        Err(e) => {
                            error!("deserialization failed: {}", e);
                            std::process::exit(1);
                        }
                    };

                    match response {
                        Response::Accept{matches_expression} => {
                            info!("received accept response: {:?}", matches_expression);
                        },
                        Response::Reject{error} => {
                            info!("received reject response: {}", error);
                        },
                    };
                },
                Err(e) => {
                    println!("{}", e);
                    error!("stream read failed: {}", e);
                },
            }
        },
        Err(e) => {
            error!("failed to connect to server: {}", e);
        },
    }
}
