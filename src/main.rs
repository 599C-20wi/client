#[macro_use]
extern crate log;

use std::net::{TcpStream};
use std::io::{Read, Write};

use task::message::{Request, Response};
use task::translate::{Language};

const BUFFER_SIZE: usize = 256;

fn get_task_addr() -> String {
    String::from("54.183.196.119:3333")
}

fn main() {
    simple_logger::init().unwrap();

    match TcpStream::connect(get_task_addr()) {
        Ok(mut stream) => {
            let message = Request{
                lang: Language::Spanish,
                text: String::from("test"),
            };
            stream.write(&message.serialize()).unwrap();

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
                        Response::Accept{text} => {
                            info!("received accept response: {}", text);
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
