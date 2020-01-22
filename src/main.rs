#[macro_use]
extern crate log;

use std::net::{TcpStream};
use std::io::{Read, Write};
use std::str::from_utf8;

const RESPONSE_SIZE: usize = 6;

fn get_task_addr() -> String {
    String::from("54.183.196.119:3333")
}

fn main() {
    simple_logger::init().unwrap();

    match TcpStream::connect(get_task_addr()) {
        Ok(mut stream) => {
            let msg = b"hello!";
            stream.write(msg).unwrap();

            let mut data = [0 as u8; RESPONSE_SIZE];
            match stream.read_exact(&mut data) {
                Ok(_) => {
                    if &data == msg {
                        trace!("received reply");
                    } else {
                        let text = from_utf8(&data).unwrap();
                        warn!("unexpected reply: {}", text);
                    }
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
