#[macro_use]
extern crate log;

use std::env;
use std::error;
use std::fmt;
use std::fs::{File};
use std::io::{Read, Write};
use std::net::{TcpStream};
use std::path::Path;

use rand::{Rng, thread_rng};
use rand::distributions::Uniform;
use rand::seq::IteratorRandom;

use task::message::{Request, Response};
use task::face::{Expression};

const BUFFER_SIZE: usize = 256;

fn get_task_addr() -> String {
    String::from("54.183.196.119:3333")
}

// Reads the given file into buffer.
fn read_file(filename: &std::path::PathBuf, buffer: &mut Vec<u8>) -> Result<usize, std::io::Error> {
    let mut file = File::open(filename).expect("error opening file");
    file.read_to_end(buffer)
}

// Custom error for expression generation.
#[derive(Debug, Clone)]
struct GenerationError;

impl fmt::Display for GenerationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid match for facial expression")
    }
}

impl error::Error for GenerationError {}

fn generate_expression(rng: &mut rand::rngs::ThreadRng, distribution: impl rand::distributions::Distribution<i32>) -> Result<Expression, GenerationError> {
    match rng.sample(distribution) {
        0 => Ok(Expression::Anger),
        1 => Ok(Expression::Happiness),
        _ => Err(GenerationError),
    }
}

fn main() {
    simple_logger::init().unwrap();

    let args: Vec<String> = env::args().collect();
    let repeat: i32 = args[1].parse().unwrap();

    // Load all images in the given directory.
    let path = Path::new("./src/resource/");
    let entries: Vec<std::path::PathBuf> = std::fs::read_dir(path).unwrap()
        .filter(|r| r.is_ok())
        .map(|r| r.unwrap().path())
        .filter(|r| r.extension().is_some())
        .filter(|r| r.extension().unwrap() == "jpg" || r.extension().unwrap() == "jpeg")
        .collect();

    let mut rng = thread_rng();
    let distribution = Uniform::new(0, 2);

    match TcpStream::connect(get_task_addr()) {
        Ok(mut stream) => {
            for _ in 0..repeat {
                let expression = match generate_expression(&mut rng, &distribution) {
                    Ok(expression) => expression,
                    Err(error) => {
                        error!("failed to generate expression: {}", error);
                        continue;
                    },
                };

                // Randomly select an image and read it into memory.
                let image_path = entries.iter().choose(&mut rng).unwrap();
                let mut image_buffer = Vec::new();
                read_file(image_path, &mut image_buffer).expect("failed to read file");

                debug!("sending image: {:?} with expression {:?}", image_path, expression);
                let message = Request{
                    expression: expression,
                    image: image_buffer,
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
            }
        },
        Err(e) => {
            error!("failed to connect to server: {}", e);
        },
    }
}
