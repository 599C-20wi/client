#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::env;
use std::error;
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process;

use rand::distributions::Uniform;
use rand::seq::IteratorRandom;
use rand::{thread_rng, Rng};

use assigner::hash;
use assigner::message::{Assignment, Get};
use task::face::Expression;
use task::message::{Request, Response};

const BUFFER_SIZE: usize = 256;

const ASSIGNER_ADDRESS: &str = "184.169.220.191:4333";

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

fn generate_expression(
    rng: &mut rand::rngs::ThreadRng,
    distribution: impl rand::distributions::Distribution<i32>,
) -> Result<Expression, GenerationError> {
    match rng.sample(distribution) {
        0 => Ok(Expression::Anger),
        1 => Ok(Expression::Happiness),
        _ => Err(GenerationError),
    }
}

fn main() {
    simple_logger::init().unwrap();

    // Map of expressions -> vector of task servers to handle requests.
    let mut assignments: HashMap<Expression, Vec<String>> = HashMap::new();
    let mut server_index = 0;

    // Cache open TCP streams.
    let mut streams: HashMap<String, TcpStream> = HashMap::new();

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("usage: cargo run <number-of-requests>");
        process::exit(1);
    }
    let repeat: i32 = args[1].parse().unwrap();

    // Load all images in the given directory.
    let path = Path::new("./src/resource/");
    let entries: Vec<std::path::PathBuf> = std::fs::read_dir(path)
        .unwrap()
        .filter(|r| r.is_ok())
        .map(|r| r.unwrap().path())
        .filter(|r| r.extension().is_some())
        .filter(|r| r.extension().unwrap() == "jpg" || r.extension().unwrap() == "jpeg")
        .collect();

    let mut rng = thread_rng();
    let distribution = Uniform::new(0, 2);

    'send: for _ in 0..repeat {
        let expression = match generate_expression(&mut rng, &distribution) {
            Ok(expression) => expression,
            Err(error) => {
                error!("failed to generate expression: {}", error);
                continue;
            }
        };

        // Randomly select an image and read it into memory.
        let image_path = entries.iter().choose(&mut rng).unwrap();
        let mut image_buffer = Vec::new();
        read_file(image_path, &mut image_buffer).expect("failed to read file");

        // Figure out which task server to send request to by looking in cache
        // or asking the assigner if value is not cached.
        if assignments.get(&expression).is_none() {
            debug!("querying assigner for expression {:?}", expression);
            // Send request to assigner.
            let assignment = match TcpStream::connect(ASSIGNER_ADDRESS) {
                Ok(mut stream) => {
                    let get = Get {
                        slice_key: hash::to_slice_key(&expression),
                    };
                    let serialized = get.serialize();
                    stream.write_all(serialized.as_bytes()).unwrap();

                    let mut buffer = [0 as u8; BUFFER_SIZE];
                    match stream.read(&mut buffer) {
                        Ok(size) => {
                            if size == 0 {
                                // Assigner died.
                                return;
                            }

                            match Assignment::deserialize(&buffer[..size]) {
                                Ok(message) => message,
                                Err(e) => {
                                    error!("deserialization failed: {}", e);
                                    std::process::exit(1);
                                }
                            }
                        }
                        Err(e) => {
                            error!("stream read failed: {}", e);
                            continue 'send;
                        }
                    }
                }
                Err(e) => {
                    error!("failed to connect to assigner: {}", e);
                    continue 'send;
                }
            };
            info!(
                "received assignment for {:?}: {:?}",
                expression, assignment.addresses
            );
            assignments.insert(expression.clone(), assignment.addresses);
            server_index = 0;
        }

        let tasks = assignments.get(&expression).unwrap();
        let task = &tasks[server_index];
        server_index += 1;

        if streams.get(task).is_none() {
            // TCP stream not cached, open new connection to task server.
            let stream = match TcpStream::connect(task) {
                Ok(stream) => stream,
                Err(e) => {
                    error!("failed to connect to task server {}: {}", task, e);
                    continue 'send;
                }
            };
            streams.insert(task.to_string(), stream);
        }

        debug!(
            "sending image: {:?} with expression {:?} to {}",
            image_path, expression, task
        );
        let mut stream = streams.get(task).unwrap();
        let message = Request {
            expression,
            image: image_buffer,
        };
        let serialized = message.serialize();
        stream.write_all(serialized.as_bytes()).unwrap();

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
                    Response::Accept { matches_expression } => {
                        info!("received accept response: {:?}", matches_expression);
                    }
                    Response::Reject {
                        error_msg: error,
                        expression,
                    } => {
                        info!(
                            "received reject response: {} for expression {:?}",
                            error, expression
                        );
                    }
                };
            }
            Err(e) => {
                error!("stream read failed: {}", e);
                continue 'send;
            }
        }
    }
}
