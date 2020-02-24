#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::error;
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::thread;

use rand::distributions::Uniform;
use rand::seq::IteratorRandom;
use rand::{thread_rng, Rng};

use assigner::hash;
use assigner::message::{Assignment, Get};
use task::face::Expression;
use task::message::{Request, Response};

mod admin;

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

// Ask the assigner for the task server assigned to hanlde the given expression.
// Returns true on success and false on failure.
fn update_assignments(
    assignments: &mut HashMap<Expression, Vec<String>>,
    expression: &Expression,
    server_index: &mut usize,
) -> bool {
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
                            return false;
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
                        return false;
                    }
                }
            }
            Err(e) => {
                error!("failed to connect to assigner: {}", e);
                return false;
            }
        };
        info!(
            "received assignment for {:?}: {:?}",
            expression, assignment.addresses
        );
        assignments.insert(expression.clone(), assignment.addresses);
        // Reset server index if a new assignment was successfully fetched.
        // server_index = 0;
        *server_index = 0;
    }

    true
}

fn task_reader(stream: &mut TcpStream) {
    let mut buffer = [0 as u8; BUFFER_SIZE];
    'read: while match stream.read(&mut buffer) {
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
                    // update_assignments(&mut assignments, &expression, &mut server_index);
                }
            };

            true
        }
        Err(e) => {
            error!("stream read failed: {}", e);
            continue 'read;
        }
    } {}
}

fn main() {
    simple_logger::init().unwrap();

    // Map of expressions -> vector of task servers to handle requests.
    let mut assignments: HashMap<Expression, Vec<String>> = HashMap::new();
    let mut server_index = 0;

    // Cache open TCP streams.
    let mut streams: HashMap<String, TcpStream> = HashMap::new();

    // Load all images in the given directory.
    let path = Path::new("./src/resource/");
    let entries: Vec<std::path::PathBuf> = std::fs::read_dir(path)
        .unwrap()
        .filter(|r| r.is_ok())
        .map(|r| r.unwrap().path())
        .filter(|r| r.extension().is_some())
        .filter(|r| r.extension().unwrap() == "jpg" || r.extension().unwrap() == "jpeg")
        .collect();

    // Start listening for administrative messages.
    admin::start();

    let mut rng = thread_rng();
    let distribution = Uniform::new(0, 2);

    'send: loop {
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
        if !update_assignments(&mut assignments, &expression, &mut server_index) {
            continue 'send;
        }
        let tasks = assignments.get(&expression).unwrap();
        let task = &tasks[server_index];
        server_index = (server_index + 1) % tasks.len();

        if streams.get(task).is_none() {
            // TCP stream not cached, open new connection to task server.
            debug!("opening new connection to {}", task);
            let stream = match TcpStream::connect(task) {
                Ok(stream) => stream,
                Err(e) => {
                    error!("failed to connect to task server {}: {}", task, e);
                    continue 'send;
                }
            };
            let mut second_stream = stream.try_clone().unwrap();
            streams.insert(task.to_string(), stream);

            thread::spawn(move || {
                task_reader(&mut second_stream);
            });
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
    }
}
