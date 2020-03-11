#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::thread;
use std::time::Instant;

use rand::distributions::{Distribution, WeightedIndex};
use rand::seq::IteratorRandom;
use rand::thread_rng;

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

#[allow(clippy::redundant_clone)]
fn generate_expression() -> Expression {
    let expressions = [Expression::Anger, Expression::Happiness];
    let weights = unsafe { &admin::DISTR_WEIGHTS };
    let dist = WeightedIndex::new(weights).unwrap();
    let mut rng = thread_rng();
    expressions[dist.sample(&mut rng)].clone()
}

// Ask the assigner for the task server assigned to hanlde the given expression.
// Returns true on success and false on failure.
fn update_assignments(
    assignments: &mut HashMap<Expression, Vec<String>>,
    expression: &Expression,
    server_index: &mut HashMap<Expression, usize>,
    start: &mut Instant,
) -> bool {
    if assignments.get(&expression).is_none() || start.elapsed().as_secs() > 10 {
        *start = Instant::now();
        debug!("querying assigner for expression {:?}", expression);
        // Send request to assigner.
        let assignment = match TcpStream::connect(ASSIGNER_ADDRESS) {
            Ok(mut stream) => {
                stream.set_nodelay(true).expect("set_nodelay call failed");
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
        server_index.insert(expression.clone(), 0);
    }

    true
}

fn task_reader(stream: &mut TcpStream) {
    let mut reader = BufReader::new(stream);
    let mut buffer = Vec::new();
    'read: while match reader.read_until(b'\n', &mut buffer) {
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

            buffer.clear();
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
    let mut server_index = HashMap::new();

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

    // Load images into memory.
    let mut images: Vec<Vec<u8>> = Vec::new();
    for entry in entries {
        let mut image_buffer = Vec::new();
        read_file(&entry, &mut image_buffer).unwrap();
        images.push(image_buffer);
    }

    // Start listening for administrative messages.
    thread::spawn(move || {
        admin::start();
    });

    let mut now = Instant::now();

    'send: loop {
        let expression = generate_expression();

        // Select a random image.
        let mut rng = thread_rng();
        let image_buffer = images.iter().choose(&mut rng).unwrap();

        // Figure out which task server to send request to by looking in cache
        // or asking the assigner if value is not cached.
        if !update_assignments(&mut assignments, &expression, &mut server_index, &mut now) {
            continue 'send;
        }
        trace!("assignments={:?}", assignments);
        let tasks = assignments.get(&expression).unwrap();
        trace!("tasks={:?}", tasks);
        trace!("server index={:?}, tasks len={}", server_index, tasks.len());
        let task = &tasks[server_index[&expression]];
        server_index.insert(
            expression.clone(),
            (server_index[&expression] + 1) % tasks.len(),
        );

        if streams.get(task).is_none() {
            // TCP stream not cached, open new connection to task server.
            debug!("opening new connection to {}", task);
            let stream = match TcpStream::connect(task) {
                Ok(stream) => {
                    stream.set_nodelay(true).expect("set_nodelay call failed");
                    stream
                }
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

        debug!("sending image with expression {:?} to {}", expression, task);
        let mut stream = streams.get(task).unwrap();
        let message = Request {
            expression,
            image: image_buffer.to_vec(),
        };
        let serialized = message.serialize();
        stream.write_all(serialized.as_bytes()).unwrap();
    }
}
