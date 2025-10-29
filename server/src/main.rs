use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use protocol::Command;
use thread_pool::ThreadPool;

mod database;
mod thread_pool;

const THREAD_POOL_SIZE: usize = 4;
const LISTEN_ADDRESS: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8080);

fn main() -> std::io::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    tracing::info!("Simple LSM DB Server starting...");

    let listener = TcpListener::bind(LISTEN_ADDRESS)?;
    let pool = ThreadPool::new(THREAD_POOL_SIZE)?;
    let database = Arc::new(Mutex::new(database::Database::new()?));

    for stream_result in listener.incoming() {
        match stream_result {
            Ok(stream) => {
                let database = Arc::clone(&database);
                if let Err(e) = pool.execute(move || handle_connection(stream, database)) {
                    tracing::error!("Failed to execute task in thread pool: {}", e);
                }
            }
            Err(e) => {
                tracing::error!("Error accepting connection: {}", e);
            }
        }
    }

    // TODO: Figure out graceful shutdown of the server

    Ok(())
}

fn handle_connection(mut stream: TcpStream, database: Arc<Mutex<database::Database>>) {
    let peer_addr = stream.peer_addr().ok();
    tracing::info!("New connection from {:?}", peer_addr);

    let mut buffer = [0; 128];

    // Read a single line
    match stream.read(&mut buffer) {
        Ok(0) => {
            tracing::info!("Connection closed by {:?}", peer_addr);
            return;
        }
        Ok(n) => {
            let bytes = &buffer[..n];

            match Command::try_from(bytes) {
                Ok(cmd) => {
                    tracing::info!("Received command from {:?}: {:?}", peer_addr, cmd);

                    let mut database = match database.lock() {
                        Ok(db) => db,
                        Err(error) => {
                            tracing::error!("Failed to lock database: {}", error);
                            return;
                        }
                    };

                    // Send a simple acknowledgment response
                    let response = match cmd {
                        Command::Get { key } => match database.get(key) {
                            Ok(Some(value)) => {
                                [b"OK: ".as_slice(), value.as_slice(), b"\n".as_slice()].concat()
                            }
                            Ok(None) => [b"OK: ".as_slice(), b"\n".as_slice()].concat(),
                            Err(error) => {
                                tracing::error!("Failed to get value from database: {}", error);
                                [&b"ERROR: "[..], error.to_string().as_bytes(), &b"\n"[..]].concat()
                            }
                        },
                        Command::Set { key, value } => match database.set(key, value) {
                            Ok(_) => [&b"OK: "[..], &b"\n"[..]].concat(),
                            Err(error) => {
                                tracing::error!("Failed to set value in database: {}", error);
                                [
                                    b"ERROR: ".as_slice(),
                                    error.to_string().as_bytes(),
                                    b"\n".as_slice(),
                                ]
                                .concat()
                            }
                        },
                        Command::Delete { key } => match database.delete(key) {
                            Ok(_) => [b"OK: ".as_slice(), b"\n".as_slice()].concat(),
                            Err(error) => {
                                tracing::error!("Failed to delete value from database: {}", error);
                                [
                                    b"ERROR: ".as_slice(),
                                    error.to_string().as_bytes(),
                                    b"\n".as_slice(),
                                ]
                                .concat()
                            }
                        },
                    };

                    if let Err(e) = stream.write_all(&response) {
                        tracing::error!("Failed to write response to {:?}: {}", peer_addr, e);
                    }
                }
                Err(e) => {
                    tracing::warn!("Invalid command from {:?}: {}", peer_addr, e);
                    let error_msg = format!("ERROR: {}\n", e);
                    if let Err(e) = stream.write_all(error_msg.as_bytes()) {
                        tracing::error!("Failed to write error to {:?}: {}", peer_addr, e);
                    }
                }
            }
        }
        Err(e) => {
            tracing::error!("Error reading from {:?}: {}", peer_addr, e);
        }
    }
}
