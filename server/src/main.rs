use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use protocol::{Command, Response};
use server::database;
use thread_pool::ThreadPool;

mod thread_pool;

const THREAD_POOL_SIZE: usize = 4;
const LISTEN_ADDRESS: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8080);

fn main() -> std::io::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    tracing::info!("Simple LSM DB Server starting...");

    let listener = TcpListener::bind(LISTEN_ADDRESS)?;
    let pool = ThreadPool::new(THREAD_POOL_SIZE)?;
    let database_dir = std::env::temp_dir().join("simple_lsm_db");
    let database = Arc::new(Mutex::new(database::Database::new(database_dir)?));

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

fn handle_connection(mut stream: TcpStream, database: Arc<Mutex<database::Database<PathBuf>>>) {
    let peer_addr = stream.peer_addr().ok();
    tracing::info!("New connection from {:?}", peer_addr);

    let mut buffer = [0; 128];

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

                    let response = match cmd {
                        Command::Get { key } => match database.get(key) {
                            Ok(possible_value) => Response::Ok(possible_value),
                            Err(error) => {
                                tracing::error!("Failed to get value from database: {}", error);
                                Response::Err(error.to_string())
                            }
                        },
                        Command::Set { key, value } => match database.set(key, value) {
                            Ok(_) => Response::Success,
                            Err(error) => {
                                tracing::error!("Failed to set value in database: {}", error);
                                Response::Err(error.to_string())
                            }
                        },
                        Command::Delete { key } => match database.delete(key) {
                            Ok(_) => Response::Success,
                            Err(error) => {
                                tracing::error!("Failed to delete value from database: {}", error);
                                Response::Err(error.to_string())
                            }
                        },
                    };

                    if let Err(e) = stream.write_all(Vec::<u8>::from(response).as_slice()) {
                        tracing::error!("Failed to write response to {:?}: {}", peer_addr, e);
                    }
                }
                Err(e) => {
                    tracing::error!("Invalid command from {:?}: {}", peer_addr, e);
                    let response = Response::Err(e.to_string());
                    if let Err(e) = stream.write_all(Vec::<u8>::from(response).as_slice()) {
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
