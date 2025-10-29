use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};

use protocol::Command;
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

    for stream_result in listener.incoming() {
        match stream_result {
            Ok(stream) => {
                if let Err(e) = pool.execute(move || {
                    handle_connection(stream);
                }) {
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

fn handle_connection(mut stream: TcpStream) {
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

                    // Send a simple acknowledgment response
                    let response = match cmd {
                        Command::Get { key } => {
                            format!("OK: GET {:?}\n", String::from_utf8_lossy(key))
                        }
                        Command::Set { key, value } => {
                            format!(
                                "OK: SET {:?} = {:?}\n",
                                String::from_utf8_lossy(key),
                                String::from_utf8_lossy(value)
                            )
                        }
                        Command::Delete { key } => {
                            format!("OK: DELETE {:?}\n", String::from_utf8_lossy(key))
                        }
                    };

                    if let Err(e) = stream.write_all(response.as_bytes()) {
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
