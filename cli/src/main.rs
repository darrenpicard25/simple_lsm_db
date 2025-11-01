use std::io::{BufRead, BufReader, Write};

use clap::{Parser, Subcommand};
use protocol::Response;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Clone)]
enum Command {
    Get { key: String },
    Set { key: String, value: String },
    Delete { key: String },
}

fn main() {
    let args = Args::parse();
    let command = match args.command {
        Command::Get { key } => protocol::Command::Get {
            key: &key.into_bytes(),
        },
        Command::Set { key, value } => protocol::Command::Set {
            key: &key.into_bytes(),
            value: &value.into_bytes(),
        },
        Command::Delete { key } => protocol::Command::Delete {
            key: &key.into_bytes(),
        },
    };

    let mut stream = match std::net::TcpStream::connect("127.0.0.1:8080") {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!("Failed to connect to server: {}", e);
            return;
        }
    };

    if let Err(error) = stream.write_all(Vec::<u8>::from(command).as_slice()) {
        eprintln!("Failed to send command to server: {}", error);
        return;
    }

    let reader = BufReader::new(&stream);

    for line in reader.lines() {
        match line {
            Ok(line) => {
                let response = match Response::try_from(line.as_bytes()) {
                    Ok(response) => response,
                    Err(_) => {
                        eprintln!("Failed to parse response from server: {}", line);
                        return;
                    }
                };

                match response {
                    Response::Ok(items) => {
                        println!(
                            "OK: {}",
                            items
                                .map(|v| String::from_utf8_lossy(v.as_slice()).to_string())
                                .unwrap_or_else(|| "[None]".to_string())
                        );
                    }
                    Response::Err(error) => {
                        eprintln!("Error: {}", error);
                        return;
                    }
                    Response::Success => {
                        println!("Success");
                    }
                }
            }
            Err(error) => {
                eprintln!("Failed to read response from server: {}", error);
                return;
            }
        }
    }
}
