use std::sync::{Arc, Mutex};

mod job;
mod worker;

const MIN_THREAD_POOL_SIZE: usize = 1;
const MAX_THREAD_POOL_SIZE: usize = 10;

pub struct ThreadPool {
    sender: Option<std::sync::mpsc::Sender<Box<job::Job>>>,
    workers: Vec<worker::Worker>,
}

impl ThreadPool {
    pub fn new(size: usize) -> std::io::Result<Self> {
        if size < MIN_THREAD_POOL_SIZE || size > MAX_THREAD_POOL_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid thread pool size",
            ));
        }

        let mut workers = Vec::with_capacity(size);
        let (sender, receiver) = std::sync::mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        for id in 0..size {
            workers.push(worker::Worker::new(id, Arc::clone(&receiver)));
        }

        Ok(Self {
            workers,
            sender: Some(sender),
        })
    }

    pub fn execute<F>(&self, f: F) -> Result<(), std::io::Error>
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        match self.sender.as_ref() {
            Some(sender) => sender
                .send(job)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error.to_string())),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Sender not found",
            )),
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        tracing::info!("Shutting down thread pool.");

        drop(std::mem::take(&mut self.sender));
        for worker in &mut self.workers {
            if let Some(thread) = std::mem::take(&mut worker.thread) {
                thread.join().unwrap();
            }
        }
    }
}
