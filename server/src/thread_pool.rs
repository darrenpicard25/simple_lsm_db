use std::sync::{Arc, Mutex};

const MIN_THREAD_POOL_SIZE: usize = 1;
const MAX_THREAD_POOL_SIZE: usize = 10;

type Job = Box<dyn FnOnce() + Send + 'static>;
pub struct ThreadPool {
    sender: Option<std::sync::mpsc::Sender<Job>>,
    workers: Vec<Worker>,
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
            workers.push(Worker::new(id, Arc::clone(&receiver)));
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

struct Worker {
    id: usize,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<std::sync::mpsc::Receiver<Job>>>) -> Self {
        let thread = std::thread::spawn(move || {
            while let Ok(Ok(message)) = receiver
                .lock()
                .inspect_err(|error| {
                    tracing::error!(
                        "Worker {} received error while acquiring receiver lock: {:?}",
                        id,
                        error
                    );
                })
                .map(|lock| lock.recv())
            {
                tracing::info!("Worker {} received message.", id);
                message();
            }

            tracing::info!("Worker {} shutting down.", id);
        });

        Self {
            id,
            thread: Some(thread),
        }
    }
}
