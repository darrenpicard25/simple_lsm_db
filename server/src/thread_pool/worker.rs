use std::sync::{Arc, Mutex};

pub struct Worker {
    id: usize,
    pub(super) thread: Option<std::thread::JoinHandle<()>>,
}

impl Worker {
    pub fn new(
        id: usize,
        receiver: Arc<Mutex<std::sync::mpsc::Receiver<Box<super::job::Job>>>>,
    ) -> Self {
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
