use std::sync::mpsc::{Sender, channel};
use std::thread::{JoinHandle, spawn};

pub(crate) struct Worker<TParam: Send + 'static> {
    sender: Option<Sender<TParam>>,
    thread: Option<JoinHandle<()>>,
}

impl<TParam: Send + 'static> Worker<TParam> {
    pub fn new<TFunc>(mut func: TFunc) -> Self
    where
        TFunc: FnMut(TParam) + Send + 'static,
    {
        let (sender, receiver) = channel::<TParam>();
        let thread = spawn(move || {
            for item in receiver.iter() {
                func(item);
            }
        });
        Self {
            sender: Some(sender),
            thread: Some(thread),
        }
    }

    pub fn sender(&self) -> &Sender<TParam> {
        self.sender
            .as_ref()
            .expect("worker sender requested after shutdown")
    }

    pub fn shutdown(&mut self) -> std::thread::Result<()> {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            thread.join()
        } else {
            Ok(())
        }
    }
}

impl<TParam: Send + 'static> Drop for Worker<TParam> {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
