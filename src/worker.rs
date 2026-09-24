use std::sync::mpsc::{Sender, channel};
use std::thread::{JoinHandle, spawn};

pub(crate) struct Worker<TParam> {
    sender: Sender<TParam>,
    _thread: JoinHandle<()>,
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
            sender,
            _thread: thread,
        }
    }

    pub fn sender(&self) -> &Sender<TParam> {
        &self.sender
    }
}
