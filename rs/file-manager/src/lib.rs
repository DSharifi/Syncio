use bytes::Bytes;
use interfaces::DirectoryManager;
use notify::{Event, EventKind, INotifyWatcher, recommended_watcher};
use std::{path::PathBuf, sync::mpsc};

pub struct DirectoryManagerImpl<Watcher> {
    root_directory: PathBuf,
    event_rx: mpsc::Receiver<Result<Event, notify::Error>>,
    watcher: Watcher,
}

impl<Watcher> DirectoryManagerImpl<Watcher>
where
    Watcher: notify::Watcher,
{
    pub fn new(
        root_directory: PathBuf,
    ) -> Result<DirectoryManagerImpl<INotifyWatcher>, notify::Error> {
        let (event_tx, event_rx) = mpsc::channel();
        let watcher = recommended_watcher(event_tx)?;

        Ok(DirectoryManagerImpl {
            root_directory,
            event_rx,
            watcher,
        })
    }
}

impl<Watcher> DirectoryManager for DirectoryManagerImpl<Watcher>
where
    Watcher: notify::Watcher,
{
    type IncomingMessage = IncomingMessage;
    type OutgoingMessage = OutgoingMessage;

    fn push(&mut self, messages: Vec<Self::IncomingMessage>) {
        todo!("write file to disk")
    }

    fn poll(&mut self) -> Vec<Self::OutgoingMessage> {
        // TODO: keep number of events processed bounded
        let mut ready_events = vec![];

        while let Ok(ready_event) = self.event_rx.try_recv() {
            // ready_events.push(ready_event);

            let ready_event = match ready_event {
                Ok(ready_event) => ready_event,
                Err(err) => {
                    tracing::error!(?err, "got an error from the file watcher");
                    continue;
                }
            };

            match ready_event.kind {
                EventKind::Access(access_kind) => todo!(),
                EventKind::Create(create_kind) => todo!(),
                EventKind::Modify(modify_kind) => todo!(),
                EventKind::Remove(remove_kind) => todo!(),
                EventKind::Other => todo!(),
                EventKind::Any => todo!(),
            }
        }

        ready_events
    }
}

pub enum IncomingMessage {
    Create(File),
    Modify(File),
    Remove(File),
}

pub enum OutgoingMessage {
    Create(File),
    Modify(File),
    Remove(File),
}

pub struct File {
    relative_path: PathBuf,
    content: Bytes,
}

enum Error {}
