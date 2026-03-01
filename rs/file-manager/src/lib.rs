use bytes::Bytes;
use interfaces::DirectoryManager;
use notify::{Event, EventKind, INotifyWatcher, RecursiveMode, Watcher as _, recommended_watcher};
use std::{fs, io, path::Path, path::PathBuf, sync::mpsc};

pub struct DirectoryManagerImpl<Watcher> {
    root_directory: PathBuf,
    event_rx: mpsc::Receiver<Result<Event, notify::Error>>,
    _watcher: Watcher,
}

impl<Watcher> DirectoryManagerImpl<Watcher>
where
    Watcher: notify::Watcher,
{
    pub fn new(
        root_directory: PathBuf,
    ) -> Result<DirectoryManagerImpl<INotifyWatcher>, notify::Error> {
        let (event_tx, event_rx) = mpsc::channel();
        let mut watcher = recommended_watcher(event_tx)?;
        watcher.watch(&root_directory, RecursiveMode::Recursive)?;

        Ok(DirectoryManagerImpl {
            root_directory,
            event_rx,
            _watcher: watcher,
        })
    }

    fn to_relative_path(&self, path: &Path) -> Option<PathBuf> {
        if path.is_relative() {
            return Some(path.to_path_buf());
        }

        match path.strip_prefix(&self.root_directory) {
            Ok(relative_path) => Some(relative_path.to_path_buf()),
            Err(_) => {
                tracing::debug!(
                    path = %path.display(),
                    root_directory = %self.root_directory.display(),
                    "ignoring watcher path outside root directory",
                );
                None
            }
        }
    }

    fn file_from_disk(&self, path: &Path) -> Option<File> {
        let relative_path = self.to_relative_path(path)?;
        match fs::read(path) {
            Ok(content) => Some(File {
                relative_path,
                content: Bytes::from(content),
            }),
            Err(err) if err.kind() == io::ErrorKind::IsADirectory => None,
            Err(err) => {
                tracing::warn!(?err, path = %path.display(), "failed to read changed file");
                None
            }
        }
    }
}

impl<Watcher> DirectoryManager for DirectoryManagerImpl<Watcher>
where
    Watcher: notify::Watcher,
{
    type IncomingMessage = IncomingMessage;
    type OutgoingMessage = OutgoingMessage;

    fn push(&mut self, _messages: Vec<Self::IncomingMessage>) {
        todo!("write file to disk")
    }

    fn poll(&mut self) -> Vec<Self::OutgoingMessage> {
        const MAX_EVENTS_PER_POLL: usize = 1024;
        let mut ready_events = Vec::new();

        for _ in 0..MAX_EVENTS_PER_POLL {
            let ready_event = match self.event_rx.try_recv() {
                Ok(ready_event) => ready_event,
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    tracing::warn!("file watcher channel disconnected");
                    break;
                }
            };

            let ready_event = match ready_event {
                Ok(ready_event) => ready_event,
                Err(err) => {
                    tracing::error!(?err, "got an error from the file watcher");
                    continue;
                }
            };

            let Event { kind, paths, .. } = ready_event;

            match kind {
                EventKind::Access(_) | EventKind::Other | EventKind::Any => {}
                EventKind::Create(_) => {
                    for path in paths {
                        if let Some(file) = self.file_from_disk(&path) {
                            ready_events.push(OutgoingMessage::Create(file));
                        }
                    }
                }
                EventKind::Modify(_) => {
                    for path in paths {
                        if let Some(file) = self.file_from_disk(&path) {
                            ready_events.push(OutgoingMessage::Modify(file));
                        }
                    }
                }
                EventKind::Remove(_) => {
                    for path in paths {
                        if let Some(relative_path) = self.to_relative_path(&path) {
                            ready_events
                                .push(OutgoingMessage::Remove(RemovedFile { relative_path }));
                        }
                    }
                }
            }
        }

        ready_events
    }
}

pub enum IncomingMessage {
    Create(File),
    Modify(File),
    Remove(RemovedFile),
}

pub enum OutgoingMessage {
    Create(File),
    Modify(File),
    Remove(RemovedFile),
}

pub struct File {
    pub relative_path: PathBuf,
    pub content: Bytes,
}

pub struct RemovedFile {
    pub relative_path: PathBuf,
}
