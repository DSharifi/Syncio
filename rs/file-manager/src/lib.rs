use bytes::Bytes;
use interfaces::DirectoryManager;
use notify::{Event, EventKind, INotifyWatcher, RecursiveMode, Watcher as _, recommended_watcher};
use std::{
    fs, io,
    path::{Component, Path, PathBuf},
    sync::mpsc,
};

const MAX_EVENTS_PER_POLL: usize = 1024;

pub struct DirectoryManagerImpl<Watcher> {
    root_directory: PathBuf,
    event_rx: mpsc::Receiver<Result<Event, notify::Error>>,
    _watcher: Watcher,
}

impl DirectoryManagerImpl<INotifyWatcher> {
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
}

impl<Watcher> DirectoryManagerImpl<Watcher> {
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

    fn resolve_relative_path(&self, relative_path: &Path) -> Option<PathBuf> {
        if relative_path.is_absolute() {
            tracing::warn!(
                relative_path = %relative_path.display(),
                "incoming path must be relative",
            );
            return None;
        }

        if relative_path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
        {
            tracing::warn!(
                relative_path = %relative_path.display(),
                "incoming path contains unsupported components",
            );
            return None;
        }

        Some(self.root_directory.join(relative_path))
    }

    fn write_file(&self, file: File) {
        let Some(path) = self.resolve_relative_path(&file.relative_path) else {
            return;
        };

        let Some(parent_directory) = path.parent() else {
            tracing::warn!(path = %path.display(), "failed to compute parent directory");
            return;
        };

        if let Err(err) = fs::create_dir_all(parent_directory) {
            tracing::error!(
                ?err,
                path = %parent_directory.display(),
                "failed to create parent directory for incoming file",
            );
            return;
        }

        if let Err(err) = fs::write(&path, file.content) {
            tracing::error!(
                ?err,
                path = %path.display(),
                "failed to write incoming file to disk",
            );
        }
    }

    fn remove_file(&self, removed_file: RemovedFile) {
        let Some(path) = self.resolve_relative_path(&removed_file.relative_path) else {
            return;
        };

        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) if err.kind() == io::ErrorKind::IsADirectory => {
                if let Err(dir_err) = fs::remove_dir_all(&path) {
                    tracing::error!(
                        ?dir_err,
                        path = %path.display(),
                        "failed to remove incoming directory path",
                    );
                }
            }
            Err(err) => {
                tracing::error!(
                    ?err,
                    path = %path.display(),
                    "failed to remove incoming path",
                );
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

    fn push(&mut self, messages: Vec<Self::IncomingMessage>) {
        for message in messages {
            match message {
                IncomingMessage::Create(file) | IncomingMessage::Modify(file) => {
                    self.write_file(file)
                }
                IncomingMessage::Remove(removed_file) => self.remove_file(removed_file),
            }
        }
    }

    fn poll(&mut self) -> Vec<Self::OutgoingMessage> {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncomingMessage {
    Create(File),
    Modify(File),
    Remove(RemovedFile),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutgoingMessage {
    Create(File),
    Modify(File),
    Remove(RemovedFile),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct File {
    pub relative_path: PathBuf,
    pub content: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovedFile {
    pub relative_path: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::mock;
    use notify::{
        Config, Error as NotifyError, EventHandler,
        event::{AccessKind, CreateKind, DataChange, ModifyKind, RemoveKind},
    };
    use tempfile::tempdir;

    mock! {
        Watcher {}

        impl notify::Watcher for Watcher {
            fn new<H: EventHandler>(
                event_handler: H,
                config: Config,
            ) -> notify::Result<Self>
            where
                Self: Sized;
            fn watch(&mut self, path: &Path, recursive_mode: RecursiveMode) -> notify::Result<()>;
            fn unwatch(&mut self, path: &Path) -> notify::Result<()>;
            fn kind() -> notify::WatcherKind
            where
                Self: Sized;
        }
    }

    fn manager_with_channel(
        root_directory: PathBuf,
    ) -> (
        DirectoryManagerImpl<MockWatcher>,
        mpsc::Sender<Result<Event, notify::Error>>,
    ) {
        let (event_tx, event_rx) = mpsc::channel();
        (
            DirectoryManagerImpl {
                root_directory,
                event_rx,
                _watcher: MockWatcher::default(),
            },
            event_tx,
        )
    }

    fn event(kind: EventKind, path: PathBuf) -> Event {
        Event {
            kind,
            paths: vec![path],
            attrs: Default::default(),
        }
    }

    #[test]
    fn resolve_relative_path_when_simple_relative_then_joins_with_root() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (manager, _event_tx) = manager_with_channel(temp_dir.path().to_path_buf());

        // When
        let resolved_path = manager.resolve_relative_path(Path::new("nested/file.txt"));

        // Then
        assert_eq!(resolved_path, Some(temp_dir.path().join("nested/file.txt")),);
    }

    #[test]
    fn resolve_relative_path_when_absolute_then_rejects_path() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (manager, _event_tx) = manager_with_channel(temp_dir.path().to_path_buf());
        let absolute_path = temp_dir.path().join("absolute.txt");

        // When
        let resolved_path = manager.resolve_relative_path(&absolute_path);

        // Then
        assert_eq!(resolved_path, None);
    }

    #[test]
    fn resolve_relative_path_when_parent_dir_component_then_rejects_path() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (manager, _event_tx) = manager_with_channel(temp_dir.path().to_path_buf());
        let traversing_path = PathBuf::from("../escape.txt");

        // When
        let resolved_path = manager.resolve_relative_path(&traversing_path);

        // Then
        assert_eq!(resolved_path, None);
    }

    #[test]
    fn to_relative_path_when_absolute_inside_root_then_strips_root_prefix() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (manager, _event_tx) = manager_with_channel(temp_dir.path().to_path_buf());
        let file_path = temp_dir.path().join("child/example.txt");

        // When
        let relative_path = manager.to_relative_path(&file_path);

        // Then
        assert_eq!(relative_path, Some(PathBuf::from("child/example.txt")));
    }

    #[test]
    fn to_relative_path_when_absolute_outside_root_then_ignored() {
        // Given
        let root_temp_dir = tempdir().expect("tempdir should be created");
        let outside_temp_dir = tempdir().expect("tempdir should be created");
        let (manager, _event_tx) = manager_with_channel(root_temp_dir.path().to_path_buf());
        let outside_path = outside_temp_dir.path().join("outside.txt");

        // When
        let relative_path = manager.to_relative_path(&outside_path);

        // Then
        assert_eq!(relative_path, None);
    }

    #[test]
    fn file_from_disk_when_regular_file_then_returns_relative_path_and_content() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (manager, _event_tx) = manager_with_channel(temp_dir.path().to_path_buf());
        let absolute_path = temp_dir.path().join("data.txt");
        fs::write(&absolute_path, b"payload").expect("file should be written");

        // When
        let file = manager.file_from_disk(&absolute_path);

        // Then
        assert_eq!(
            file,
            Some(File {
                relative_path: PathBuf::from("data.txt"),
                content: Bytes::from_static(b"payload"),
            }),
        );
    }

    #[test]
    fn file_from_disk_when_path_is_directory_then_returns_none() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (manager, _event_tx) = manager_with_channel(temp_dir.path().to_path_buf());
        let directory_path = temp_dir.path().join("directory");
        fs::create_dir_all(&directory_path).expect("directory should be created");

        // When
        let file = manager.file_from_disk(&directory_path);

        // Then
        assert_eq!(file, None);
    }

    #[test]
    fn push_when_create_modify_remove_then_applies_changes_to_disk_in_order() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (mut manager, _event_tx) = manager_with_channel(temp_dir.path().to_path_buf());
        let relative_path = PathBuf::from("sub/tree/file.txt");
        let absolute_path = temp_dir.path().join(&relative_path);

        // When
        interfaces::DirectoryManager::push(
            &mut manager,
            vec![IncomingMessage::Create(File {
                relative_path: relative_path.clone(),
                content: Bytes::from_static(b"v1"),
            })],
        );
        interfaces::DirectoryManager::push(
            &mut manager,
            vec![IncomingMessage::Modify(File {
                relative_path: relative_path.clone(),
                content: Bytes::from_static(b"v2"),
            })],
        );
        interfaces::DirectoryManager::push(
            &mut manager,
            vec![IncomingMessage::Remove(RemovedFile {
                relative_path: relative_path.clone(),
            })],
        );

        // Then
        assert!(!absolute_path.exists());
    }

    #[test]
    fn remove_file_when_target_missing_then_no_error_and_no_side_effects() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (manager, _event_tx) = manager_with_channel(temp_dir.path().to_path_buf());
        let missing_relative_path = PathBuf::from("missing.txt");

        // When
        manager.remove_file(RemovedFile {
            relative_path: missing_relative_path.clone(),
        });

        // Then
        assert!(!temp_dir.path().join(missing_relative_path).exists());
    }

    #[cfg(unix)]
    #[test]
    fn remove_file_when_target_is_directory_then_removes_directory_tree() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (manager, _event_tx) = manager_with_channel(temp_dir.path().to_path_buf());
        let relative_directory = PathBuf::from("dir-to-remove");
        let absolute_directory = temp_dir.path().join(&relative_directory);
        fs::create_dir_all(absolute_directory.join("nested")).expect("directory should be created");
        fs::write(absolute_directory.join("nested/file.txt"), b"value")
            .expect("file should be written");

        // When
        manager.remove_file(RemovedFile {
            relative_path: relative_directory,
        });

        // Then
        assert!(!absolute_directory.exists());
    }

    #[test]
    fn poll_when_create_event_then_emits_create_message_with_content() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (mut manager, event_tx) = manager_with_channel(temp_dir.path().to_path_buf());
        let absolute_path = temp_dir.path().join("created.txt");
        fs::write(&absolute_path, b"create-content").expect("file should be written");
        event_tx
            .send(Ok(event(
                EventKind::Create(CreateKind::File),
                absolute_path.clone(),
            )))
            .expect("event should be sent");

        // When
        let messages = interfaces::DirectoryManager::poll(&mut manager);

        // Then
        assert_eq!(
            messages,
            vec![OutgoingMessage::Create(File {
                relative_path: PathBuf::from("created.txt"),
                content: Bytes::from_static(b"create-content"),
            })],
        );
    }

    #[test]
    fn poll_when_modify_event_then_emits_modify_message_with_content() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (mut manager, event_tx) = manager_with_channel(temp_dir.path().to_path_buf());
        let absolute_path = temp_dir.path().join("updated.txt");
        fs::write(&absolute_path, b"modify-content").expect("file should be written");
        event_tx
            .send(Ok(event(
                EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                absolute_path.clone(),
            )))
            .expect("event should be sent");

        // When
        let messages = interfaces::DirectoryManager::poll(&mut manager);

        // Then
        assert_eq!(
            messages,
            vec![OutgoingMessage::Modify(File {
                relative_path: PathBuf::from("updated.txt"),
                content: Bytes::from_static(b"modify-content"),
            })],
        );
    }

    #[test]
    fn poll_when_remove_event_then_emits_remove_message_with_relative_path() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (mut manager, event_tx) = manager_with_channel(temp_dir.path().to_path_buf());
        let absolute_path = temp_dir.path().join("deleted.txt");
        event_tx
            .send(Ok(event(
                EventKind::Remove(RemoveKind::File),
                absolute_path.clone(),
            )))
            .expect("event should be sent");

        // When
        let messages = interfaces::DirectoryManager::poll(&mut manager);

        // Then
        assert_eq!(
            messages,
            vec![OutgoingMessage::Remove(RemovedFile {
                relative_path: PathBuf::from("deleted.txt"),
            })],
        );
    }

    #[test]
    fn poll_when_event_path_outside_root_then_ignores_event() {
        // Given
        let root_temp_dir = tempdir().expect("tempdir should be created");
        let outside_temp_dir = tempdir().expect("tempdir should be created");
        let (mut manager, event_tx) = manager_with_channel(root_temp_dir.path().to_path_buf());
        let outside_path = outside_temp_dir.path().join("outside.txt");
        event_tx
            .send(Ok(event(
                EventKind::Remove(RemoveKind::File),
                outside_path.clone(),
            )))
            .expect("event should be sent");

        // When
        let messages = interfaces::DirectoryManager::poll(&mut manager);

        // Then
        assert!(messages.is_empty());
    }

    #[test]
    fn poll_when_access_any_or_other_events_then_ignores_all() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (mut manager, event_tx) = manager_with_channel(temp_dir.path().to_path_buf());

        event_tx
            .send(Ok(event(
                EventKind::Access(AccessKind::Read),
                PathBuf::from("access.txt"),
            )))
            .expect("event should be sent");
        event_tx
            .send(Ok(event(EventKind::Any, PathBuf::from("any.txt"))))
            .expect("event should be sent");
        event_tx
            .send(Ok(event(EventKind::Other, PathBuf::from("other.txt"))))
            .expect("event should be sent");

        // When
        let messages = interfaces::DirectoryManager::poll(&mut manager);

        // Then
        assert!(messages.is_empty());
    }

    #[test]
    fn poll_when_watcher_returns_error_event_then_skips_it() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (mut manager, event_tx) = manager_with_channel(temp_dir.path().to_path_buf());
        event_tx
            .send(Err(NotifyError::generic("watcher failed")))
            .expect("event should be sent");

        // When
        let messages = interfaces::DirectoryManager::poll(&mut manager);

        // Then
        assert!(messages.is_empty());
    }

    #[test]
    fn poll_when_channel_disconnected_then_returns_empty_list() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (event_tx, event_rx) = mpsc::channel::<Result<Event, notify::Error>>();
        drop(event_tx);
        let mut manager = DirectoryManagerImpl {
            root_directory: temp_dir.path().to_path_buf(),
            event_rx,
            _watcher: MockWatcher::default(),
        };

        // When
        let messages = interfaces::DirectoryManager::poll(&mut manager);

        // Then
        assert!(messages.is_empty());
    }

    #[test]
    fn poll_when_more_than_max_events_arrive_then_processes_them_across_multiple_calls() {
        // Given
        let temp_dir = tempdir().expect("tempdir should be created");
        let (mut manager, event_tx) = manager_with_channel(temp_dir.path().to_path_buf());

        for index in 0..(MAX_EVENTS_PER_POLL + 1) {
            event_tx
                .send(Ok(event(
                    EventKind::Remove(RemoveKind::File),
                    PathBuf::from(format!("file-{index}.txt")),
                )))
                .expect("event should be sent");
        }

        // When
        let first_poll_messages = interfaces::DirectoryManager::poll(&mut manager);
        let second_poll_messages = interfaces::DirectoryManager::poll(&mut manager);

        // Then
        assert_eq!(first_poll_messages.len(), MAX_EVENTS_PER_POLL);
        assert_eq!(second_poll_messages.len(), 1);
    }
}
