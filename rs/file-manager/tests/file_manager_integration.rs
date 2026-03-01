use bytes::Bytes;
use file_manager::{DirectoryManagerImpl, File, IncomingMessage, OutgoingMessage, RemovedFile};
use interfaces::DirectoryManager;
use std::{
    fs,
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};
use tempfile::tempdir;

fn wait_for_message<F>(
    manager: &mut DirectoryManagerImpl<notify::INotifyWatcher>,
    timeout: Duration,
    mut predicate: F,
) -> OutgoingMessage
where
    F: FnMut(&OutgoingMessage) -> bool,
{
    let deadline = Instant::now() + timeout;

    loop {
        let messages = DirectoryManager::poll(manager);
        for message in messages {
            if predicate(&message) {
                return message;
            }
        }

        if Instant::now() >= deadline {
            panic!("timed out waiting for expected message");
        }

        thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn push_with_real_manager_when_create_modify_remove_then_updates_disk_state() {
    // Given
    let temp_dir = tempdir().expect("tempdir should be created");
    let root_directory = temp_dir.path().to_path_buf();
    let mut manager = DirectoryManagerImpl::<notify::INotifyWatcher>::new(root_directory.clone())
        .expect("directory manager should be created");
    let relative_path = PathBuf::from("integration/roundtrip.txt");
    let absolute_path = root_directory.join(&relative_path);

    // When
    DirectoryManager::push(
        &mut manager,
        vec![IncomingMessage::Create(File {
            relative_path: relative_path.clone(),
            content: Bytes::from_static(b"first"),
        })],
    );
    DirectoryManager::push(
        &mut manager,
        vec![IncomingMessage::Modify(File {
            relative_path: relative_path.clone(),
            content: Bytes::from_static(b"second"),
        })],
    );
    DirectoryManager::push(
        &mut manager,
        vec![IncomingMessage::Remove(RemovedFile {
            relative_path: relative_path.clone(),
        })],
    );

    // Then
    assert!(!absolute_path.exists());
}

#[test]
fn poll_with_real_watcher_when_file_created_then_emits_create_or_modify_message() {
    // Given
    let temp_dir = tempdir().expect("tempdir should be created");
    let root_directory = temp_dir.path().to_path_buf();
    let relative_path = PathBuf::from("integration/create.txt");
    let absolute_path = root_directory.join(&relative_path);
    fs::create_dir_all(absolute_path.parent().expect("parent should exist"))
        .expect("directory should be created");
    let mut manager = DirectoryManagerImpl::<notify::INotifyWatcher>::new(root_directory.clone())
        .expect("directory manager should be created");

    // When
    fs::write(&absolute_path, b"created").expect("file should be written");

    // Then
    let message = wait_for_message(&mut manager, Duration::from_secs(5), |message| {
        matches!(
            message,
            OutgoingMessage::Create(file)
                if file.relative_path == relative_path && file.content.as_ref() == b"created"
        ) || matches!(
            message,
            OutgoingMessage::Modify(file)
                if file.relative_path == relative_path && file.content.as_ref() == b"created"
        )
    });
    assert!(matches!(
        message,
        OutgoingMessage::Create(_) | OutgoingMessage::Modify(_)
    ));
}

#[test]
fn poll_with_real_watcher_when_existing_file_modified_then_emits_modify_message() {
    // Given
    let temp_dir = tempdir().expect("tempdir should be created");
    let root_directory = temp_dir.path().to_path_buf();
    let relative_path = PathBuf::from("integration/modify.txt");
    let absolute_path = root_directory.join(&relative_path);
    fs::create_dir_all(absolute_path.parent().expect("parent should exist"))
        .expect("directory should be created");
    fs::write(&absolute_path, b"before").expect("file should be written");
    let mut manager = DirectoryManagerImpl::<notify::INotifyWatcher>::new(root_directory.clone())
        .expect("directory manager should be created");

    // When
    fs::write(&absolute_path, b"after").expect("file should be written");

    // Then
    let message = wait_for_message(&mut manager, Duration::from_secs(5), |message| {
        matches!(
            message,
            OutgoingMessage::Modify(file)
                if file.relative_path == relative_path && file.content.as_ref() == b"after"
        )
    });
    assert!(matches!(message, OutgoingMessage::Modify(_)));
}

#[test]
fn poll_with_real_watcher_when_file_removed_then_emits_remove_message() {
    // Given
    let temp_dir = tempdir().expect("tempdir should be created");
    let root_directory = temp_dir.path().to_path_buf();
    let relative_path = PathBuf::from("integration/remove.txt");
    let absolute_path = root_directory.join(&relative_path);
    fs::create_dir_all(absolute_path.parent().expect("parent should exist"))
        .expect("directory should be created");
    fs::write(&absolute_path, b"value").expect("file should be written");
    let mut manager = DirectoryManagerImpl::<notify::INotifyWatcher>::new(root_directory.clone())
        .expect("directory manager should be created");

    // When
    fs::remove_file(&absolute_path).expect("file should be removed");

    // Then
    let message = wait_for_message(&mut manager, Duration::from_secs(5), |message| {
        matches!(
            message,
            OutgoingMessage::Remove(removed_file) if removed_file.relative_path == relative_path
        )
    });
    assert!(matches!(message, OutgoingMessage::Remove(_)));
}
