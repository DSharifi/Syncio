pub trait DirectoryManager {
    type IncomingMessage;
    type OutgoingMessage;

    // push new messages coming
    fn push(&mut self, messages: Vec<Self::IncomingMessage>);

    // poll for actions
    fn poll(&mut self) -> Vec<Self::OutgoingMessage>;
}
