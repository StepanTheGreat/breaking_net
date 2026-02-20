//! Channels act as filters for our messages. They serve 2 main purposes:
//! 1. Some of them maintain message order (for example by stalling until all messages are received)
//! 2. They filter out dublicate messages (that were already received by the user)
//!
//! Some messages serve a bit more purposes than that, but the primary usecases are these.

use std::{cmp::Reverse, collections::{BTreeMap, VecDeque}};

use crate::{
    Reliability,
    packet::{MessageId, UserMessage},
    window::SlidingAckWindow,
};

/// A super minimal trait for all channels
pub trait Channel {
    /// Process the provided message  
    fn process_message(&mut self, window: &SlidingAckWindow, messagee: UserMessage);

    /// Try receive a message (if available)
    fn recv_message(&mut self, window: &SlidingAckWindow) -> Option<UserMessage>;
}

struct ReorderedMessage {
    message: UserMessage,
    
    // This is reversed to ensure that we sort from the smallest to the biggest sequenced message ID
    msg_id: Reverse<MessageId>
}

impl ReorderedMessage {
    pub fn new(message: UserMessage) -> Self {
        let msg_id = message.message_id().expect("Reordered messages must always contain a sequence ID");

        Self {
            msg_id: Reverse(msg_id),
            message,
        }
    }
}

impl PartialEq for ReorderedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.msg_id.eq(&other.msg_id)
    }
}

impl PartialOrd for ReorderedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.msg_id.partial_cmp(&other.msg_id)
    }
}

/// A fully reliable channel (reliable and ordered). The slowest, but most reliable
struct ReliableChannel {
    /// The receive buffer
    recv_buff: BTreeMap<MessageId, UserMessage>,
    window_pos: MessageId,
}

impl ReliableChannel {
    fn new() -> Self {
        Self {
            recv_buff: BTreeMap::new(),
            window_pos: 0,
        }
    }
}

impl Channel for ReliableChannel {
    fn process_message(&mut self, window: &SlidingAckWindow, message: UserMessage) {
        assert_eq!(message.reliability(), Reliability::Reliable);

        // Get the most recent window position
        self.window_pos = window.window_position();

        let msg_id = message.message_id().unwrap();

        // The filter here is simple: if our message is not yet marked - add it to the receiving buffer
        if !window.is_marked(msg_id) {
            self.recv_buff.insert(msg_id, message);
        }
    }

    /// Try receive a user message if possible
    fn recv_message(&mut self, window: &SlidingAckWindow) -> Option<UserMessage> {
        if self.recv_buff.is_empty() {
            return None;
        }

        let msg_id = *self.recv_buff.first_key_value().unwrap().0;

        // If the message's sequence ID is actually now considered "old". Only then we can receive said message
        if msg_id < window.window_position() {
            Some(self.recv_buff.pop_first().unwrap().1)
        } else {
            None
        }
    }
}

/// A reliable channel only cares about reliability and deduplication
struct ReliableUnorderedChannel {
    recv_buff: VecDeque<UserMessage>,
}

impl ReliableUnorderedChannel {
    fn new() -> Self {
        Self {
            recv_buff: VecDeque::new(),
        }
    }
}

impl Channel for ReliableUnorderedChannel {
    fn process_message(&mut self, window: &SlidingAckWindow, message: UserMessage) {
        let msg_id = message
            .message_id()
            .expect("Reliable messages always have sequence IDs");

        // Here we don't care about any order whatsoever

        if !window.is_marked(msg_id) {
            self.recv_buff.push_back(message);
        }
    }

    fn recv_message(&mut self, _: &SlidingAckWindow) -> Option<UserMessage> {
        self.recv_buff.pop_front()
    }
}

/// A reliable channel only cares about reliability and deduplication
struct UnreliableChannel {
    recv_buff: VecDeque<UserMessage>,
}

impl UnreliableChannel {
    fn new() -> Self {
        Self {
            recv_buff: VecDeque::new(),
        }
    }
}

impl Channel for UnreliableChannel {
    fn process_message(&mut self, _: &SlidingAckWindow, message: UserMessage) {
        self.recv_buff.push_back(message);
    }

    fn recv_message(&mut self, _: &SlidingAckWindow) -> Option<UserMessage> {
        self.recv_buff.pop_front()
    }
}

/// A storage of different channels
pub struct ChannelStorage {
    reliable_unordered: ReliableUnorderedChannel,
    reliable: ReliableChannel,
    unreliable: UnreliableChannel,
}

impl ChannelStorage {
    pub fn new() -> Self {
        Self {
            reliable_unordered: ReliableUnorderedChannel::new(),
            reliable: ReliableChannel::new(),
            unreliable: UnreliableChannel::new(),
        }
    }
}

impl Channel for ChannelStorage {
    fn process_message(&mut self, window: &SlidingAckWindow, message: UserMessage) {
        match message.reliability() {
            Reliability::Reliable => self.reliable.process_message(window, message),
            Reliability::ReliableUnordered => {
                self.reliable_unordered.process_message(window, message)
            }
            Reliability::Unreliable => self.unreliable.process_message(window, message),
        }
    }

    fn recv_message(&mut self, window: &SlidingAckWindow) -> Option<UserMessage> {
        if let Some(message) = self.unreliable.recv_message(window) {
            return Some(message);
        }

        if let Some(message) = self.reliable_unordered.recv_message(window) {
            return Some(message);
        }

        if let Some(message) = self.reliable.recv_message(window) {
            return Some(message);
        }

        None
    }
}
