use std::collections::HashMap;
use std::sync::mpsc::Sender;
use rand::prelude::IteratorRandom;
use crate::{Message, MessageBody, PeerId};

#[derive(Default)]
pub struct Network {
    senders: HashMap<PeerId, Sender<Message>>,
}

impl Network {
    
    pub fn send_client_message(&self, message_body: MessageBody) -> Result<(), String> {
       if let Some(&to) = self.senders.keys().choose(&mut rand::rng()) {
            self.send(Message{
                from: 0.into(),
                to,
                body: message_body,
            });
           Ok(())
        } else {
           Err("Warning: No peers to send message to".to_string())
       }
    }
    
    pub fn send(&self, message: Message) {
        if let Some(sender) = self.senders.get(&message.to) {
            sender.send(message).expect("Failed to send message");
        } else {
            println!(
                "Warning: Attempted to send message to unknown peer {:?}",
                message.to
            );
        }
    }

    pub fn broadcast(&self, message_body: &MessageBody, from: PeerId, recipients: &Vec<PeerId>) {
        for to in recipients {
            self.send(Message {
                from,
                to: *to,
                body: message_body.clone(),
            })
        }
    }

    pub fn add_peer(&mut self, peer_id: PeerId, sender: Sender<Message>) {
        self.senders.insert(peer_id, sender);
    }
}