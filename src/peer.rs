pub mod consensus;
mod effects;
mod messages;

pub use crate::config::DEFAULT_CHANNEL_SIZE;
use crate::network::NetworkInterface;
use crate::peer::consensus::{ConsensusEffect, ConsensusEngine, ConsensusState};
use crate::peer::effects::PeerEffects;
pub use crate::peer::messages::{Message, MessageBody, PeerId, RaftReplicatedBlock, TxPayload};
use crate::storage::{
    BlockFile, BlockHash, BlockKeeper, BlockStatus, BlockStorageState, BlockStorageView,
};
use crate::transactions::{SignedTransaction, VerifiedTransaction};
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use log::debug;
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;

pub struct Peer<Network: NetworkInterface> {
    pub id: PeerId,
    consensus: ConsensusEngine,
    consensus_state_tx: watch::Sender<ConsensusState>,
    effects: PeerEffects<Network>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PeerState {
    pub peer_id: PeerId,
    pub known_peers: Vec<PeerId>,
    pub block: BlockStorageState,
    pub consensus: ConsensusState,
}

pub struct PeerStateView {
    peer_id: PeerId,
    block_storage_view: BlockStorageView,
    consensus_state_rx: watch::Receiver<ConsensusState>,
}

impl PeerStateView {
    pub fn get_state(&self, known_peers: Vec<PeerId>) -> PeerState {
        PeerState {
            peer_id: self.peer_id,
            known_peers,
            block: self.block_storage_view.get_latest_state(),
            consensus: self.consensus_state_rx.borrow().clone(),
        }
    }
}

impl<Network: NetworkInterface> Peer<Network> {
    pub fn new(
        id: PeerId,
        network: Arc<Network>,
        consensus: ConsensusEngine,
        block_keeper: BlockKeeper,
        signing_key: SigningKey,
    ) -> Peer<Network> {
        let (consensus_state_tx, _) = watch::channel(consensus.state());
        Self {
            id,
            consensus,
            consensus_state_tx,
            effects: PeerEffects::new(id, network, block_keeper, signing_key),
        }
    }

    pub fn block_keeper_mut(&mut self) -> &mut BlockKeeper {
        self.effects.block_keeper_mut()
    }

    /// Validates a block obtained through block synchronization against the local chain
    /// before staging and committing it, instead of trusting the sync source blindly.
    pub fn apply_synchronized_block(&mut self, block_file: BlockFile) -> Result<(), String> {
        self.effects.apply_synchronized_block(block_file)
    }

    pub fn create_state_view(&self) -> PeerStateView {
        PeerStateView {
            peer_id: self.id,
            block_storage_view: self.effects.create_block_storage_view(),
            consensus_state_rx: self.consensus_state_tx.subscribe(),
        }
    }

    pub fn handle_message(&mut self, message: Message) {
        debug!("Received message: {message}");
        if let Err(e) = match message.body {
            MessageBody::ClientTransaction(client_tx) => self.process_client_transaction(client_tx),
            MessageBody::Synchronization(verified_tx) => self.synchronize_transaction(verified_tx),
            MessageBody::BlockProposal {
                block_hash,
                block_file,
                signature,
                public_key,
            } => self.process_block_proposal(
                block_hash,
                block_file,
                signature,
                public_key,
                message.from,
            ),
            MessageBody::BlockApproved { block_hash } => {
                self.process_block_vote(block_hash, message.from, true)
            }
            MessageBody::BlockReject { block_hash } => {
                self.process_block_vote(block_hash, message.from, false)
            }
            body => self.dispatch_to_consensus(message.from, body),
        } {
            eprintln!("Failed to process message: {e}");
        }
    }

    fn process_client_transaction(&mut self, client_tx: SignedTransaction) -> Result<(), String> {
        client_tx.verify()?;
        let effects = self.consensus.on_client_transaction(client_tx)?;
        self.execute_consensus_effects(effects)
    }

    fn synchronize_transaction(&mut self, verified_tx: VerifiedTransaction) -> Result<(), String> {
        // Verify both client and peer signatures
        verified_tx.verify()?;

        let client_tx = verified_tx.client_tx;
        if let BlockStatus::NewBlockCreated { block_hash } =
            self.effects.stage_client_transaction(client_tx)?
        {
            let effects = self.consensus.on_block_created(block_hash)?;
            self.execute_consensus_effects(effects)?;
        }
        Ok(())
    }

    fn process_block_proposal(
        &mut self,
        block_hash: BlockHash,
        block_file: Vec<u8>,
        signature: Signature,
        public_key: VerifyingKey,
        from: PeerId,
    ) -> Result<(), String> {
        if self.effects.last_completed_block() == block_hash {
            return Ok(());
        }

        let verification_result =
            BlockFile::verify_block_vec(block_hash.clone(), &block_file, signature, public_key);
        let is_ok = match verification_result {
            Ok(block_file) => self.effects.stage_external_block(block_file)?,
            Err(_) => false,
            // Probably, we should call synchronization here if the height
            // is less than block_index - 1
        };
        let effects = self.consensus.on_block_proposal_validated(
            block_hash,
            from,
            is_ok,
            self.effects.known_peers(),
        )?;
        self.execute_consensus_effects(effects)
    }

    fn process_block_vote(
        &mut self,
        block_hash: BlockHash,
        from: PeerId,
        approve: bool,
    ) -> Result<(), String> {
        if self.effects.last_completed_block() != block_hash {
            self.effects.ensure_uncommitted_block(&block_hash)?;
            let effects = self.consensus.on_block_vote(
                block_hash,
                from,
                approve,
                self.effects.known_peers(),
            )?;
            self.execute_consensus_effects(effects)?;
        }
        Ok(())
    }

    pub fn make_vote(
        &mut self,
        block_hash: BlockHash,
        from: PeerId,
        approve: bool,
    ) -> Result<(), String> {
        let effects =
            self.consensus
                .on_block_vote(block_hash, from, approve, self.effects.known_peers())?;
        self.execute_consensus_effects(effects)
    }

    fn dispatch_to_consensus(&mut self, from: PeerId, body: MessageBody) -> Result<(), String> {
        let effects = self.consensus.on_message(from, body, Instant::now())?;
        self.execute_consensus_effects(effects)
    }

    pub fn handle_tick(&mut self, now: Instant) -> Result<(), String> {
        let effects = self.consensus.on_tick(now, self.effects.known_peers())?;
        self.execute_consensus_effects(effects)
    }

    fn execute_consensus_effects(&mut self, effects: Vec<ConsensusEffect>) -> Result<(), String> {
        self.effects.execute_all(&mut self.consensus, effects)?;
        self.consensus_state_tx.send_replace(self.consensus.state());
        Ok(())
    }
}
