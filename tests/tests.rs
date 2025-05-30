#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use ::rustchain::transactions::{AssetType, BlockKeeper, Metadata, Operation, Transaction};
    use rustchain::transactions::TransactionProcessor;

    #[test]
    fn account_state_should_be_restored_from_saved_blocks() {
        let path_to_blocks = PathBuf::from("target/test/data/peer_1");
        fs::remove_dir_all(&path_to_blocks).expect("Failed to remove directory");
        fs::create_dir_all(&path_to_blocks).expect("Failed to create directory");
        let block_keeper = BlockKeeper::new(path_to_blocks.clone(), 3);
        let mut transaction_processor = TransactionProcessor::with_block_keeper(block_keeper);

        let transaction1 = Transaction {
            operation: Operation::AddCoin {
                amount: 10,
                asset_type: AssetType::BTC,
            },
            signature: "signature1".to_string(),
            public_key: "public_key1".to_string(),
            metadata: Metadata {
                timestamp_nanos: 100,
                sequence_number: 1,
            },
        };
        let mut transaction2 = transaction1.clone();
        transaction2.signature = "signature2".to_string();
        transaction2.public_key = "public_key2".to_string();
        let transaction3 = transaction1.clone();

        transaction_processor.process_transaction(transaction1);
        transaction_processor.process_transaction(transaction2);
        transaction_processor.process_transaction(transaction3);
        validate_accounts(&transaction_processor);

        let mut transaction_processor =
            TransactionProcessor::with_block_keeper(BlockKeeper::new(path_to_blocks, 3));
        transaction_processor.read_state();
        validate_accounts(&transaction_processor);
    }

    fn validate_accounts(transaction_processor: &TransactionProcessor) {
        let acc1 = transaction_processor.get_account("public_key1").unwrap();
        assert!(acc1.asset_type == AssetType::BTC);
        assert_eq!(acc1.balance, 20);
        let acc2 = transaction_processor.get_account("public_key2").unwrap();
        assert!(acc2.asset_type == AssetType::BTC);
        assert_eq!(acc2.balance, 10);
    }
}
