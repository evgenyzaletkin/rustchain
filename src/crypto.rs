use k256::ecdsa::signature::{Signer, Verifier};
use k256::ecdsa::{Signature, SigningKey, VerifyingKey, signature};
use serde::Serialize;
use std::fs;
use std::path::PathBuf;

pub struct KeyManager {}

const KEY_FILE_NAME: &str = "private_key.bin";

pub trait Signable: Serialize {}

impl KeyManager {
    pub fn get_or_create_key(key_dir: &PathBuf) -> SigningKey {
        fs::create_dir_all(&key_dir)
            .expect(format!("Failed to create directory {key_dir:?}").as_str());
        let key_file_path = key_dir.join(KEY_FILE_NAME);
        if let Some(key_bytes) = fs::read(&key_file_path).ok() {
            let key_array: [u8; 32] = key_bytes.try_into().expect("Invalid key length");
            SigningKey::from_bytes(&key_array.into()).expect("Invalid key data")
        } else {
            let key = Self::create_key();
            let key_file = key.to_bytes();
            fs::write(key_file_path, key_file).expect("Failed to write key file");
            key
        }
    }

    pub fn create_key() -> SigningKey {
        SigningKey::random(&mut rand::rng())
    }

    pub fn public_key_to_bytes(key: &VerifyingKey) -> [u8; 33] {
        key.to_encoded_point(true).as_bytes().try_into().unwrap()
    }

    pub fn bytes_to_key(key_bytes: &[u8; 33]) -> Result<VerifyingKey, signature::Error> {
        VerifyingKey::from_sec1_bytes(key_bytes)
    }

    pub fn to_string_hex(verifying_key: &VerifyingKey) -> String {
        hex::encode(verifying_key.to_sec1_bytes())
    }

    pub fn sign_message<T: Signable>(key: &SigningKey, signable: &T) -> Signature {
        let message = serde_json::to_vec(signable).unwrap();
        key.sign(&message)
    }

    pub fn verify_message(
        public_key: &VerifyingKey,
        signature: &Signature,
        message: &Vec<u8>,
    ) -> Result<(), signature::Error> {
        public_key.verify(message, signature)?;
        Ok(())
    }
}
