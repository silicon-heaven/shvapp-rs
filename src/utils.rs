use sha1::{Sha1, Digest};

pub fn sha1_password_hash(password: &str, nonce: &str) -> String {
    let mut hasher = Sha1::new();
    let mut hash = password.as_bytes().to_vec();
    if password.len() != 40 {
        hasher.update(hash);
        let result = hasher.finalize();
        hash = hex::encode(&result[..]).as_bytes().to_vec();
    }
    let mut nonce = nonce.as_bytes().to_vec();
    nonce.append(&mut hash);

    let mut hasher = Sha1::new();
    hasher.update(&nonce);
    let result = hasher.finalize();
    let hash = hex::encode(&result[..]);
    hash
}