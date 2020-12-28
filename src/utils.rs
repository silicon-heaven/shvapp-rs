use sha1::{Sha1, Digest};

pub fn sha1_password_hash(password: &[u8], nonce: &[u8]) -> String {
    let mut hasher = Sha1::new();
    let mut hash = password.to_vec();
    if password.len() != 40 {
        hasher.update(hash);
        let result = hasher.finalize();
        hash = hex::encode(&result[..]).as_bytes().to_vec();
    }
    let mut nonce = nonce.to_vec();
    nonce.append(&mut hash);

    let mut hasher = Sha1::new();
    hasher.update(&nonce);
    let result = hasher.finalize();
    let hash = hex::encode(&result[..]);
    hash
}

pub fn split_shv_path(path: &str) -> Vec<&str> {
    let v = path.split('/')
        .filter(|s| !(*s).is_empty())
        .collect();
    return v
}

pub fn join_shv_path(path: &[&str]) -> String {
    let mut s = String::new();
    for p in path {
        if !s.is_empty() {
            s.push('/');
        }
        s.push_str(*p);
    }
    s
}