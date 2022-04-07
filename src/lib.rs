use serde_derive::{Deserialize, Serialize};

pub mod block;
pub mod io;
pub mod kv;
pub mod storage;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Object {
    pub key: String,
    pub value: String,
    pub version: u32,
}

impl Object {
    pub fn new<K: Into<String>, V: Into<String>>(key: K, value: V) -> Self {
        Self::versioned(key, value, 0)
    }

    pub fn versioned<K: Into<String>, V: Into<String>>(key: K, value: V, version: u32) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            version,
        }
    }
}
