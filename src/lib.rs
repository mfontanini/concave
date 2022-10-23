use serde_derive::{Deserialize, Serialize};
use std::fmt;

pub mod block;
pub mod codec;
pub mod io;
pub mod kv;
pub mod storage;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Object {
    pub key: String,
    pub value: ObjectValue,
    pub version: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
// #[serde(untagged)]
pub enum ObjectValue {
    String(String),
    Number(i64),
    Bytes(Vec<u8>),
    Float(f64),
    Bool(bool),
}

impl fmt::Display for ObjectValue {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::String(value) => write!(f, "{}", value),
            Self::Number(value) => write!(f, "{}", value),
            Self::Bytes(value) => write!(f, "{:?}", value),
            Self::Float(value) => write!(f, "{}", value),
            Self::Bool(value) => write!(f, "{}", value),
        }
    }
}

impl Object {
    pub fn new<K: Into<String>, V: Into<ObjectValue>>(key: K, value: V) -> Self {
        Self::versioned(key, value, 0)
    }

    pub fn versioned<K: Into<String>, V: Into<ObjectValue>>(key: K, value: V, version: u32) -> Self {
        Self { key: key.into(), value: value.into(), version }
    }
}

impl From<String> for ObjectValue {
    fn from(value: String) -> ObjectValue {
        ObjectValue::String(value)
    }
}

impl From<&str> for ObjectValue {
    fn from(value: &str) -> ObjectValue {
        ObjectValue::String(value.into())
    }
}

impl From<i64> for ObjectValue {
    fn from(value: i64) -> ObjectValue {
        ObjectValue::Number(value)
    }
}

impl From<Vec<u8>> for ObjectValue {
    fn from(value: Vec<u8>) -> ObjectValue {
        ObjectValue::Bytes(value)
    }
}

impl From<f64> for ObjectValue {
    fn from(value: f64) -> ObjectValue {
        ObjectValue::Float(value)
    }
}

impl From<bool> for ObjectValue {
    fn from(value: bool) -> ObjectValue {
        ObjectValue::Bool(value)
    }
}
