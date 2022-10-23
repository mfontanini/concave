use lazy_static::lazy_static;
use regex::Regex;
use std::{fmt, str::FromStr};
use thiserror::Error;

#[derive(PartialEq, Debug)]
pub struct BlockPath {
    pub id: u64,
}

impl FromStr for BlockPath {
    type Err = BlockNameParseError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r#"kv-([\d]+)\.block$"#).unwrap();
        }
        match RE.captures_iter(name).next() {
            Some(id) => {
                let id: u64 = id.get(1).unwrap().as_str().parse().map_err(|_| Self::Err::InvalidBlockIndex)?;
                Ok(BlockPath { id })
            }
            None => Err(Self::Err::InvalidBlockName),
        }
    }
}

impl fmt::Display for BlockPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "kv-{}.block", self.id)
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum BlockNameParseError {
    #[error("invalid block name")]
    InvalidBlockName,

    #[error("invalid block index")]
    InvalidBlockIndex,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Block {
    pub id: u64,
    pub size: u64,
}

impl Block {
    pub fn new(id: u64) -> Self {
        Self { id, size: 0 }
    }

    pub fn existing(id: u64, size: u64) -> Self {
        Self { id, size }
    }

    pub fn path(&self) -> BlockPath {
        BlockPath { id: self.id }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_block_path() {
        assert_eq!(BlockPath::from_str("kv-0.block"), Ok(BlockPath { id: 0 }));
        assert_eq!(BlockPath::from_str("kv-1.block"), Ok(BlockPath { id: 1 }));
        assert_eq!(BlockPath::from_str("kv-1337.block"), Ok(BlockPath { id: 1337 }));
        assert_eq!(BlockPath::from_str("/tmp/some/path/kv-42.block"), Ok(BlockPath { id: 42 }));

        assert_eq!(BlockPath::from_str("kv-1337.blocks"), Err(BlockNameParseError::InvalidBlockName));
        assert_eq!(BlockPath::from_str("kv-a.block"), Err(BlockNameParseError::InvalidBlockName));
        assert_eq!(BlockPath::from_str("kv-18446744073709551616.block"), Err(BlockNameParseError::InvalidBlockIndex));
    }
}
