use lazy_static::lazy_static;
use regex::Regex;
use std::fmt;
use std::io;
use std::str::FromStr;
use thiserror::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};

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
                let id: u64 = id
                    .get(1)
                    .unwrap()
                    .as_str()
                    .parse()
                    .map_err(|_| Self::Err::InvalidBlockIndex)?;
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
}

impl Block {
    pub fn path(&self) -> BlockPath {
        BlockPath { id: self.id }
    }
}

pub struct OpenBlock<W: AsyncWrite + Unpin + Send> {
    block: Block,
    writer: BufWriter<W>,
    size: usize,
}

impl<W: AsyncWrite + Unpin + Send> OpenBlock<W> {
    pub fn new(block: Block, writer: W, size: usize) -> Self {
        Self {
            block,
            writer: BufWriter::new(writer),
            size,
        }
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<usize, io::Error> {
        self.writer.write(data).await?;
        self.writer.flush().await?;
        self.size += data.len();
        Ok(self.size)
    }

    pub fn block(&self) -> &Block {
        &self.block
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn into_writer(self) -> W {
        self.writer.into_inner()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;
    use tokio::fs::File;

    #[test]
    fn parse_block_path() {
        assert_eq!(BlockPath::from_str("kv-0.block"), Ok(BlockPath { id: 0 }));
        assert_eq!(BlockPath::from_str("kv-1.block"), Ok(BlockPath { id: 1 }));
        assert_eq!(
            BlockPath::from_str("kv-1337.block"),
            Ok(BlockPath { id: 1337 })
        );
        assert_eq!(
            BlockPath::from_str("/tmp/some/path/kv-42.block"),
            Ok(BlockPath { id: 42 })
        );

        assert_eq!(
            BlockPath::from_str("kv-1337.blocks"),
            Err(BlockNameParseError::InvalidBlockName)
        );
        assert_eq!(
            BlockPath::from_str("kv-a.block"),
            Err(BlockNameParseError::InvalidBlockName)
        );
        assert_eq!(
            BlockPath::from_str("kv-18446744073709551616.block"),
            Err(BlockNameParseError::InvalidBlockIndex)
        );
    }

    #[tokio::test]
    async fn open_block_write() {
        let file = tempfile().unwrap();
        let mut open_block = OpenBlock::new(Block { id: 0 }, File::from_std(file), 0);
        assert_eq!(
            open_block.write("hello world, ".as_bytes()).await.unwrap(),
            13
        );
        assert_eq!(open_block.write("bye!".as_bytes()).await.unwrap(), 17);
        assert_eq!(open_block.size(), 17);
    }
}
