use crate::block::{Block, BlockNameParseError, BlockPath, OpenBlock};
use async_trait::async_trait;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncRead, AsyncWrite},
};

/// This trait allows managing block, both opening, closing, and reading their contents.
#[async_trait]
pub trait BlockIO {
    type Writer: AsyncWrite + Unpin + Send;

    async fn open_block(&mut self, id: u64) -> Result<OpenBlock<Self::Writer>, BlockOpenError>;
    async fn close_block(
        &mut self,
        open_block: OpenBlock<Self::Writer>,
    ) -> Result<(), BlockCloseError>;
    async fn block_reader(&self, block: &Block) -> io::Result<Box<dyn AsyncRead + Unpin>>;
    async fn find_blocks(&self) -> Result<Vec<Block>, FindBlocksError>;
}

/// A durable implementation of BlockIO that uses file blocks to persist data..
pub struct FilesystemBlockIO {
    base_path: PathBuf,
}

impl FilesystemBlockIO {
    pub fn new<P: Into<PathBuf>>(base_path: P) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }
}

#[async_trait]
impl BlockIO for FilesystemBlockIO {
    type Writer = File;

    async fn open_block(&mut self, id: u64) -> Result<OpenBlock<Self::Writer>, BlockOpenError> {
        let block_path = self.base_path.join(BlockPath { id }.to_string());
        let block = Block { id };
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&block_path)
            .await?;
        let metadata = file.metadata().await?;

        Ok(OpenBlock::new(block, file, metadata.len() as usize))
    }

    async fn close_block(
        &mut self,
        _open_block: OpenBlock<Self::Writer>,
    ) -> Result<(), BlockCloseError> {
        Ok(())
    }

    async fn block_reader(&self, block: &Block) -> io::Result<Box<dyn AsyncRead + Unpin>> {
        let path = self.base_path.join(block.path().to_string());
        let file = File::open(path).await?;
        Ok(Box::new(file))
    }

    async fn find_blocks(&self) -> Result<Vec<Block>, FindBlocksError> {
        let mut blocks: Vec<Block> = Vec::new();
        let mut dirs = fs::read_dir(&self.base_path).await?;
        while let Some(entry) = dirs.next_entry().await? {
            if entry.file_type().await?.is_file() {
                let file_name = entry.file_name();
                let str_file_name = file_name
                    .to_str()
                    .ok_or(BlockNameParseError::InvalidBlockName)?;
                let block_path: BlockPath = str_file_name.parse()?;
                blocks.push(Block { id: block_path.id });
            }
        }
        blocks.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(blocks)
    }
}

/// An in memory version of BlockIO. This is used mostly for testing.
pub struct NullBlockIO {
    blocks: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl NullBlockIO {
    pub fn blocks_data(&self) -> Arc<Mutex<Vec<Vec<u8>>>> {
        self.blocks.clone()
    }
}

impl Default for NullBlockIO {
    fn default() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl BlockIO for NullBlockIO {
    type Writer = io::Cursor<Vec<u8>>;

    async fn open_block(&mut self, id: u64) -> Result<OpenBlock<Self::Writer>, BlockOpenError> {
        let storage = io::Cursor::new(Vec::new());
        let block = Block { id };
        Ok(OpenBlock::new(block, storage, 0))
    }

    async fn close_block(
        &mut self,
        open_block: OpenBlock<Self::Writer>,
    ) -> Result<(), BlockCloseError> {
        // Save this block for later
        let writer = open_block.into_writer();
        self.blocks.lock().unwrap().push(writer.into_inner());
        Ok(())
    }

    async fn block_reader(&self, block: &Block) -> io::Result<Box<dyn AsyncRead + Unpin>> {
        // Note that this can panic but this is used during testing so #yolo
        let block = self.blocks.lock().unwrap()[block.id as usize].clone();
        Ok(Box::new(io::Cursor::new(block)))
    }

    async fn find_blocks(&self) -> Result<Vec<Block>, FindBlocksError> {
        let blocks = self.blocks.lock().unwrap();
        let blocks = (0..blocks.len())
            .map(|id| Block { id: id as u64 })
            .collect();
        Ok(blocks)
    }
}

#[derive(Error, Debug)]
pub enum StorageCreateError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    BlockNameParse(#[from] BlockNameParseError),
}

#[derive(Error, Debug)]
pub enum FindBlocksError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    BlockNameParse(#[from] BlockNameParseError),
}

#[derive(Error, Debug)]
pub enum BlockOpenError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("a block is already open")]
    BlockAlreadyOpen,
}

#[derive(Error, Debug)]
pub enum BlockCloseError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("block is not currently open")]
    NotOpenBlock,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::io::AsyncReadExt;

    async fn test_block_opens<B: BlockIO>(mut block_io: B) {
        let first_block = block_io.open_block(0).await.unwrap();
        assert_eq!(first_block.block().id, 0);
        block_io.close_block(first_block).await.unwrap();

        let second_block = block_io.open_block(1).await.unwrap();
        assert_eq!(second_block.block().id, 1);
    }

    #[tokio::test]
    async fn open_block() {
        let dir = tempdir().unwrap();
        let block_io = FilesystemBlockIO::new(dir.path());
        test_block_opens(block_io).await;
    }

    #[tokio::test]
    async fn write_and_read() {
        let dir = tempdir().unwrap();
        let mut block_io = FilesystemBlockIO::new(dir.path());
        let mut block = block_io.open_block(0).await.unwrap();
        block.write(b"hello world").await.unwrap();
        block_io.close_block(block).await.unwrap();

        let blocks = block_io.find_blocks().await.unwrap();
        assert_eq!(blocks.len(), 1);
        let mut reader = block_io.block_reader(&blocks[0]).await.unwrap();
        let mut buffer = String::new();
        reader.read_to_string(&mut buffer).await.unwrap();
        assert_eq!(buffer, "hello world");
    }

    #[tokio::test]
    async fn create_from_existing_blocks() {
        let dir = tempdir().unwrap();
        File::create(dir.path().join(BlockPath { id: 0 }.to_string()))
            .await
            .unwrap();
        File::create(dir.path().join(BlockPath { id: 1 }.to_string()))
            .await
            .unwrap();

        let block_io = FilesystemBlockIO::new(dir.path());
        let blocks = block_io.find_blocks().await.unwrap();
        assert_eq!(blocks, &[Block { id: 0 }, Block { id: 1 }]);
    }

    #[tokio::test]
    async fn null_block_io() {
        test_block_opens(NullBlockIO::default()).await;
    }
}
