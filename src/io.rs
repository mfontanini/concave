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

    async fn open_block(&mut self) -> Result<OpenBlock<Self::Writer>, BlockOpenError>;
    async fn close_block(
        &mut self,
        open_block: OpenBlock<Self::Writer>,
    ) -> Result<(), BlockCloseError>;
    async fn block_reader(&self, block: &Block) -> io::Result<Box<dyn AsyncRead + Unpin>>;
    fn existing_blocks(&self) -> Vec<Block>;
}

#[derive(PartialEq, Debug, Clone)]
enum BlockWritingState {
    Open { id: u64 },
    Closed,
}

/// A durable implementation of BlockIO that uses file blocks to persist data.
///
/// Upon creation, the storage directory is scanned to find existing blocks. The last existing block
/// will be open in append more and written to.
pub struct FilesystemBlockIO {
    base_path: PathBuf,
    blocks: Vec<Block>,
    next_block_id: u64,
    state: BlockWritingState,
}

impl FilesystemBlockIO {
    pub async fn new<P: Into<PathBuf>>(base_path: P) -> Result<Self, StorageCreateError> {
        let base_path: PathBuf = base_path.into();
        let mut blocks: Vec<Block> = Vec::new();
        let mut dirs = fs::read_dir(&base_path).await?;
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
        let next_block_id = blocks.last().map(|block| block.id).unwrap_or(0);
        Ok(Self {
            base_path,
            blocks,
            next_block_id,
            state: BlockWritingState::Closed,
        })
    }

    pub fn blocks(&self) -> &[Block] {
        &self.blocks
    }
}

#[async_trait]
impl BlockIO for FilesystemBlockIO {
    type Writer = File;

    async fn open_block(&mut self) -> Result<OpenBlock<Self::Writer>, BlockOpenError> {
        if matches!(self.state, BlockWritingState::Open { .. }) {
            return Err(BlockOpenError::BlockAlreadyOpen);
        }
        let block_id = self.next_block_id;
        let block_path = self.base_path.join(BlockPath { id: block_id }.to_string());
        let block = Block { id: block_id };
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&block_path)
            .await?;
        let metadata = file.metadata().await?;

        self.state = BlockWritingState::Open { id: block_id };
        self.next_block_id += 1;
        Ok(OpenBlock::new(block, file, metadata.len() as usize))
    }

    async fn close_block(
        &mut self,
        open_block: OpenBlock<Self::Writer>,
    ) -> Result<(), BlockCloseError> {
        let expected_state = BlockWritingState::Open {
            id: open_block.block().id,
        };
        if self.state != expected_state {
            return Err(BlockCloseError::NotOpenBlock);
        }
        self.state = BlockWritingState::Closed;
        self.blocks.push(open_block.block().clone());
        Ok(())
    }

    async fn block_reader(&self, block: &Block) -> io::Result<Box<dyn AsyncRead + Unpin>> {
        let path = self.base_path.join(block.path().to_string());
        let file = File::open(path).await?;
        Ok(Box::new(file))
    }

    fn existing_blocks(&self) -> Vec<Block> {
        self.blocks.clone()
    }
}

/// An in memory version of BlockIO. This is used mostly for testing.
pub struct NullBlockIO {
    state: BlockWritingState,
    next_block_id: u64,
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
            state: BlockWritingState::Closed,
            next_block_id: 0,
            blocks: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl BlockIO for NullBlockIO {
    type Writer = io::Cursor<Vec<u8>>;

    async fn open_block(&mut self) -> Result<OpenBlock<Self::Writer>, BlockOpenError> {
        if matches!(self.state, BlockWritingState::Open { .. }) {
            return Err(BlockOpenError::BlockAlreadyOpen);
        }
        let block_id = self.next_block_id;
        let storage = io::Cursor::new(Vec::new());
        let block = Block { id: block_id };
        self.next_block_id += 1;
        self.state = BlockWritingState::Open { id: block_id };
        Ok(OpenBlock::new(block, storage, 0))
    }

    async fn close_block(
        &mut self,
        open_block: OpenBlock<Self::Writer>,
    ) -> Result<(), BlockCloseError> {
        let expected_state = BlockWritingState::Open {
            id: open_block.block().id,
        };
        if self.state != expected_state {
            return Err(BlockCloseError::NotOpenBlock);
        }
        self.state = BlockWritingState::Closed;
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

    fn existing_blocks(&self) -> Vec<Block> {
        let blocks = self.blocks.lock().unwrap();
        (0..blocks.len())
            .map(|id| Block { id: id as u64 })
            .collect()
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
        let first_block = block_io.open_block().await.unwrap();
        assert_eq!(first_block.block().id, 0);
        block_io.close_block(first_block).await.unwrap();

        let second_block = block_io.open_block().await.unwrap();
        assert_eq!(second_block.block().id, 1);

        // Shouldn't be allowed to open while another block is open
        assert!(block_io.open_block().await.is_err());
    }

    #[tokio::test]
    async fn open_block() {
        let dir = tempdir().unwrap();
        let block_io = FilesystemBlockIO::new(dir.path()).await.unwrap();
        test_block_opens(block_io).await;
    }

    #[tokio::test]
    async fn write_and_read() {
        let dir = tempdir().unwrap();
        let mut block_io = FilesystemBlockIO::new(dir.path()).await.unwrap();
        let mut block = block_io.open_block().await.unwrap();
        block.write(b"hello world").await.unwrap();
        block_io.close_block(block).await.unwrap();

        let blocks = block_io.existing_blocks();
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

        let mut block_io = FilesystemBlockIO::new(dir.path()).await.unwrap();
        assert_eq!(block_io.blocks(), &[Block { id: 0 }, Block { id: 1 }]);

        // The first open block should be the last written one, as we assume it's partially completed
        let first_block = block_io.open_block().await.unwrap();
        assert_eq!(first_block.block().id, 1);
    }

    #[tokio::test]
    async fn null_block_io() {
        test_block_opens(NullBlockIO::default()).await;
    }
}
