use crate::block::{Block, BlockNameParseError, BlockPath};
use async_trait::async_trait;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tempfile::{NamedTempFile, TempPath};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncRead, AsyncWrite},
};

#[async_trait]
pub trait TemporaryBlock {
    async fn write(&mut self, data: &[u8]) -> io::Result<()>;
}

/// A block that is currently open and can be actively written to
pub struct OpenBlock<W: AsyncWrite + Unpin + Send> {
    block: Block,
    writer: W,
}

impl<W: AsyncWrite + Unpin + Send> OpenBlock<W> {
    pub fn new(block: Block, writer: W) -> Self {
        Self { block, writer }
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<(), io::Error> {
        self.writer.write_all(data).await?;
        self.writer.flush().await?;
        self.block.size += data.len() as u64;
        Ok(())
    }

    pub fn block(&self) -> &Block {
        &self.block
    }

    pub fn size(&self) -> u64 {
        self.block.size
    }

    pub fn into_writer(self) -> W {
        self.writer
    }
}

/// This trait allows managing block, both opening, closing, and reading their contents.
#[async_trait]
pub trait BlockIO {
    type Writer: AsyncWrite + Unpin + Send;
    type Temporary: TemporaryBlock + Unpin + Send;

    async fn open_block(&self, id: u64) -> Result<OpenBlock<Self::Writer>, BlockOpenError>;
    async fn close_block(&self, open_block: OpenBlock<Self::Writer>)
        -> Result<(), BlockCloseError>;
    async fn block_reader(&self, id: u64) -> io::Result<Box<dyn AsyncRead + Unpin + Send>>;
    async fn find_blocks(&self) -> Result<Vec<Block>, FindBlocksError>;
    async fn temporary_block(&self) -> io::Result<Self::Temporary>;
    async fn replace_block(&self, id: u64, temporary: Self::Temporary) -> io::Result<()>;
    async fn drop_block(&self, id: u64) -> io::Result<()>;
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
    type Temporary = FilesystemTemporaryBlock;

    async fn open_block(&self, id: u64) -> Result<OpenBlock<Self::Writer>, BlockOpenError> {
        let block_path = self.base_path.join(BlockPath { id }.to_string());
        let mut block = Block::new(id);
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&block_path)
            .await?;
        let metadata = file.metadata().await?;
        block.size = metadata.len();

        Ok(OpenBlock::new(block, file))
    }

    async fn close_block(
        &self,
        _open_block: OpenBlock<Self::Writer>,
    ) -> Result<(), BlockCloseError> {
        Ok(())
    }

    async fn block_reader(&self, id: u64) -> io::Result<Box<dyn AsyncRead + Unpin + Send>> {
        let path = self.base_path.join(BlockPath { id }.to_string());
        let file = File::open(path).await?;
        Ok(Box::new(file))
    }

    async fn find_blocks(&self) -> Result<Vec<Block>, FindBlocksError> {
        let mut blocks: Vec<Block> = Vec::new();
        let mut dirs = fs::read_dir(&self.base_path).await?;
        while let Some(entry) = dirs.next_entry().await? {
            if entry.file_type().await?.is_file() {
                let file_name = entry.file_name();
                let metadata = entry.metadata().await?;
                let str_file_name = file_name
                    .to_str()
                    .ok_or(BlockNameParseError::InvalidBlockName)?;
                let block_path: BlockPath = str_file_name.parse()?;
                blocks.push(Block::existing(block_path.id, metadata.len()));
            }
        }
        blocks.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(blocks)
    }

    async fn temporary_block(&self) -> io::Result<Self::Temporary> {
        let (file, path) = NamedTempFile::new_in(&self.base_path)?.into_parts();
        let file = File::from_std(file);
        Ok(FilesystemTemporaryBlock { file, path })
    }

    async fn replace_block(&self, id: u64, temporary: Self::Temporary) -> io::Result<()> {
        let path = self.base_path.join(BlockPath { id }.to_string());
        temporary.persist(&path)?;
        Ok(())
    }

    async fn drop_block(&self, id: u64) -> io::Result<()> {
        let path = self.base_path.join(BlockPath { id }.to_string());
        fs::remove_file(path).await
    }
}

pub struct FilesystemTemporaryBlock {
    file: File,
    path: TempPath,
}

impl FilesystemTemporaryBlock {
    fn persist(self, destination: &Path) -> io::Result<()> {
        // Note: not async, maybe replace with some async equivalent at some point
        self.path.persist(destination)?;
        Ok(())
    }
}

#[async_trait]
impl TemporaryBlock for FilesystemTemporaryBlock {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.file.write_all(data).await
    }
}

#[derive(Default)]
pub struct MemoryTemporaryBlock {
    contents: Vec<u8>,
}

#[async_trait]
impl TemporaryBlock for MemoryTemporaryBlock {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.contents.extend(data);
        Ok(())
    }
}

#[derive(Clone)]
struct MemoryBlock {
    id: u64,
    data: Vec<u8>,
}

/// An in-memory version of BlockIO. This is used for testing purposes only, and therefore
/// a bit fragile.
pub struct MemoryBlockIO {
    blocks: Arc<Mutex<Vec<MemoryBlock>>>,
}

impl MemoryBlockIO {
    fn block_index(&self, id: u64) -> usize {
        let blocks = self.blocks.lock().unwrap();
        for (index, block) in blocks.iter().enumerate() {
            if block.id == id {
                return index;
            }
        }
        panic!("Failed to find index for block {id}");
    }
}

impl Default for MemoryBlockIO {
    fn default() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl BlockIO for MemoryBlockIO {
    type Writer = io::Cursor<Vec<u8>>;
    type Temporary = MemoryTemporaryBlock;

    async fn open_block(&self, id: u64) -> Result<OpenBlock<Self::Writer>, BlockOpenError> {
        let storage = io::Cursor::new(Vec::new());
        let block = Block::new(id);
        Ok(OpenBlock::new(block, storage))
    }

    async fn close_block(
        &self,
        open_block: OpenBlock<Self::Writer>,
    ) -> Result<(), BlockCloseError> {
        let id = open_block.block().id;
        let writer = open_block.into_writer();
        let block = MemoryBlock {
            id,
            data: writer.into_inner(),
        };
        self.blocks.lock().unwrap().push(block);
        Ok(())
    }

    async fn block_reader(&self, id: u64) -> io::Result<Box<dyn AsyncRead + Unpin + Send>> {
        let index = self.block_index(id);
        let block = self.blocks.lock().unwrap()[index].clone();
        Ok(Box::new(io::Cursor::new(block.data)))
    }

    async fn find_blocks(&self) -> Result<Vec<Block>, FindBlocksError> {
        let blocks = self.blocks.lock().unwrap();
        let blocks = blocks
            .iter()
            .map(|memory_block| Block {
                id: memory_block.id,
                size: memory_block.data.len() as u64,
            })
            .collect();
        Ok(blocks)
    }

    async fn temporary_block(&self) -> io::Result<Self::Temporary> {
        Ok(MemoryTemporaryBlock::default())
    }

    async fn replace_block(&self, id: u64, temporary: Self::Temporary) -> io::Result<()> {
        let index = self.block_index(id);
        let mut blocks = self.blocks.lock().unwrap();
        // Note that this can panic but this is used during testing only so #yolo
        blocks[index].data = temporary.contents;
        Ok(())
    }

    async fn drop_block(&self, id: u64) -> io::Result<()> {
        let index = self.block_index(id);
        let mut blocks = self.blocks.lock().unwrap();
        blocks.remove(index);
        Ok(())
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
    use tempfile::tempfile;
    use tokio::io::AsyncReadExt;
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    async fn test_block_opens<B: BlockIO>(block_io: B) -> TestResult {
        let first_block = block_io.open_block(0).await?;
        assert_eq!(first_block.block().id, 0);
        block_io.close_block(first_block).await?;

        let second_block = block_io.open_block(1).await?;
        assert_eq!(second_block.block().id, 1);
        Ok(())
    }

    #[tokio::test]
    async fn open_block() -> TestResult {
        let dir = tempdir()?;
        let block_io = FilesystemBlockIO::new(dir.path());
        test_block_opens(block_io).await
    }

    #[tokio::test]
    async fn temporary_block() -> TestResult {
        let dir = tempdir()?;
        let block_io = FilesystemBlockIO::new(dir.path());
        let mut block = block_io.open_block(0).await?;
        block.write("hello world".as_bytes()).await?;
        block_io.close_block(block).await?;

        let mut temporary = block_io.temporary_block().await?;
        temporary.write("bye world".as_bytes()).await?;
        block_io.replace_block(0, temporary).await?;

        // Ensure contents were overwritten
        let mut reader = block_io.block_reader(0).await?;
        let mut buffer = String::new();
        reader.read_to_string(&mut buffer).await?;
        assert_eq!(buffer, "bye world");

        // Ensure no other file is there
        assert_eq!(block_io.find_blocks().await?.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn drop_block() -> TestResult {
        let dir = tempdir()?;
        let block_io = FilesystemBlockIO::new(dir.path());
        let mut block = block_io.open_block(0).await?;
        assert_eq!(block.size(), 0);
        block.write("hello world".as_bytes()).await?;
        block_io.close_block(block).await?;

        // Drop it and expect no files to be there
        block_io.drop_block(0).await?;
        assert_eq!(block_io.find_blocks().await?.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn write_and_read() -> TestResult {
        let dir = tempdir()?;
        let block_io = FilesystemBlockIO::new(dir.path());
        let mut block = block_io.open_block(0).await?;
        block.write(b"hello world").await?;
        block_io.close_block(block).await?;

        let blocks = block_io.find_blocks().await?;
        assert_eq!(blocks.len(), 1);
        let mut reader = block_io.block_reader(0).await?;
        let mut buffer = String::new();
        reader.read_to_string(&mut buffer).await?;
        assert_eq!(buffer, "hello world");
        assert_eq!(blocks[0].size, buffer.len() as u64);
        assert_eq!(block_io.open_block(0).await?.size(), buffer.len() as u64);
        Ok(())
    }

    #[tokio::test]
    async fn create_from_existing_blocks() -> TestResult {
        let dir = tempdir()?;
        File::create(dir.path().join(BlockPath { id: 0 }.to_string())).await?;
        File::create(dir.path().join(BlockPath { id: 1 }.to_string())).await?;

        let block_io = FilesystemBlockIO::new(dir.path());
        let blocks = block_io.find_blocks().await?;
        assert_eq!(blocks, &[Block::new(0), Block::new(1)]);
        Ok(())
    }

    #[tokio::test]
    async fn null_block_io() -> TestResult {
        test_block_opens(MemoryBlockIO::default()).await
    }

    #[tokio::test]
    async fn open_block_write() -> TestResult {
        let file = tempfile()?;
        let mut open_block = OpenBlock::new(Block::new(0), File::from_std(file));
        open_block.write("hello world, ".as_bytes()).await?;
        open_block.write("bye!".as_bytes()).await?;
        assert_eq!(open_block.size(), 17);
        Ok(())
    }
}
