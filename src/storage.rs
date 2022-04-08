use crate::{
    block::{Block, OpenBlock},
    io::{BlockCloseError, BlockIO, BlockOpenError, FindBlocksError},
    Object,
};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use log::{error, info, warn};
use prost::Message;
use std::collections::HashMap;
use std::io;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::spawn;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use tokio::time::timeout;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/storage.rs"));
}

/// The result of a write operation
#[derive(Debug, Clone)]
pub struct WriteResult {
    pub success: bool,
}

impl WriteResult {
    pub fn success() -> Self {
        Self { success: true }
    }

    pub fn failure() -> Self {
        Self { success: false }
    }
}

/// A request to write objects.
#[derive(Debug)]
pub struct WriteRequest {
    /// The objects to be written.
    pub objects: Vec<Object>,

    /// A oneshot channel that will be fulfilled when this write is completed.
    pub notifier: oneshot::Sender<WriteResult>,
}

impl WriteRequest {
    pub fn new(objects: Vec<Object>, notifier: oneshot::Sender<WriteResult>) -> Self {
        Self { objects, notifier }
    }
}

pub struct StorageConfig {
    pub batch_time: Duration,
    pub max_batch_size: usize,
    pub max_block_size: usize,
}

/// This is the glue between writers and the underlying filesystem.
pub struct Storage<B: BlockIO + Send + Sync + 'static> {
    writer_context: WriterContext,
    block_io: Arc<B>,
}

impl<B: BlockIO + Send + Sync + 'static> Storage<B> {
    pub async fn new(block_io: Arc<B>, config: StorageConfig) -> Result<Self, StorageCreateError> {
        let writer_context = StorageWriter::new(block_io.clone(), config).await?.launch();
        Ok(Self {
            block_io,
            writer_context,
        })
    }

    /// Schedule a write. The oneshot channel in the write request will be fulfilled when the
    /// write is completed.
    pub async fn write(&self, request: WriteRequest) -> Result<(), WriteError> {
        let result = self.writer_context.request_sender.send(request).await;
        if result.is_err() {
            return Err(WriteError::ScheduleFailure);
        }
        Ok(())
    }

    /// Reads all objects in a list of blocks. Blocks are assumed to be in order, meaning if key X
    /// shows up in block N and N+1, then its version in N+1 is assumed to be greater than the one in N.
    pub async fn read_blocks(
        &self,
        blocks: &[Block],
    ) -> Result<HashMap<String, Object>, ReadError> {
        let mut objects = HashMap::new();
        for block in blocks {
            info!("Processing block {}", block.id);
            let reader = self.block_io.block_reader(block).await?;
            let mut stream = Box::pin(iter_objects(reader));
            while let Some(object) = stream.next().await {
                let object = object?;
                objects.insert(object.key.clone(), object);
            }
        }
        Ok(objects)
    }

    pub fn block_io(&self) -> Arc<B> {
        self.block_io.clone()
    }
}

#[derive(Debug, Error)]
pub enum StorageCreateError {
    #[error("failed to find blocks: {0}")]
    FindBlocks(#[from] FindBlocksError),
}

#[derive(Error, Debug)]
pub enum ReadError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("malformed stream")]
    MalformedStream(#[from] prost::DecodeError),
}

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("failed to schedule write")]
    ScheduleFailure,
}

/// This is the type doing the heavy work: this batches data, manages BlockIOs, etc.
struct StorageWriter<B: BlockIO> {
    block_io: Arc<B>,
    closed_blocks: Vec<Block>,
    next_block_id: u64,
    config: StorageConfig,
}

impl<B: BlockIO + Send + Sync + 'static> StorageWriter<B> {
    pub async fn new(block_io: Arc<B>, config: StorageConfig) -> Result<Self, StorageCreateError> {
        let closed_blocks = block_io.find_blocks().await?;
        let next_block_id = closed_blocks.last().map(|block| block.id).unwrap_or(0);
        Ok(Self {
            block_io,
            closed_blocks,
            next_block_id,
            config,
        })
    }

    /// Launches the writer. The sender in the returned context can be used to schedule write requests.
    pub fn launch(self) -> WriterContext {
        let running = Arc::new(AtomicBool::new(true));
        let (request_sender, receiver) = channel(100);
        spawn({
            let running = running.clone();
            async move {
                match self.write_loop(receiver, running).await {
                    Ok(_) => info!("Writer loop done"),
                    Err(e) => error!("Error on write loop: {e}"),
                };
            }
        });
        WriterContext::new(request_sender, running)
    }

    async fn write_loop(
        mut self,
        mut receiver: Receiver<WriteRequest>,
        running: Arc<AtomicBool>,
    ) -> Result<(), WriteLoopError> {
        let mut open_block = self.open_block().await?;
        while running.load(Ordering::Acquire) {
            let batch = self.build_batch(&mut receiver).await?;
            open_block = self.write_batch(open_block, &batch).await?;
            Self::notify_all(batch.notifiers, WriteResult::success()).await;
        }
        Ok(())
    }

    async fn build_batch(
        &self,
        receiver: &mut Receiver<WriteRequest>,
    ) -> Result<WriteBatch, WriteLoopError> {
        let mut batch = WriteBatch::default();
        let deadline = Instant::now() + self.config.batch_time;
        let mut time_remaining = self.config.batch_time;
        loop {
            let request = match timeout(time_remaining, receiver.recv()).await {
                Ok(Some(request)) => request,
                Ok(None) => {
                    Self::notify_all(batch.notifiers, WriteResult::failure()).await;
                    return Err(WriteLoopError::Disconnected);
                }
                Err(_) if batch.data.is_empty() => {
                    // If we don't have any data, just keep looping
                    time_remaining = self.config.batch_time;
                    continue;
                }
                Err(_) => break,
            };
            batch = Self::add_to_batch(request, batch)?;
            if batch.data.len() >= self.config.max_batch_size {
                info!("Batch reached max configured size, writing it");
                break;
            }
            time_remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(duration) => duration,
                None => break,
            };
        }
        Ok(batch)
    }

    fn add_to_batch(request: WriteRequest, mut batch: WriteBatch) -> io::Result<WriteBatch> {
        for object in request.objects {
            proto::Object::from(object).encode_length_delimited(&mut batch.data)?;
        }
        batch.notifiers.push(request.notifier);
        Ok(batch)
    }

    async fn write_batch(
        &mut self,
        mut open_block: OpenBlock<B::Writer>,
        batch: &WriteBatch,
    ) -> Result<OpenBlock<B::Writer>, WriteLoopError> {
        open_block.write(&batch.data).await?;
        if open_block.size() >= self.config.max_block_size {
            self.close_block(open_block).await?;
            open_block = self.open_block().await?;
        }
        Ok(open_block)
    }

    async fn open_block(&mut self) -> Result<OpenBlock<B::Writer>, BlockOpenError> {
        let block = self.block_io.open_block(self.next_block_id).await?;
        self.next_block_id += 1;
        Ok(block)
    }

    async fn close_block(
        &mut self,
        open_block: OpenBlock<B::Writer>,
    ) -> Result<(), BlockCloseError> {
        let block = open_block.block().clone();
        self.block_io.close_block(open_block).await?;
        self.closed_blocks.push(block);
        Ok(())
    }

    async fn notify_all(notifiers: Vec<oneshot::Sender<WriteResult>>, result: WriteResult) {
        for notifier in notifiers {
            if let Err(e) = notifier.send(result.clone()) {
                warn!("Could not send response to notifier: {e:?}");
            }
        }
    }
}

fn iter_objects<R: AsyncRead + Unpin>(reader: R) -> impl Stream<Item = Result<Object, ReadError>> {
    let mut reader = BufReader::new(reader);

    try_stream! {
        loop {
            let buffer = reader.fill_buf().await?;
            if buffer.is_empty() {
                break;
            }
            let object = proto::Object::decode_length_delimited(buffer)?;
            // TODO: this +1 needs to be tweaked (will break for larger objects)
            reader.consume(object.encoded_len() + 1);

            yield Object {
                key: object.key,
                value: object.value,
                version: object.version,
            };
        }
    }
}

#[derive(Error, Debug)]
enum WriteLoopError {
    #[error("receiver disconnected")]
    Disconnected,

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    BlockOpen(#[from] BlockOpenError),

    #[error(transparent)]
    BlockClose(#[from] BlockCloseError),
}

#[derive(Default)]
struct WriteBatch {
    /// The data to be included in this batch
    data: Vec<u8>,

    /// The senders to notify all of the pending writers
    notifiers: Vec<oneshot::Sender<WriteResult>>,
}

struct WriterContext {
    request_sender: Sender<WriteRequest>,
    running: Arc<AtomicBool>,
}

impl WriterContext {
    fn new(request_sender: Sender<WriteRequest>, running: Arc<AtomicBool>) -> Self {
        Self {
            request_sender,
            running,
        }
    }

    fn stop(&mut self) -> bool {
        self.running.swap(false, Ordering::AcqRel)
    }
}

impl Drop for WriterContext {
    fn drop(&mut self) {
        self.stop();
    }
}

impl From<Object> for proto::Object {
    fn from(object: Object) -> proto::Object {
        proto::Object {
            key: object.key,
            value: object.value,
            version: object.version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::NullBlockIO;
    use std::sync::Arc;

    #[ctor::ctor]
    fn init() {
        env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .init();
    }

    fn make_config(max_batch_size: usize, batch_time: Duration) -> StorageConfig {
        StorageConfig {
            batch_time,
            max_batch_size,
            max_block_size: 10,
        }
    }

    async fn make_storage(max_batch_size: usize, batch_time: Duration) -> Storage<NullBlockIO> {
        let block_io = Arc::new(NullBlockIO::default());
        Storage::new(block_io, make_config(max_batch_size, batch_time))
            .await
            .unwrap()
    }

    async fn make_instant_storage() -> Storage<NullBlockIO> {
        // A storage that will immediately dump a batch request
        make_storage(1, Duration::from_millis(0)).await
    }

    async fn make_buffered_storage() -> Storage<NullBlockIO> {
        // A storage that will attempt to buffer writes a bit
        make_storage(100, Duration::from_millis(100)).await
    }

    #[tokio::test]
    async fn launch_write_loop() {
        // Simply create it and implicitly join to make sure we don't get stuck
        let _storage = make_instant_storage().await;
    }

    #[tokio::test]
    async fn write_batch() {
        let storage = make_buffered_storage().await;
        let batch_objects = vec![
            Object::new("my key", "and its value"),
            Object::new("another key", "another value"),
        ];
        let (notifier, receiver) = oneshot::channel();
        let batch = WriteRequest::new(batch_objects.clone(), notifier);
        storage.write(batch).await.unwrap();
        assert!(receiver.await.unwrap().success);

        let blocks = storage.block_io().find_blocks().await.unwrap();
        assert_eq!(blocks.len(), 1);

        let objects = storage.read_blocks(&blocks).await.unwrap();
        assert_eq!(objects.len(), 2);
        assert_eq!(objects.get("my key"), Some(&batch_objects[0]));
        assert_eq!(objects.get("another key"), Some(&batch_objects[1]));
    }

    #[tokio::test]
    async fn multiple_blocks() {
        let storage = make_instant_storage().await;
        for key in ["a", "b"] {
            let (notifier, receiver) = oneshot::channel();
            let batch = WriteRequest::new(vec![Object::new(key, "and its value")], notifier);
            storage.write(batch).await.unwrap();
            assert!(receiver.await.unwrap().success);
        }

        let blocks = storage.block_io().find_blocks().await.unwrap();
        assert_eq!(blocks.len(), 2);

        let objects = storage.read_blocks(&blocks).await.unwrap();
        assert_eq!(objects.len(), 2);
    }
}
