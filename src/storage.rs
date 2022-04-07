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
pub struct Storage {
    writer_context: WriterContext,
}

impl Storage {
    pub async fn new<B: BlockIO + Send + Sync + 'static>(
        block_io: B,
        config: StorageConfig,
    ) -> Result<Self, StorageCreateError> {
        let writer_context = StorageWriter::new(block_io, config).await?.launch();
        Ok(Self { writer_context })
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
    pub async fn read_blocks<B: BlockIO>(
        block_io: &B,
        blocks: &[Block],
    ) -> Result<HashMap<String, Object>, ReadError> {
        let mut objects = HashMap::new();
        for block in blocks {
            info!("Processing block {}", block.id);
            let reader = block_io.block_reader(block).await?;
            let mut stream = Box::pin(iter_objects(reader));
            while let Some(object) = stream.next().await {
                let object = object?;
                objects.insert(object.key.clone(), object);
            }
        }
        Ok(objects)
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
    block_io: B,
    closed_blocks: Vec<Block>,
    next_block_id: u64,
    config: StorageConfig,
}

impl<B: BlockIO + Send + Sync + 'static> StorageWriter<B> {
    pub async fn new(block_io: B, config: StorageConfig) -> Result<Self, StorageCreateError> {
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
    use futures::StreamExt;
    use std::sync::{Arc, Mutex};

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

    async fn make_writer(
        max_batch_size: usize,
        batch_time: Duration,
    ) -> (StorageWriter<NullBlockIO>, Arc<Mutex<Vec<Vec<u8>>>>) {
        let block_io = NullBlockIO::default();
        let blocks = block_io.blocks_data();
        (
            StorageWriter::new(block_io, make_config(max_batch_size, batch_time))
                .await
                .unwrap(),
            blocks,
        )
    }

    async fn make_instant_writer() -> (StorageWriter<NullBlockIO>, Arc<Mutex<Vec<Vec<u8>>>>) {
        make_writer(1, Duration::from_millis(10)).await
    }

    async fn make_buffered_writer() -> (StorageWriter<NullBlockIO>, Arc<Mutex<Vec<Vec<u8>>>>) {
        make_writer(100, Duration::from_millis(100)).await
    }

    #[tokio::test]
    async fn launch_write_loop() {
        let writer = make_instant_writer().await;
        // Simply launch and join to make sure we don't get stuck
        writer.0.launch();
    }

    #[tokio::test]
    async fn write_batch() {
        let (writer, blocks) = make_buffered_writer().await;
        let context = writer.launch();
        let batch_objects = vec![
            Object::new("my key", "and its value"),
            Object::new("another key", "another value"),
        ];
        let (notifier, receiver) = oneshot::channel();
        let batch = WriteRequest::new(batch_objects.clone(), notifier);
        let sender = &context.request_sender;
        sender.send(batch).await.unwrap();
        assert!(receiver.await.unwrap().success);

        let blocks = blocks.lock().unwrap();
        assert_eq!(blocks.len(), 1);

        let objects: Vec<_> = iter_objects(io::Cursor::new(&blocks[0]))
            .map(Result::unwrap)
            .collect()
            .await;
        assert_eq!(objects, batch_objects);
    }
}
