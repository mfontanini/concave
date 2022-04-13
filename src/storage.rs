use crate::{
    block::Block,
    io::{BlockCloseError, BlockIO, BlockOpenError, FindBlocksError, OpenBlock, TemporaryBlock},
    Object, ObjectValue,
};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use log::{debug, error, info, warn};
use prost::Message;
use serde_derive::Deserialize;
use std::collections::HashMap;
use std::io;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::time::timeout;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    spawn,
    sync::{
        mpsc::{channel, Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};

mod proto {
    include!(concat!(env!("OUT_DIR"), "/storage.rs"));
}

/// The result of a write operation
#[derive(Debug)]
pub struct BatchWriteResult {
    pub result: WriteResult,
    pub notifiers: Vec<oneshot::Sender<WriteResult>>,
    pub objects: Vec<Object>,
}

impl BatchWriteResult {
    fn new(result: WriteResult, batch: WriteBatch) -> Self {
        Self {
            result,
            notifiers: batch.notifiers,
            objects: batch.objects,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum WriteResult {
    Success,
    Failure,
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

#[derive(Deserialize, Debug)]
pub struct StorageConfig {
    #[serde(with = "humantime_serde")]
    pub batch_time: Duration,
    pub max_batch_size: usize,
    pub max_block_size: u64,
    pub max_blocks: usize,
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
        read_blocks(self.block_io.as_ref(), blocks).await
    }

    pub fn block_io(&self) -> Arc<B> {
        self.block_io.clone()
    }

    pub fn take_batch_result_receiver(&mut self) -> Option<Receiver<BatchWriteResult>> {
        self.writer_context.batch_result_receiver.take()
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

struct CompactableBlocks {
    blocks: [Block; 2],
    remove_index: usize,
    merge_index: usize,
}

/// The state of the compaction task, if any
enum CompactionState {
    Stopped,
    Running,
}

struct WriterSharedState {
    closed_blocks: Vec<Block>,
    compaction_state: CompactionState,
}

/// This is the type doing the heavy work: this batches data, manages BlockIOs, etc.
struct StorageWriter<B: BlockIO> {
    block_io: Arc<B>,
    state: Arc<Mutex<WriterSharedState>>,
    next_block_id: u64,
    config: StorageConfig,
    write_result_sender: Sender<BatchWriteResult>,
    write_result_receiver: Option<Receiver<BatchWriteResult>>,
}

impl<B: BlockIO + Send + Sync + 'static> StorageWriter<B> {
    pub async fn new(block_io: Arc<B>, config: StorageConfig) -> Result<Self, StorageCreateError> {
        let mut closed_blocks = block_io.find_blocks().await?;
        let next_block_id = closed_blocks.last().map(|block| block.id).unwrap_or(0);
        // The last block is not closed yet
        closed_blocks.pop();
        let state = Arc::new(Mutex::new(WriterSharedState {
            closed_blocks,
            compaction_state: CompactionState::Stopped,
        }));
        let (write_result_sender, write_result_receiver) = channel(100);
        Ok(Self {
            block_io,
            state,
            next_block_id,
            config,
            write_result_sender,
            write_result_receiver: Some(write_result_receiver),
        })
    }

    /// Launches the writer. The sender in the returned context can be used to schedule write requests.
    pub fn launch(mut self) -> WriterContext {
        let running = Arc::new(AtomicBool::new(true));
        let (request_sender, receiver) = channel(100);
        let write_result_receiver = self.write_result_receiver.take().unwrap();
        spawn({
            let running = running.clone();
            async move {
                match self.write_loop(receiver, running).await {
                    Ok(_) => info!("Writer loop done"),
                    Err(e) => error!("Error on write loop: {e}"),
                };
            }
        });
        WriterContext::new(request_sender, running, write_result_receiver)
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
            if self
                .write_result_sender
                .send(BatchWriteResult::new(WriteResult::Success, batch))
                .await
                .is_err()
            {
                return Err(WriteLoopError::Disconnected);
            }
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
                    // Ignore this as we're already in trouble
                    let _ = self
                        .write_result_sender
                        .send(BatchWriteResult::new(WriteResult::Failure, batch))
                        .await;
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
                debug!("Batch reached max configured size, writing it");
                break;
            }
            time_remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(duration) => duration,
                None => break,
            };
        }
        Ok(batch)
    }

    fn serialize(objects: impl Iterator<Item = Object>, data: &mut Vec<u8>) -> io::Result<()> {
        for object in objects {
            proto::Object::from(object).encode_length_delimited(data)?;
        }
        Ok(())
    }

    fn add_to_batch(request: WriteRequest, mut batch: WriteBatch) -> io::Result<WriteBatch> {
        for object in request.objects {
            let proto_object = proto::Object::from(object);
            proto_object.encode_length_delimited(&mut batch.data)?;
            batch.objects.push(Object::from(proto_object));
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
        info!("Opening block {}", self.next_block_id);
        let block = self.block_io.open_block(self.next_block_id).await?;
        self.next_block_id += 1;
        Ok(block)
    }

    async fn close_block(
        &mut self,
        open_block: OpenBlock<B::Writer>,
    ) -> Result<(), BlockCloseError> {
        let block = open_block.block().clone();
        info!("Closing block {}, size is {}", block.id, block.size);
        self.block_io.close_block(open_block).await?;
        self.state.lock().unwrap().closed_blocks.push(block);
        // Launch compaction if needed
        Self::launch_compaction(
            self.block_io.clone(),
            self.state.clone(),
            self.config.max_blocks,
        )
        .await;
        Ok(())
    }

    // Finds the "best" 2 blocks to be compacted. The goal here is to always compact the 2 consecutive blocks
    // that have the smallest max combined size. This should eventually end up making the block size
    // distribution fairly even`.
    fn compactable_blocks(
        state: &WriterSharedState,
        max_blocks: usize,
    ) -> Option<CompactableBlocks> {
        if state.closed_blocks.len() <= max_blocks {
            None
        } else if matches!(state.compaction_state, CompactionState::Running) {
            warn!("Should trigger compaction but it's already in progress");
            None
        } else {
            // Window closed blocks in chunks of 2, sort by combined size and keep the smallest one
            let mut candidates: Vec<_> = state.closed_blocks.windows(2).collect();
            candidates.sort_by_key(|chunk| chunk[0].size + chunk[1].size);
            let compactables = candidates[0];
            // Find the index we'll remove. The next one is the one we'll merge into
            let remove_index = state
                .closed_blocks
                .iter()
                .position(|block| block.id == compactables[0].id)
                .unwrap();
            let merge_index = remove_index + 1;
            Some(CompactableBlocks {
                blocks: [compactables[0].clone(), compactables[1].clone()],
                remove_index,
                merge_index,
            })
        }
    }

    async fn launch_compaction(
        block_io: Arc<B>,
        state: Arc<Mutex<WriterSharedState>>,
        threshold: usize,
    ) -> JoinHandle<()> {
        spawn(async move {
            loop {
                // Get the first 2 compactable blocks, if any
                let compactable_blocks = {
                    let mut state = state.lock().unwrap();
                    let blocks = match Self::compactable_blocks(&state, threshold) {
                        Some(blocks) => blocks,
                        None => break,
                    };
                    state.compaction_state = CompactionState::Running;
                    blocks
                };
                // Trigger compaction on these
                let resulting_size =
                    match Self::compact_blocks(&block_io, compactable_blocks.blocks).await {
                        Ok(size) => size,
                        Err(e) => {
                            error!("Error during compaction: {e}");
                            panic!("Error during compaction, cannot proceed");
                        }
                    };
                // Drop the first one, update the second one's size, and reset our state
                {
                    let mut state = state.lock().unwrap();
                    state.closed_blocks[compactable_blocks.merge_index].size = resulting_size;
                    state.closed_blocks.remove(compactable_blocks.remove_index);
                    state.compaction_state = CompactionState::Stopped;
                }
            }
        })
    }

    async fn compact_blocks(block_io: &B, blocks: [Block; 2]) -> Result<u64, CompactError> {
        info!("Compacting block {} into {}", blocks[0].id, blocks[1].id);
        let entries = read_blocks(block_io, &blocks).await?;

        // Create a temporary file and write all data to it
        let mut temporary = block_io.temporary_block().await?;
        let mut data = Vec::new();
        Self::serialize(entries.into_values(), &mut data)?;
        temporary.write(&data).await?;

        // Replace the newer one with the combined data and drop the older one
        block_io.replace_block(blocks[1].id, temporary).await?;
        block_io.drop_block(blocks[0].id).await?;
        Ok(data.len() as u64)
    }
}

async fn read_blocks<B: BlockIO>(
    block_io: &B,
    blocks: &[Block],
) -> Result<HashMap<String, Object>, ReadError> {
    let mut objects = HashMap::new();
    for block in blocks {
        info!("Reading block {}", block.id);
        let reader = block_io.block_reader(block.id).await?;
        let mut stream = Box::pin(iter_objects(reader));
        while let Some(object) = stream.next().await {
            let object = object?;
            objects.insert(object.key.clone(), object);
        }
    }
    Ok(objects)
}

fn iter_objects<R: AsyncRead + Unpin>(
    mut reader: R,
) -> impl Stream<Item = Result<Object, ReadError>> {
    try_stream! {
        let mut buffer = bytes::BytesMut::with_capacity(8192);
        reader.read_buf(&mut buffer).await?;
        let mut index = 0;
        let mut eof = false;

        loop {
            // TODO: this is gnarly and I'm tired
            if buffer.len() - index < 2048 && !eof {
                buffer = buffer.split_off(index);
                let bytes_read = reader.read_buf(&mut buffer).await?;
                eof = bytes_read == 0;
                index = 0;
            }
            if buffer.len() - index == 0 && eof {
                break;
            }
            let object = proto::Object::decode_length_delimited(&buffer[index..])?;
            let lenght_field_size = prost::length_delimiter_len(object.encoded_len());
            index += object.encoded_len() + lenght_field_size;

            yield Object::from(object);
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

#[derive(Error, Debug)]
enum CompactError {
    #[error("error reading blocks: {0}")]
    Read(#[from] ReadError),

    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Default)]
struct WriteBatch {
    /// The data to be included in this batch
    data: Vec<u8>,

    /// The senders to notify all of the pending writers
    notifiers: Vec<oneshot::Sender<WriteResult>>,

    /// The objects being included in this batch
    objects: Vec<Object>,
}

struct WriterContext {
    request_sender: Sender<WriteRequest>,
    running: Arc<AtomicBool>,
    batch_result_receiver: Option<Receiver<BatchWriteResult>>,
}

impl WriterContext {
    fn new(
        request_sender: Sender<WriteRequest>,
        running: Arc<AtomicBool>,
        batch_result_receiver: Receiver<BatchWriteResult>,
    ) -> Self {
        Self {
            request_sender,
            running,
            batch_result_receiver: Some(batch_result_receiver),
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

// Rust -> proto
impl From<Object> for proto::Object {
    fn from(object: Object) -> proto::Object {
        proto::Object {
            key: object.key,
            value: Some(object.value.into()),
            version: object.version,
        }
    }
}

impl From<ObjectValue> for proto::ObjectValue {
    fn from(value: ObjectValue) -> proto::ObjectValue {
        use proto::object_value::SingleMulti;
        use proto::single_value::Single;
        match value {
            ObjectValue::String(value) => proto::ObjectValue {
                single_multi: Some(SingleMulti::Single(proto::SingleValue {
                    single: Some(Single::String(value)),
                })),
            },
            ObjectValue::Number(value) => proto::ObjectValue {
                single_multi: Some(SingleMulti::Single(proto::SingleValue {
                    single: Some(Single::Number(value)),
                })),
            },
            ObjectValue::Bytes(value) => proto::ObjectValue {
                single_multi: Some(SingleMulti::Single(proto::SingleValue {
                    single: Some(Single::Bytes(value)),
                })),
            },
            ObjectValue::Float(value) => proto::ObjectValue {
                single_multi: Some(SingleMulti::Single(proto::SingleValue {
                    single: Some(Single::Float(value)),
                })),
            },
            ObjectValue::Bool(value) => proto::ObjectValue {
                single_multi: Some(SingleMulti::Single(proto::SingleValue {
                    single: Some(Single::Bool(value)),
                })),
            },
        }
    }
}

// Proto -> Rust

// Note: these are actually fallible but given this is used internally it works for now

impl From<proto::Object> for Object {
    fn from(object: proto::Object) -> Object {
        Object {
            key: object.key,
            value: object.value.unwrap().into(),
            version: object.version,
        }
    }
}

impl From<proto::ObjectValue> for ObjectValue {
    fn from(value: proto::ObjectValue) -> ObjectValue {
        use proto::object_value::SingleMulti;
        match value.single_multi.unwrap() {
            SingleMulti::Single(single) => single.into(),
        }
    }
}

impl From<proto::SingleValue> for ObjectValue {
    fn from(value: proto::SingleValue) -> ObjectValue {
        use proto::single_value::Single;
        match value.single.unwrap() {
            Single::String(value) => ObjectValue::String(value),
            Single::Number(value) => ObjectValue::Number(value),
            Single::Bytes(value) => ObjectValue::Bytes(value),
            Single::Float(value) => ObjectValue::Float(value),
            Single::Bool(value) => ObjectValue::Bool(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MemoryBlockIO;
    use std::sync::Arc;
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[ctor::ctor]
    fn init() {
        env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug)
            .init();
    }

    fn make_config(
        max_batch_size: usize,
        max_block_size: u64,
        batch_time: Duration,
    ) -> StorageConfig {
        StorageConfig {
            batch_time,
            max_batch_size,
            max_block_size,
            max_blocks: 5,
        }
    }

    async fn make_storage(
        max_batch_size: usize,
        max_block_size: u64,
        batch_time: Duration,
    ) -> Storage<MemoryBlockIO> {
        let block_io = Arc::new(MemoryBlockIO::default());
        let mut storage = Storage::new(
            block_io,
            make_config(max_batch_size, max_block_size, batch_time),
        )
        .await
        .unwrap();

        let mut result_receiver = storage.take_batch_result_receiver().unwrap();
        spawn(async move {
            while let Some(batch_result) = result_receiver.recv().await {
                for notifier in batch_result.notifiers {
                    notifier.send(batch_result.result.clone()).unwrap();
                }
            }
        });

        storage
    }

    async fn make_instant_storage() -> Storage<MemoryBlockIO> {
        // A storage that will immediately dump a batch request
        make_storage(1, 1, Duration::from_millis(0)).await
    }

    async fn make_buffered_storage() -> Storage<MemoryBlockIO> {
        // A storage that will attempt to buffer writes a bit
        make_storage(100, 10, Duration::from_millis(100)).await
    }

    #[tokio::test]
    async fn launch_write_loop() {
        // Simply create it and implicitly join to make sure we don't get stuck
        let _storage = make_instant_storage().await;
    }

    #[tokio::test]
    async fn write_batch() -> TestResult {
        let storage = make_buffered_storage().await;
        let batch_objects = vec![
            Object::new("my key", "and its value"),
            Object::new("another key", "another value"),
        ];
        let (notifier, receiver) = oneshot::channel();
        let batch = WriteRequest::new(batch_objects.clone(), notifier);
        storage.write(batch).await?;
        assert_eq!(receiver.await?, WriteResult::Success);

        let blocks = storage.block_io().find_blocks().await?;
        assert_eq!(blocks.len(), 1);

        let objects = storage.read_blocks(&blocks).await?;
        assert_eq!(objects.len(), 2);
        assert_eq!(objects.get("my key"), Some(&batch_objects[0]));
        assert_eq!(objects.get("another key"), Some(&batch_objects[1]));
        Ok(())
    }

    #[tokio::test]
    async fn multiple_blocks() -> TestResult {
        let storage = make_instant_storage().await;
        for key in ["a", "b"] {
            let (notifier, receiver) = oneshot::channel();
            let batch = WriteRequest::new(vec![Object::new(key, "and its value")], notifier);
            storage.write(batch).await?;
            assert_eq!(receiver.await?, WriteResult::Success);
        }

        let blocks = storage.block_io().find_blocks().await?;
        assert_eq!(blocks.len(), 2);

        let objects = storage.read_blocks(&blocks).await?;
        assert_eq!(objects.len(), 2);
        Ok(())
    }

    #[test]
    fn compactable_blocks() {
        type Writer = StorageWriter<MemoryBlockIO>;
        let mut state = WriterSharedState {
            closed_blocks: Vec::new(),
            compaction_state: CompactionState::Running,
        };
        // Running -> no compaction
        assert!(Writer::compactable_blocks(&state, 2).is_none());

        // Fewer than 2 files -> no compaction
        state.compaction_state = CompactionState::Stopped;
        state.closed_blocks = vec![Block::new(0), Block::new(1)];
        assert!(Writer::compactable_blocks(&state, 2).is_none());

        // Expect the last 2 to be compacted
        state.closed_blocks = vec![
            Block::existing(0, 3),
            Block::existing(1, 2),
            Block::existing(2, 1),
        ];
        let blocks = Writer::compactable_blocks(&state, 2).unwrap();
        assert_eq!(blocks.remove_index, 1);
        assert_eq!(blocks.merge_index, 2);
        assert_eq!(
            blocks.blocks.as_slice(),
            &[
                state.closed_blocks[1].clone(),
                state.closed_blocks[2].clone()
            ]
        );

        // Now expect the first two
        state.closed_blocks[2].size = 100;
        let blocks = Writer::compactable_blocks(&state, 2).unwrap();
        assert_eq!(blocks.remove_index, 0);
        assert_eq!(blocks.merge_index, 1);
        assert_eq!(
            blocks.blocks.as_slice(),
            &[
                state.closed_blocks[0].clone(),
                state.closed_blocks[1].clone()
            ]
        );
    }

    #[tokio::test]
    async fn compaction() -> TestResult {
        let storage = make_instant_storage().await;
        for (version, value) in ["1", "2", "3"].iter().enumerate() {
            let (notifier, receiver) = oneshot::channel();
            let objects = vec![
                Object::versioned("a", *value, version as u32),
                Object::versioned("b", *value, version as u32),
            ];
            storage.write(WriteRequest::new(objects, notifier)).await?;
            assert_eq!(receiver.await?, WriteResult::Success);
        }

        // Ensure there's 3 blocks and compact the first two
        let storage_blocks = storage.block_io().find_blocks().await?;
        assert_eq!(storage_blocks.len(), 3);
        let blocks = [Block::new(0), Block::new(1)];
        StorageWriter::compact_blocks(storage.block_io().as_ref(), blocks).await?;

        // We should no longer have the first one
        let blocks = storage.block_io().find_blocks().await?;
        assert_eq!(
            blocks.as_slice(),
            [storage_blocks[1].clone(), storage_blocks[2].clone()]
        );

        // We should have both of them set to version 1 (they start at version 0 here)
        let objects = storage.read_blocks(&[Block::new(1)]).await?;
        assert_eq!(objects.len(), 2);
        assert_eq!(objects.get("a"), Some(&Object::versioned("a", "2", 1)));
        assert_eq!(objects.get("b"), Some(&Object::versioned("b", "2", 1)));
        Ok(())
    }

    #[tokio::test]
    async fn launch_compaction() -> TestResult {
        // Run a storage for a bit just to initialize some blocks
        let block_io = {
            let storage = make_instant_storage().await;
            for (version, value) in ["1", "2", "3"].iter().enumerate() {
                let (notifier, receiver) = oneshot::channel();
                let objects = Object::versioned("a", *value, version as u32);
                storage
                    .write(WriteRequest::new(vec![objects], notifier))
                    .await?;
                assert_eq!(receiver.await?, WriteResult::Success);
            }
            storage.block_io()
        };
        let state = Arc::new(Mutex::new(WriterSharedState {
            closed_blocks: vec![Block::new(0), Block::new(1), Block::new(2)],
            compaction_state: CompactionState::Running,
        }));
        // Compaction state is running so nothing should happen
        StorageWriter::launch_compaction(block_io.clone(), state.clone(), 1)
            .await
            .await?;
        assert_eq!(block_io.find_blocks().await?.len(), 3);

        // Now set the state to stopped, and re-run. This should actually compact twice, as the max blocks is 1
        state.lock().unwrap().compaction_state = CompactionState::Stopped;
        StorageWriter::launch_compaction(block_io.clone(), state.clone(), 1)
            .await
            .await?;
        let blocks = block_io.find_blocks().await?;
        let total_size = blocks.iter().map(|block| block.size).sum();
        let expected_block = Block {
            id: 2,
            size: total_size,
        };

        assert_eq!(blocks.as_slice(), &[expected_block.clone()]);

        // Ensure the state is now stopped and we only have one file
        let state = state.lock().unwrap();
        assert!(matches!(state.compaction_state, CompactionState::Stopped));

        assert_eq!(state.closed_blocks.as_slice(), &[expected_block]);
        Ok(())
    }
}
