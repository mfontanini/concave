use crate::{
    io::BlockIO,
    storage::{self, BatchWriteResult, Storage, WriteRequest, WriteResult},
    Object, ObjectValue,
};
use futures::future::join_all;
use log::{debug, error};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::{
    spawn,
    sync::{mpsc::Receiver, oneshot, RwLock},
};

struct VersionedValue {
    value: ObjectValue,
    version: u32,
    state: VersionedValueState,
}

impl VersionedValue {
    fn new<S: Into<ObjectValue>>(value: S) -> Self {
        Self {
            value: value.into(),
            version: 0,
            state: VersionedValueState::CreationInProgress,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
enum VersionedValueState {
    ActiveWrite,
    CreationInProgress,
    Idle,
}

#[derive(Default)]
struct KeyValueCache {
    entries: HashMap<String, VersionedValue>,
}

/// The engine that provides the primitives to allow changing key/value pairs.
#[derive(Default)]
pub struct KeyValueEngine {
    cache: RwLock<KeyValueCache>,
}

impl KeyValueEngine {
    pub fn from_existing(objects: HashMap<String, Object>) -> Self {
        let entries = objects
            .into_iter()
            .map(|(key, object)| {
                (
                    key,
                    VersionedValue {
                        value: object.value,
                        version: object.version,
                        state: VersionedValueState::Idle,
                    },
                )
            })
            .collect();
        let cache = KeyValueCache { entries };
        Self {
            cache: cache.into(),
        }
    }

    pub async fn get(&self, key: &str) -> Option<Object> {
        let cache = self.cache.read().await;
        let entry = cache.entries.get(key)?;
        // Don't externalize an active write until it's done
        if entry.state == VersionedValueState::CreationInProgress {
            return None;
        }
        Some(Object::versioned(key, entry.value.clone(), entry.version))
    }

    pub async fn acquire<'a>(&self, keys: Vec<KeyVersion<'a>>) -> Result<(), AcquireError> {
        let mut cache = self.cache.write().await;
        for (index, key) in keys.iter().enumerate() {
            if let Err(e) = Self::acquire_key(&mut cache, key.key, key.version) {
                Self::rollback_staged_changes(&mut cache, &keys[0..index]);
                return Err(e);
            }
        }
        Ok(())
    }

    pub async fn commit(&self, key_values: Vec<KeyValue>) -> Result<(), CommitError> {
        let mut cache = self.cache.write().await;
        for key_value in key_values {
            match cache.entries.get_mut(&key_value.key) {
                Some(value) => {
                    value.value = key_value.value;
                    value.version += 1;
                    value.state = VersionedValueState::Idle;
                }
                // Note: this shouldn't happen unless the API is being misused. Also this would
                // require more cleanups as the previously committed key/values are still there
                None => return Err(CommitError::KeyDoesNotExist),
            };
        }
        Ok(())
    }

    fn acquire_key(
        cache: &mut KeyValueCache,
        key: &str,
        expected_version: u32,
    ) -> Result<(), AcquireError> {
        // TODO: make this better
        match cache.entries.get_mut(key) {
            Some(value) => {
                if value.state != VersionedValueState::Idle {
                    Err(AcquireError::WriteInProgress)
                } else if value.version != expected_version {
                    Err(AcquireError::OutdatedVersion)
                } else {
                    value.state = VersionedValueState::ActiveWrite;
                    Ok(())
                }
            }
            None => {
                if expected_version != 0 {
                    Err(AcquireError::IncorrectNewKeyVersion)
                } else {
                    cache
                        .entries
                        .insert(key.to_string(), VersionedValue::new(0));
                    Ok(())
                }
            }
        }
    }

    fn rollback_staged_changes<'a>(cache: &mut KeyValueCache, key_versions: &[KeyVersion<'a>]) {
        for key in key_versions {
            if key.version == 0 {
                cache.entries.remove(key.key);
            } else {
                cache.entries.get_mut(key.key).unwrap().state = VersionedValueState::Idle;
            }
        }
    }
}

pub struct KeyVersion<'a> {
    pub key: &'a str,
    pub version: u32,
}

pub struct KeyValue {
    pub key: String,
    pub value: ObjectValue,
}

impl<K: Into<String>, V: Into<ObjectValue>> From<(K, V)> for KeyValue {
    fn from(key_value: (K, V)) -> Self {
        Self {
            key: key_value.0.into(),
            value: key_value.1.into(),
        }
    }
}

impl<'a> KeyVersion<'a> {
    pub fn new(key: &'a str) -> Self {
        Self { key, version: 0 }
    }

    pub fn versioned(key: &'a str, version: u32) -> Self {
        Self { key, version }
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum AcquireError {
    #[error("outdated versions found")]
    OutdatedVersion,

    #[error("keys are actively being written to")]
    WriteInProgress,

    #[error("version incorrect for new key")]
    IncorrectNewKeyVersion,
}

#[derive(Error, Debug, PartialEq)]
pub enum CommitError {
    #[error("key does not exist")]
    KeyDoesNotExist,
}

pub struct KeyValueService<B: BlockIO + Send + Sync + 'static> {
    engine: KeyValueEngine,
    storage: Storage<B>,
}

/// The key value service.
///
/// This service interfaces directly with the key/value engine and the persistent storage
/// and exposes a get/put API using their primitives.
impl<B: BlockIO + Send + Sync + 'static> KeyValueService<B> {
    pub fn new(engine: KeyValueEngine, mut storage: Storage<B>) -> Arc<Self> {
        let batch_result_receiver = storage.take_batch_result_receiver().unwrap();
        let this = Arc::new(Self { engine, storage });
        spawn({
            let this = this.clone();
            this.process_batch_results(batch_result_receiver)
        });
        this
    }

    pub async fn get(&self, key: &str) -> Option<Object> {
        self.engine.get(key).await
    }

    pub async fn put(&self, objects: Vec<Object>) -> Result<(), PutError> {
        self.acquire(&objects).await?;
        self.write(objects).await?;
        Ok(())
    }

    async fn acquire(&self, objects: &[Object]) -> Result<(), PutError> {
        let mut key_versions = Vec::new();
        for object in objects {
            key_versions.push(KeyVersion {
                key: &object.key,
                version: object.version,
            });
        }
        self.engine.acquire(key_versions).await?;
        Ok(())
    }

    async fn write(&self, objects: Vec<Object>) -> Result<(), PutError> {
        let (sender, receiver) = oneshot::channel();
        let request = WriteRequest::new(objects, sender);
        self.storage.write(request).await?;
        receiver.await?;
        Ok(())
    }

    async fn process_batch_results(self: Arc<Self>, mut receiver: Receiver<BatchWriteResult>) {
        while let Some(batch_result) = receiver.recv().await {
            if let Err(e) = self.process_batch_result(batch_result).await {
                error!("Error processing batch result: {e}");
            }
        }
    }

    async fn process_batch_result(&self, batch_result: BatchWriteResult) -> Result<(), PutError> {
        match batch_result.result {
            WriteResult::Success => self.commit(batch_result.objects).await?,
            // TODO: rollback on error
            WriteResult::Failure => error!("Rollbacks not implemented yet!"),
        };
        let mut notifiers_futs = Vec::new();
        for notifier in batch_result.notifiers {
            let result = batch_result.result.clone();
            notifiers_futs.push(async move {
                if notifier.send(result).is_err() {
                    // This is fine: the client just disconnected. Their write succeeded but they won't know about it
                    debug!("Error sending batch result, ignoring");
                }
            });
        }
        join_all(notifiers_futs).await;
        Ok(())
    }

    async fn commit(&self, objects: Vec<Object>) -> Result<(), PutError> {
        let mut key_values = Vec::new();
        for object in objects {
            key_values.push(KeyValue {
                key: object.key,
                value: object.value,
            });
        }
        self.engine.commit(key_values).await?;
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum PutError {
    #[error(transparent)]
    Acquire(#[from] AcquireError),

    #[error(transparent)]
    Commit(#[from] CommitError),

    #[error(transparent)]
    Write(#[from] storage::WriteError),

    #[error(transparent)]
    Acknowledge(#[from] oneshot::error::RecvError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{io::MemoryBlockIO, storage::StorageConfig};
    use std::sync::Arc;
    use std::time::Duration;

    async fn make_storage() -> Storage<MemoryBlockIO> {
        let config = StorageConfig {
            batch_time: Duration::from_millis(1),
            max_batch_size: 10,
            max_block_size: 10,
            max_blocks: 5,
        };
        Storage::new(Arc::new(MemoryBlockIO::default()), config)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn acquire_and_commit() {
        let engine = KeyValueEngine::default();
        // Try to acquire using an incorrect initial version
        assert_eq!(
            engine
                .acquire(vec![KeyVersion::versioned("hello", 1)])
                .await,
            Err(AcquireError::IncorrectNewKeyVersion),
        );

        // Acquire and commit
        engine
            .acquire(vec![KeyVersion::new("hello")])
            .await
            .unwrap();
        engine
            .commit(vec![("hello", "world").into()])
            .await
            .unwrap();

        // Make sure the write actually worked
        assert_eq!(
            engine.get("hello").await,
            Some(Object::versioned("hello", "world", 1))
        );

        // We should not be allowed to acquire for version 0
        assert_eq!(
            engine.acquire(vec![KeyVersion::new("hello")]).await,
            Err(AcquireError::OutdatedVersion),
        );

        // We should be able to acquire again using version 1
        assert_eq!(
            engine
                .acquire(vec![KeyVersion::versioned("hello", 1)])
                .await,
            Ok(())
        );

        // Write to it again and make sure it changed
        assert_eq!(
            engine.commit(vec![("hello", "world!").into()]).await,
            Ok(())
        );
        assert_eq!(
            engine.get("hello").await,
            Some(Object::versioned("hello", "world!", 2))
        );
    }

    #[tokio::test]
    async fn acquire_overlapping() {
        let engine = KeyValueEngine::default();
        assert_eq!(
            engine
                .acquire(vec![KeyVersion::new("a"), KeyVersion::new("b")])
                .await,
            Ok(())
        );
        // b overlaps
        assert_eq!(
            engine
                .acquire(vec![KeyVersion::new("c"), KeyVersion::new("b")])
                .await,
            Err(AcquireError::WriteInProgress),
        );
        // We should still be able to acquire c on its own (meaning rollback worked)
        assert_eq!(engine.acquire(vec![KeyVersion::new("c")]).await, Ok(()),);
    }

    #[tokio::test]
    async fn service_put_and_get() {
        let service = KeyValueService::new(KeyValueEngine::default(), make_storage().await);
        // Not here yet
        assert_eq!(service.get("hello").await, None);
        // Wrong version
        assert!(service
            .put(vec![Object::versioned("hello", "world", 1)])
            .await
            .is_err());
        // Put and get it
        service
            .put(vec![Object::new("hello", "world")])
            .await
            .unwrap();
        assert_eq!(
            service.get("hello").await,
            Some(Object::versioned("hello", "world", 1))
        );
    }
}
