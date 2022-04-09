use actix_web::{get, post, web, App, HttpServer};
use concave::{
    io::{BlockIO, FilesystemBlockIO},
    kv::{KeyValueEngine, KeyValueService},
    storage::{Storage, StorageConfig},
    Object,
};
use log::info;
use serde_derive::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::create_dir_all;

#[derive(Serialize)]
#[serde(tag = "result")]
enum PutResponse {
    Success,
    Failure { error: String },
}

#[get("/get/{key}")]
async fn get(
    path: web::Path<(String,)>,
    service: web::Data<KeyValueService<FilesystemBlockIO>>,
) -> actix_web::Result<web::Json<Object>> {
    let (key,) = path.into_inner();
    match service.get(&key).await {
        Some(object) => Ok(web::Json(object)),
        None => Err(actix_web::error::ErrorNotFound("nope")),
    }
}

#[post("/put")]
async fn put(
    objects: web::Json<Vec<Object>>,
    service: web::Data<KeyValueService<FilesystemBlockIO>>,
) -> web::Json<PutResponse> {
    let result = match service.put(objects.into_inner()).await {
        Ok(_) => PutResponse::Success,
        Err(e) => PutResponse::Failure {
            error: e.to_string(),
        },
    };
    web::Json(result)
}

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();
    // TODO: config
    let storage_path = "/tmp/concave";
    create_dir_all(storage_path).await?;
    let storage_config = StorageConfig {
        batch_time: Duration::from_millis(10),
        max_batch_size: 4096,
        max_block_size: 1024,
        max_blocks: 5,
    };

    let storage = Storage::new(
        Arc::new(FilesystemBlockIO::new(storage_path)),
        storage_config,
    )
    .await?;

    // Read any existing objects from the storage
    let existing_blocks = storage.block_io().find_blocks().await?;
    let existing_objects;
    if !existing_blocks.is_empty() {
        info!(
            "Found {} existing blocks, loading objects...",
            existing_blocks.len()
        );
        existing_objects = storage.read_blocks(&existing_blocks).await?;
        match existing_objects.len() {
            0 => info!("Found no existing objects"),
            count => info!("Found {count} existing objects"),
        }
    } else {
        info!("Found no existing blocks");
        existing_objects = HashMap::new();
    }
    let engine = KeyValueEngine::from_existing(existing_objects);
    let service = web::Data::new(KeyValueService::new(engine, storage));

    HttpServer::new(move || {
        App::new()
            .app_data(service.clone())
            .service(web::scope("/v1").service(get).service(put))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await?;
    Ok(())
}
