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
use std::time::Duration;
use tokio::fs::create_dir_all;

#[derive(Serialize)]
#[serde(tag = "result")]
enum PutResponse {
    Success,
    Failure { error: String },
}

// TODO: move v1s to namespace in actix
#[get("/v1/get/{key}")]
async fn get(
    path: web::Path<(String,)>,
    service: web::Data<KeyValueService>,
) -> actix_web::Result<web::Json<Object>> {
    let (key,) = path.into_inner();
    match service.get(&key).await {
        Some(object) => Ok(web::Json(object)),
        None => Err(actix_web::error::ErrorNotFound("nope")),
    }
}

#[post("/v1/put")]
async fn put(
    objects: web::Json<Vec<Object>>,
    service: web::Data<KeyValueService>,
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
        max_block_size: 1024 * 1024,
    };

    let block_io = FilesystemBlockIO::new(storage_path).await?;

    // Read any existing objects from the storage
    info!("Loading existing objects...");
    let mut existing_objects = HashMap::new();
    for block in block_io.blocks() {
        info!("Processing block {}", block.id);
        let reader = block_io.block_reader(block).await?;
        for object in Storage::read_objects(reader).await? {
            existing_objects.insert(object.key.clone(), object);
        }
    }
    match existing_objects.len() {
        0 => info!("Found no existing objects"),
        count => info!("Found {count} existing objects"),
    };
    let storage = Storage::new(block_io, storage_config);
    let engine = KeyValueEngine::from_existing(existing_objects);
    let service = web::Data::new(KeyValueService::new(engine, storage));

    HttpServer::new(move || {
        App::new()
            .app_data(service.clone())
            .service(get)
            .service(put)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await?;
    Ok(())
}
