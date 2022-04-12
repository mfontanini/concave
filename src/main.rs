use actix_web::{get, post, web, App, HttpServer};
use concave::{
    io::{BlockIO, FilesystemBlockIO},
    kv::{KeyValueEngine, KeyValueService},
    storage::{Storage, StorageConfig},
    Object,
};
use log::info;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
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

#[derive(Deserialize, Debug)]
struct Config {
    storage: StorageConfig,
    blocks_path: String,
    bind_endpoint: String,
}

fn load_config(path: &str) -> Result<Config, config::ConfigError> {
    let builder = config::Config::builder()
        .add_source(config::File::with_name(path))
        .add_source(config::Environment::default().separator("_"));
    builder.build()?.try_deserialize()
}

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();
    // TODO: config
    let config = load_config("config.sample.yaml")?;
    create_dir_all(&config.blocks_path).await?;

    let storage = Storage::new(
        Arc::new(FilesystemBlockIO::new(config.blocks_path)),
        config.storage,
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
    let service = KeyValueService::new(engine, storage);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::from(service.clone()))
            .service(web::scope("/v1").service(get).service(put))
    })
    .bind(config.bind_endpoint)?
    .run()
    .await?;
    info!("Server stopped, exiting");
    Ok(())
}
