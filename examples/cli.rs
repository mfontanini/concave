use anyhow::Result;
use concave::Object;
use reqwest::{Client, StatusCode};
use serde_derive::Deserialize;
use std::sync::Arc;
use std::time::Instant;
use structopt::StructOpt;
use tokio::spawn;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(tag = "result")]
enum PutResponse {
    Success,
    Failure { error: String },
}

struct Api {
    client: Client,
    url: String,
}

impl Api {
    fn new(url: String) -> Self {
        Self {
            client: Client::default(),
            url,
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Object>> {
        let result = self
            .client
            .get(format!("{}/v1/get", self.url))
            .query(&[("key", key)])
            .send()
            .await?;
        if result.status() == StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Ok(result.json().await?)
        }
    }

    async fn put(&self, objects: &[Object]) -> Result<PutResponse> {
        let result = self
            .client
            .post(format!("{}/v1/put", self.url))
            .json(objects)
            .send()
            .await?;
        Ok(result.json().await?)
    }
}

#[derive(StructOpt, Debug)]
enum Command {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
        version: u32,
    },
    BenchmarkPuts {
        threads: u32,
        batches: u32,
        batch_size: u32,
    },
}

#[derive(StructOpt, Debug)]
#[structopt(name = "concave-cli")]
struct Options {
    /// The url where the concave server is running
    #[structopt(short, long)]
    url: String,

    /// The command to be ran
    #[structopt(subcommand)]
    command: Command,
}

async fn benchmark_puts(api: Api, tasks: u32, batches: u32, batch_size: u32) -> Result<()> {
    let api = Arc::new(api);
    let mut handles = Vec::new();

    println!("Running {tasks} parallel tasks inserting {batches} batches of size {batch_size} objects each...");
    let start_time = Instant::now();
    for _ in 0..tasks {
        let api = api.clone();
        let task = async move {
            for _ in 0..batches {
                let mut objects = Vec::new();
                for _ in 0..batch_size {
                    let object = Object::versioned(
                        format!("{}", Uuid::new_v4()),
                        Uuid::new_v4().as_bytes().to_vec(),
                        0,
                    );
                    objects.push(object);
                }
                api.put(&objects).await.unwrap();
            }
        };
        handles.push(spawn(task));
    }
    for handle in handles {
        handle.await?;
    }
    let duration = start_time.elapsed();
    let total_insertions = tasks * batches * batch_size;
    let objects_per_second = total_insertions as f64 / (duration.as_millis() as f64 / 1000.0);
    println!(
        "Finished inserting {total_insertions} objects, total time {duration:?},
objects per second: {objects_per_second:.2}"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = Options::from_args();
    let api = Api::new(options.url);
    match options.command {
        Command::Get { key } => match api.get(&key).await? {
            Some(object) => println!("Value: {}, version: {}", object.value, object.version),
            None => println!("Key not found"),
        },
        Command::Put {
            key,
            value,
            version,
        } => {
            let object = Object::versioned(key, value, version);
            match api.put(&[object]).await? {
                PutResponse::Success => println!("Success!"),
                PutResponse::Failure { error } => println!("Put failed: {error}"),
            }
        }
        Command::BenchmarkPuts {
            threads,
            batches,
            batch_size,
        } => benchmark_puts(api, threads, batches, batch_size).await?,
    };
    Ok(())
}
