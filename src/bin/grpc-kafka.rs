#[path = "../generated/mod.rs"]
mod generated;

use {
    anyhow::Context,
    clap::{Parser, Subcommand},
    futures::{future::BoxFuture, stream::StreamExt},
    rdkafka::{config::ClientConfig, consumer::Consumer, message::Message, producer::FutureRecord},
    sha2::{Digest, Sha256},
    std::{net::SocketAddr, sync::Arc, time::Duration},
    tokio::task::JoinSet,
    tonic::transport::ClientTlsConfig,
    tracing::{debug, trace, warn},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_kafka::{
        config::{load as config_load, GrpcRequestToProto},
        create_shutdown,
        kafka::{
            config::{Config, ConfigDedup, ConfigGrpc2Kafka, ConfigKafka2Grpc},
            dedup::KafkaDedup,
            grpc::GrpcService,
            metrics,
        },
        metrics::{run_server as prometheus_run_server, GprcMessageKind},
        setup_tracing,
    },
    yellowstone_grpc_proto::{
        prelude::{subscribe_update::UpdateOneof, SubscribeUpdate},
        prost::Message as _,
    },
    serde_json,
    actix_web::{App, HttpServer, Responder},
    actix_web_codegen::routes,
    std::thread,
};
use base64::{engine::general_purpose, Engine as _};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone gRPC Kafka Tool")]
struct Args {
    /// Path to config file
    #[clap(short, long, default_value = "./config-kafka.json")]
    config: String,

    /// Prometheus listen address
    #[clap(long)]
    prometheus: Option<SocketAddr>,

    #[command(subcommand)]
    action: Option<ArgsAction>,
}

#[derive(Debug, Clone, Subcommand, Default)]
enum ArgsAction {
    /// Receive data from Kafka, deduplicate and send them back to Kafka
    Dedup,
    /// Receive data from gRPC and send them to the Kafka
    #[default]
    #[command(name = "grpc2kafka")]
    Grpc2Kafka,
    /// Receive data from Kafka and send them over gRPC
    #[command(name = "kafka2grpc")]
    Kafka2Grpc,
}

impl ArgsAction {
    async fn run(self, config: Config, kafka_config: ClientConfig) -> anyhow::Result<()> {
        let shutdown = create_shutdown()?;
        println!("running {:?}", self);
        match self {
            ArgsAction::Dedup => {
                println!("running Dedup");
                let config = config.dedup.ok_or_else(|| {
                    anyhow::anyhow!("`dedup` section in config should be defined")
                })?;
                Self::dedup(kafka_config, config, shutdown).await
            }
            ArgsAction::Grpc2Kafka => {
                println!("running Grpc2Kafka");
                let config = config.grpc2kafka.ok_or_else(|| {
                    anyhow::anyhow!("`grpc2kafka` section in config should be defined")
                })?;
                Self::grpc2kafka(kafka_config, config, shutdown).await
            }
            ArgsAction::Kafka2Grpc => {
                println!("running Kafka2Grpc");
                let config = config.kafka2grpc.ok_or_else(|| {
                    anyhow::anyhow!("`kafka2grpc` section in config should be defined")
                })?;
                Self::kafka2grpc(kafka_config, config, shutdown).await
            }
        }
    }

    async fn dedup(
        mut kafka_config: ClientConfig,
        config: ConfigDedup,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        for (key, value) in config.kafka.into_iter() {
            kafka_config.set(key, value);
        }

        // input
        let (consumer, kafka_error_rx1) =
            metrics::StatsContext::create_stream_consumer(&kafka_config)
                .context("failed to create kafka consumer")?;
        consumer.subscribe(&[&config.kafka_input])?;

        // output
        let (kafka, kafka_error_rx2) = metrics::StatsContext::create_future_producer(&kafka_config)
            .context("failed to create kafka producer")?;

        let mut kafka_error = false;
        let kafka_error_rx = futures::future::join(kafka_error_rx1, kafka_error_rx2);
        tokio::pin!(kafka_error_rx);

        // dedup
        let dedup = config.backend.create().await?;

        // input -> output loop
        let kafka_output = Arc::new(config.kafka_output);
        let mut send_tasks = JoinSet::new();
        loop {
            let message = tokio::select! {
                _ = &mut shutdown => break,
                _ = &mut kafka_error_rx => {
                    kafka_error = true;
                    break;
                }
                maybe_result = send_tasks.join_next() => match maybe_result {
                    Some(result) => {
                        result??;
                        continue;
                    }
                    None => tokio::select! {
                        _ = &mut shutdown => break,
                        _ = &mut kafka_error_rx => {
                            kafka_error = true;
                            break;
                        }
                        message = consumer.recv() => message,
                    }
                },
                message = consumer.recv() => message,
            }?;
            metrics::recv_inc();
            trace!(
                "received message with key: {:?}",
                message.key().and_then(|k| std::str::from_utf8(k).ok())
            );

            let (key, payload) = match (
                message
                    .key()
                    .and_then(|k| String::from_utf8(k.to_vec()).ok()),
                message.payload(),
            ) {
                (Some(key), Some(payload)) => (key, payload.to_vec()),
                _ => continue,
            };
            let Some((slot, hash, bytes)) = key
                .split_once('_')
                .and_then(|(slot, hash)| slot.parse::<u64>().ok().map(|slot| (slot, hash)))
                .and_then(|(slot, hash)| {
                    let mut bytes: [u8; 32] = [0u8; 32];
                    const_hex::decode_to_slice(hash, &mut bytes)
                        .ok()
                        .map(|()| (slot, hash, bytes))
                })
            else {
                continue;
            };
            debug!("received message slot #{slot} with hash {hash}");

            let kafka = kafka.clone();
            let dedup = dedup.clone();
            let kafka_output = Arc::clone(&kafka_output);
            send_tasks.spawn(async move {
                if dedup.allowed(slot, bytes).await {
                    let record = FutureRecord::to(&kafka_output).key(&key).payload(&payload);
                    match kafka.send_result(record) {
                        Ok(future) => {
                            let result = future.await;
                            debug!("kafka send message with key: {key}, result: {result:?}");

                            result?.map_err(|(error, _message)| error)?;
                            metrics::sent_inc(GprcMessageKind::Unknown);
                            Ok::<(), anyhow::Error>(())
                        }
                        Err(error) => Err(error.0.into()),
                    }
                } else {
                    metrics::dedup_inc();
                    Ok(())
                }
            });
            if send_tasks.len() >= config.kafka_queue_size {
                tokio::select! {
                    _ = &mut shutdown => break,
                    _ = &mut kafka_error_rx => {
                        kafka_error = true;
                        break;
                    }
                    result = send_tasks.join_next() => {
                        if let Some(result) = result {
                            result??;
                        }
                    }
                }
            }
        }
        if !kafka_error {
            warn!("shutdown received...");
            loop {
                tokio::select! {
                    _ = &mut kafka_error_rx => break,
                    result = send_tasks.join_next() => match result {
                        Some(result) => result??,
                        None => break
                    }
                }
            }
        }
        Ok(())
    }

    async fn grpc2kafka(
        mut kafka_config: ClientConfig,
        config: ConfigGrpc2Kafka,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        for (key, value) in config.kafka.into_iter() {
            print!("kafka_config:  key {}, value {}", &key, &value);
            kafka_config.set(key, value);
        }

        // Connect to kafka
        let (kafka, kafka_error_rx) = metrics::StatsContext::create_future_producer(&kafka_config)
            .context("failed to create kafka producer")?;
        let mut kafka_error = false;
        tokio::pin!(kafka_error_rx);

        let endpoints: Vec<String> = config
        .endpoint
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
        let mut ep_idx = 0;
        let ep_count = endpoints.len();

        loop {
            let ep = &endpoints[ep_idx];
            println!("trying connect to endpoint[{}]: {}", ep_idx, ep);

            let builder = GeyserGrpcClient::build_from_shared(ep.clone())?    // :contentReference[oaicite:0]{index=0}
            .x_token(config.x_token.clone())?                               // :contentReference[oaicite:1]{index=1}
            .connect_timeout(Duration::from_secs(10))                     // :contentReference[oaicite:2]{index=2}
            .timeout(Duration::from_secs(5))                              // :contentReference[oaicite:3]{index=3}
            .tls_config(ClientTlsConfig::new().with_native_roots())?;     // :contentReference[oaicite:4]{index=4}

            // 关键：用 builder.connect() 而非私有的 build()
            let mut client = match builder.connect().await {                 // :contentReference[oaicite:5]{index=5}
                Ok(c) => {
                    println!("connected success, gRPC client is ready");
                    c
                }
                Err(err) => {
                    println!("connected failed: {:?}, swtich to next endpoint", err);
                    ep_idx = (ep_idx + 1) % ep_count;
                    thread::sleep(Duration::from_millis(2000)); 
                    continue;
                }
            };

            let req = config.request.clone(); 

            println!("subscribe, {:?}", req); 
            // let mut geyser = client.subscribe_once(config.request.to_proto()).await?;
            let mut geyser = match client.subscribe_once(req.to_proto()).await {
                Ok(s) => s,
                Err(err) => {
                    println!("subscribe failed: {:?}, switch to next endpoint", err);
                    ep_idx = (ep_idx + 1) % ep_count;
                    thread::sleep(Duration::from_millis(2000)); 
                    continue;
                }
            };

            // Receive-send loop
            let mut send_tasks = JoinSet::new();
            'stream_loop: loop {
                let msg_result = tokio::select! {
                    _ = &mut shutdown => break,
                    _ = &mut kafka_error_rx => {
                        kafka_error = true;
                        break;
                    }
                    maybe_result = send_tasks.join_next() => match maybe_result {
                        Some(result) => {
                            result??;
                            continue;
                        }
                        None => tokio::select! {
                            _ = &mut shutdown => break,
                            _ = &mut kafka_error_rx => {
                                kafka_error = true;
                                break;
                            }
                            message = geyser.next() => message,
                        }
                    },
                    message = geyser.next() => message,
                }
                .transpose();

                let message;
                match msg_result {
                    Ok(Some(msg)) => {
                        message = msg;
                        // let payload = message.encode_to_vec();
                        let mut payload: Option<Vec<u8>> = None;
                        let message = match &message.update_oneof {
                            Some(value) => value,
                            None => unreachable!("Expect valid message"),
                        };
                        let slot = match message {
                            UpdateOneof::Account(msg) => msg.slot,
                            UpdateOneof::Slot(msg) => msg.slot,
                            UpdateOneof::Transaction(msg) => {
                                payload = msg.transaction.as_ref().and_then(|transaction| {
                                    let tx_data = transaction.encode_to_vec();
                                    let b64: String = general_purpose::STANDARD.encode(&tx_data);
                                    print!("tx_data: {}", b64);
                                    match crate::generated::prelude::SubscribeUpdateTransactionInfo::decode(tx_data.as_slice()) {
                                        Ok(tx) => {
                                            let tx_json = serde_json::to_string(&tx).unwrap();
                                            // print!("tx_json: {}", &tx_json);
                                            Some(tx_json.into_bytes())
                                        }
                                        Err(error) => {
                                            warn!("failed to decode message: {}", error);
                                            None
                                        }
                                    }
                                });
                                msg.slot
                            },
                            UpdateOneof::TransactionStatus(msg) => msg.slot,
                            UpdateOneof::Block(msg) => msg.slot,
                            UpdateOneof::Ping(_) => continue,
                            UpdateOneof::Pong(_) => continue,
                            UpdateOneof::BlockMeta(msg) => msg.slot,
                            UpdateOneof::Entry(msg) => msg.slot,
                        };
                        
                        let Some(send_data) = payload else {
                            continue;
                        };

                        let hash = Sha256::digest(&send_data);
                        let key = format!("{slot}_{}", const_hex::encode(hash));
                        let prom_kind = GprcMessageKind::from(message);
                        // print!("received data, key: {}\n", &key);

                        let record = FutureRecord::to(&config.kafka_topic)
                            .key(&key)
                            .payload(&send_data);

                        match kafka.send_result(record) {
                            Ok(future) => {
                                let _ = send_tasks.spawn(async move {
                                    let result = future.await;
                                    println!("kafka send message with key: {key}, result: {result:?}");

                                    let _ = result?.map_err(|(error, _message)| error)?;
                                    metrics::sent_inc(prom_kind);
                                    Ok::<(), anyhow::Error>(())
                                });
                                if send_tasks.len() >= config.kafka_queue_size {
                                    tokio::select! {
                                        _ = &mut shutdown => break,
                                        _ = &mut kafka_error_rx => {
                                            kafka_error = true;
                                            break;
                                        }
                                        result = send_tasks.join_next() => {
                                            if let Some(result) = result {
                                                result??;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(error) => return Err(error.0.into()),
                        }
                    }
                    Ok(None) => {
                        // closed by the remote peer
                        println!("gRPC is closed (Ok(None)), switch to next endpoint");  // 
                        break 'stream_loop;
                    }
                    Err(status) => {
                        // RPC/connection error
                        println!("rpc error(code={:?}): {}, switch to next endpoint", 
                                 status.code(), status.message());                  // 
                        break 'stream_loop;
                    }
                }
                ep_idx = (ep_idx + 1) % ep_count;
                thread::sleep(Duration::from_millis(2000));
            }
            if !kafka_error {
                warn!("shutdown received...");
                loop {
                    tokio::select! {
                        _ = &mut kafka_error_rx => break,
                        result = send_tasks.join_next() => match result {
                            Some(result) => result??,
                            None => break
                        }
                    }
                }
            }
        }
    }

    async fn kafka2grpc(
        mut kafka_config: ClientConfig,
        config: ConfigKafka2Grpc,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        for (key, value) in config.kafka.into_iter() {
            kafka_config.set(key, value);
        }

        let (grpc_tx, grpc_shutdown) = GrpcService::run(config.listen, config.channel_capacity)?;

        let (consumer, kafka_error_rx) =
            metrics::StatsContext::create_stream_consumer(&kafka_config)
                .context("failed to create kafka consumer")?;
        let mut kafka_error = false;
        tokio::pin!(kafka_error_rx);
        consumer.subscribe(&[&config.kafka_topic])?;

        loop {
            let message = tokio::select! {
                _ = &mut shutdown => break,
                _ = &mut kafka_error_rx => {
                    kafka_error = true;
                    break
                },
                message = consumer.recv() => message?,
            };
            metrics::recv_inc();
            debug!(
                "received message with key: {:?}",
                message.key().and_then(|k| std::str::from_utf8(k).ok())
            );

            if let Some(payload) = message.payload() {
                match SubscribeUpdate::decode(payload) {
                    Ok(message) => {
                        let _ = grpc_tx.send(message);
                    }
                    Err(error) => {
                        warn!("failed to decode message: {error}");
                    }
                }
            }
        }

        if !kafka_error {
            warn!("shutdown received...");
        }
        Ok(grpc_shutdown.await??)
    }
}

#[routes]
#[get("/health")]
#[get("/internal/health")]
async fn health() -> impl Responder {
    "OK"
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing()?;

    // Parse args
    let args = Args::parse();
    // let args = Args {
    //     config: "/home/luke/go/src/github.com/lukeweb3/yellowstone-grpc-kafka/config-kafka.json".to_string(),  // 必须提供 String 类型值
    //     prometheus: Some("127.0.0.1:9090".parse().unwrap()),  // Option<SocketAddr> 类型
    //     action: ArgsAction::Grpc2Kafka,   // 子命令枚举实例化
    // };
    let config = config_load::<Config>(&args.config).await?;

    // Run prometheus server
    if let Some(address) = args.prometheus.or(config.prometheus) {
        prometheus_run_server(address).await?;
    }

    // Create kafka config
    let mut kafka_config = ClientConfig::new();
    for (key, value) in config.kafka.iter() {
        kafka_config.set(key, value);
    }

    // args.action.run(config, kafka_config).await

    // Actix-web Server Future
    let actix_srv = HttpServer::new(|| {
        App::new()
            // register the macro-routed handler directly
            .service(health)
    })
    .bind(("127.0.0.1", 8080))?
    .run();

    let action = args.action.unwrap_or_default();
    let biz = action.run(config, kafka_config);
    let (srv_res, biz_res) = tokio::join!(actix_srv, biz);
    srv_res?; biz_res?;
    Ok(())
}
