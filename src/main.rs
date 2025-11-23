use anyhow::Result;
use clap::Parser;
use config::{Config as ConfigLoader, File};
use graphyne::{GraphiteClient, GraphiteMessage};
use log::{error, info, warn};
use rumqttc::{Client, MqttOptions, QoS};
use serde::Deserialize;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
struct MqttConfig {
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
    client_id: String,
    topic: String,
}

#[derive(Debug, Deserialize, Clone)]
struct GraphiteConfig {
    host: String,
    port: u16,
    prefix: String,
}

#[derive(Debug, Deserialize, Clone)]
struct MappingsConfig {
    prefixes: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Clone)]
struct Config {
    mqtt: MqttConfig,
    graphite: GraphiteConfig,
    mappings: Option<MappingsConfig>,
}

fn resolve_address(address: &str) -> anyhow::Result<SocketAddr> {
    address
        .to_socket_addrs()?
        .find(|addr| addr.is_ipv4())
        .ok_or_else(|| anyhow::anyhow!("failed to resolve graphite server address: {}", address))
}

fn create_graphite_client(config: &GraphiteConfig) -> anyhow::Result<GraphiteClient> {
    let address = format!("{}:{}", config.host, config.port);
    let resolved_address = resolve_address(&address)?;

    GraphiteClient::builder()
        .address(resolved_address.ip().to_string())
        .port(resolved_address.port())
        .build()
        .map_err(|e| anyhow::anyhow!(e))
}

fn process_payload(payload: &[u8]) -> Option<f64> {
    let value_str = match String::from_utf8(payload.to_vec()) {
        Ok(v) => v,
        Err(e) => {
            warn!("Failed to parse payload as UTF-8: {}", e);
            return None;
        }
    };

    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&value_str) {
        if let Some(value_field) = json_value.get("value") {
            value_field.as_f64()
        } else {
            json_value.as_f64()
        }
    } else if let Ok(val) = value_str.parse::<f64>() {
        Some(val)
    } else {
        warn!(
            "Could not extract numeric value from payload: {}",
            value_str
        );
        None
    }
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    let config = ConfigLoader::builder()
        .add_source(File::from(args.config))
        .build()?
        .try_deserialize::<Config>()?;

    let mut graphite_client = create_graphite_client(&config.graphite)
        .map_err(|e| anyhow::anyhow!("Failed to create Graphite client: {}", e))?;

    let mut mqtt_options = MqttOptions::new(
        config.mqtt.client_id.clone(),
        config.mqtt.host.clone(),
        config.mqtt.port,
    );
    if let (Some(username), Some(password)) =
        (config.mqtt.username.clone(), config.mqtt.password.clone())
    {
        mqtt_options.set_credentials(username, password);
    }
    mqtt_options.set_keep_alive(Duration::from_secs(5));

    let (client, mut connection) = Client::new(mqtt_options, 10);

    // Subscribe on connect/reconnect
    let subscribed_topic_path = PathBuf::from(&config.mqtt.topic);
    let topic = format!("{}/#", config.mqtt.topic);
    client.subscribe(topic.clone(), QoS::AtMostOnce)?;

    info!("Waiting for MQTT connection...");

    for notification in connection.iter() {
        match notification {
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                info!("MQTT connected! Subscribing to topic '{}'...", topic);
                if let Err(e) = client.subscribe(topic.clone(), QoS::AtMostOnce) {
                    error!("Failed to subscribe after connect: {}", e);
                }
            }
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Disconnect)) => {
                warn!("MQTT client disconnected, will try to reconnect...");
            }
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                if let Some(value) = process_payload(&publish.payload) {
                    let topic_path = PathBuf::from(publish.topic.strip_suffix("/state").unwrap_or(&publish.topic));
                    dbg!(&topic_path, &subscribed_topic_path);
                    let clean_path = topic_path.strip_prefix(&subscribed_topic_path).unwrap();

                    let mut metric_path = clean_path
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy().to_string())
                        .collect::<Vec<_>>();

                    if let Some(last_part) = metric_path.last_mut() {
                        if let Some(mappings) = &config.mappings {
                            if let Some(prefixes) = &mappings.prefixes {
                                for prefix in prefixes {
                                    let prefix_with_underscore = format!("{}_", prefix);
                                    if last_part.starts_with(&prefix_with_underscore) {
                                        *last_part = last_part.replacen(
                                            &prefix_with_underscore,
                                            &format!("{}.", prefix),
                                            1,
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    let metric_name = format!(
                        "{}.{}",
                        config.graphite.prefix,
                        metric_path.join(".")
                    );
                    let message = GraphiteMessage::new(&metric_name, &value.to_string());
                    if let Err(e) = graphite_client.send_message(&message) {
                        error!("failed to send metric to graphite: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("MQTT connection error: {}", e);
                std::thread::sleep(Duration::from_secs(5));
            }
            _ => {}
        }
    }

    Ok(())
}
