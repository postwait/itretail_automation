use config::{Config, ConfigError, Environment, File};
use serde_derive::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct ITRetail {
    pub username: String,
    pub password: String,
    pub store_id: String,
    pub external_sale_shrink_reason: u32,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct LocalExpress {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct Mailchimp {
    pub token: String,
    pub dc: String,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct Scales {
    pub addresses: Vec<String>,
    pub timeout_seconds: u32,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub enum SquareEnvironment {
    Production,
    Sandbox,
}

impl Into<config::ValueKind> for SquareEnvironment {
    fn into(self) -> config::ValueKind {
        match self {
            SquareEnvironment::Production => config::ValueKind::String(String::from("Production")),
            SquareEnvironment::Sandbox => config::ValueKind::String(String::from("Sandbox"))
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct Square {
    pub environment: SquareEnvironment,
    pub sandbox_appid: String,
    pub sandbox_secret: String,
    pub production_appid: String,
    pub production_secret: String,
    pub location: String,
    pub max_retries: u32,
    pub weight_unit: String,
    pub weight_precision: i32,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct Postgres {
    pub connect_string: String,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct Tasmota {
    pub light1: String,
    pub light2: String,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct Settings {
    pub itretail: ITRetail,
    pub localexpress: LocalExpress,
    pub mailchimp: Mailchimp,
    pub postgres: Postgres,
    pub scales: Scales,
    pub square: Square,
    pub tasmota: Tasmota,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut token_filepath = PathBuf::new();
        match home::home_dir() {
            Some(path) => token_filepath.push(path),
            None => return Err(ConfigError::Message("unknown home directory".to_owned())),
        };
        token_filepath.push(".itretail");
        if !token_filepath.is_dir() {
            match std::fs::create_dir(&token_filepath) {
                Ok(()) => {}
                Err(err) => return Err(ConfigError::Foreign(Box::new(err))),
            }
        }
        let basepath = token_filepath.to_str().unwrap();

        let s = Config::builder()
            .add_source(File::with_name(&format!("{}/config", basepath)).required(false))
            .add_source(Environment::with_prefix("app"))
            // You may also programmatically change settings?
            .set_default("itretail.store_id", "")?
            .set_default("itretail.username", "")?
            .set_default("itretail.password", "")?
            .set_default("itretail.external_sale_shrink_reason", 5)?
            .set_default("postgres.connect_string", "")?
            .set_default("mailchimp.token", "")?
            .set_default("mailchimp.dc", "us21")?
            .set_default("scales.addresses", Vec::<String>::with_capacity(0))?
            .set_default("scales.timeout_seconds", 300)?
            .set_default("square.environment", "Production")?
            .set_default("square.sandbox_appid", "")?
            .set_default("square.sandbox_secret", "")?
            .set_default("square.production_appid", "")?
            .set_default("square.production_secret", "")?
            .set_default("square.location", "")?
            .set_default("square.weight_unit", "IMPERIAL_POUND")?
            .set_default("square.weight_precision", 3)?
            .set_default("square.location", "")?
            .set_default("square.max_retries", 3)?
            .set_default("tasmota.light1", "192.168.202.7")?
            .set_default("tasmota.light2", "192.168.202.151")?
            .build()?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_deserialize()
    }
}
