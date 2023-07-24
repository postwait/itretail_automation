use config::{Config, ConfigError, Environment, File};
use serde_derive::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct ITRetail {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Mailchimp {
    pub token: String,
    pub dc: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Scales {
    pub addresses: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    pub itretail: ITRetail,
    pub mailchimp: Mailchimp,
    pub scales: Scales,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut token_filepath = PathBuf::new();
        match home::home_dir() {
            Some(path) => token_filepath.push(path),
            None => {
                return Err(ConfigError::Message("unknown home directory".to_owned()))
            }
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
            .add_source(
                File::with_name(&format!("{}/config", basepath))
                    .required(false),
            )
            .add_source(Environment::with_prefix("app"))
            // You may also programmatically change settings
            .set_default("itretail.username", "")?
            .set_default("itretail.password", "")?
            .set_default("mailchimp.token", "")?
            .set_default("mailchimp.dc", "us21")?
            .set_default("scales.addresses", Vec::<String>::with_capacity(0))?
            .build()?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_deserialize()
    }
}