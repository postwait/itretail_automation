use reqwest;
use anyhow::{anyhow, Result};

pub struct Light {
    ip: String,
}

pub fn new_light(ip: String) -> Light {
  Light{ ip: ip }
}

impl Light {
    pub async fn power(&mut self, state: bool) -> Result<()> {
        let client = reqwest::Client::new();
        let res = client.get(format!("http://{}/cm?cmnd=Power%20{}", self.ip, if state { "on" } else { "off" })).send().await;
        match res {
            Ok(result) => {
                result.text().await?;
                Ok(())
            },
            Err(e) => Err(anyhow!("{}", e.to_string())),
        }
    }
}