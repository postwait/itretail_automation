use reqwest;
use anyhow::{anyhow, Result};

pub struct Light {
    ip: String,
}

pub fn new_light(ip: String) -> Light {
  Light{ ip: ip }
}

impl Light {
    pub fn power(&mut self, state: bool) -> Result<()> {
        let client = reqwest::blocking::Client::new();
        let res = client.get(format!("http://{}/cm?cmnd=Power%20{}", self.ip, if state { "on" } else { "off" })).send();
        match res {
            Ok(result) => {
                let res = result.text();
                Ok(())
            },
            Err(e) => Err(anyhow!("{}", e.to_string())),
        }
    }
}