use home;
use reqwest;
use reqwest::header::CONTENT_TYPE;
use anyhow::{anyhow, Result};

use serde::{Deserialize, Serialize};
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Serialize)]
struct Empty {
}
#[derive(Serialize, Deserialize, Debug)]
pub struct MinimalCustomer {
    #[serde(rename = "FirstName")]
    pub first_name: String,
    #[serde(rename = "LastName")]
    pub last_name: String,
    #[serde(rename = "Email")]
    pub email: String,
    #[serde(rename = "Phone")]
    pub phone: String,
}
#[derive(Deserialize, Debug)]
pub struct Customer {
    #[serde(rename = "Id")]
    pub id: Option<String>,
    #[serde(rename = "CardNo")]
    pub card_no: Option<String>,
    #[serde(rename = "LastName")]
    pub last_name: String,
    #[serde(rename = "FirstName")]
    pub first_name: String,
    #[serde(rename = "BirthDate")]
    pub birth_date: Option<String>,
    #[serde(rename = "Phone")]
    pub phone: String,
    #[serde(rename = "Discount")]
    pub discount: Option<u8>,
    #[serde(rename = "Deleted")]
    pub deleted: bool,
    #[serde(rename = "Email")]
    pub email: String,
    #[serde(rename = "Balance")]
    pub balance: Option<f64>,
    #[serde(rename = "BalanceLimit")]
    pub balance_limit: Option<f64>,
    #[serde(rename = "LoyaltyPoints")]
    pub loyalty_points: Option<u32>,
    #[serde(rename = "ExpirationDate")]
    pub expiration_date: Option<String>,
    #[serde(rename = "InstoreChargeEnabled")]
    pub instore_charge_enabled: bool,
}

#[derive(Deserialize, Debug)]
pub struct ProductData {
    pub upc: String,
    pub description: String,
    #[serde(rename = "secondDescription")]
    pub second_description: Option<String>,
    pub normal_price: f64,
    pub scale: bool,
    pub active: bool,
    #[serde(rename = "Deleted")]
    pub deleted: bool,
    #[serde(rename = "PLU")]
    pub plu: Option<String>,
    #[serde(rename = "departmentId")]
    pub department_id: i32,
    pub wicable: Option<i32>,
    pub foodstamp: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
struct BearerToken {
    access_token: String,
    token_type: String,
    expires_in: u64,
    expires_at: Option<u64>,
}

impl Default for BearerToken {
    fn default() -> Self {
        BearerToken {
            access_token: String::new(),
            token_type: String::new(),
            expires_in: 0,
            expires_at: None,
        }
    }
}

pub struct ITRApi {
    backingfile: File,
    bearer_token: BearerToken,
}

fn bearer_token_from_json(json: String) -> BearerToken {
    let bto: BearerToken = match serde_json::from_str::<BearerToken>(&json) {
        Ok(bt_ro) => {
            let mut bt = bt_ro;
            if bt.expires_at.is_none() && bt.expires_in > 0 {
                match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                    Ok(n) => {
                        bt.expires_at = Some(bt.expires_in + n.as_secs());
                        ()
                    }
                    Err(..) => (),
                }
            };
            bt
        }
        Err(err) => {
            if json.len() > 0 {
                println!("Error reading json: {}\nJSON: {}", err, json);
            }
            return BearerToken::default();
        }
    };
    bto
}

pub fn create_api() -> Result<ITRApi> {
    let mut token_filepath = PathBuf::new();
    match home::home_dir() {
        Some(path) => token_filepath.push(path),
        None => {
            return Err(anyhow!("unknown home directory"))
        }
    };
    token_filepath.push(".itretail");
    if !token_filepath.is_dir() {
        match std::fs::create_dir(&token_filepath) {
            Ok(()) => {}
            Err(err) => return Err(err.into()),
        }
    }
    token_filepath.push("token.json");
    let backingfile = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(token_filepath);
    if backingfile.is_err() {
        return Err(backingfile.err().unwrap().into());
    }
    Ok(ITRApi {
        backingfile: backingfile.ok().unwrap(),
        bearer_token: BearerToken::default(),
    })
}
impl ITRApi {
    fn clear_token(&mut self) -> Result<()> {
        self.backingfile.set_len(0)?;
        self.bearer_token = BearerToken::default();
        Ok(())
    }

    pub fn auth(&mut self) -> Result<()> {
        let mut contents = String::new();
        self.backingfile.read_to_string(&mut contents)?;
        self.bearer_token = bearer_token_from_json(contents);

        let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(..) => u64::MAX,
        };
        if self.bearer_token.expires_at.unwrap_or(0) > now {
            println!("Using stored token");
            return Ok(());
        }

        self.clear_token()?;

        println!("Fetching token");
        let client = reqwest::blocking::Client::new();
        let user = match env::var("ITRETAIL_USER") {
            Ok(p) => p,
            Err(..) => {
                return Err(anyhow!("no username provided"))
            }
        };
        let pass = match env::var("ITRETAIL_PASS") {
            Ok(p) => p,
            Err(..) => {
                return Err(anyhow!("no password provided"))
            }
        };
        let params = [
            ("grant_type", "password"),
            ("username", &user),
            ("password", &pass),
        ];
        let res = client
            .post("https://retailnext.itretail.com/token?accesslevel=0&securityCode=undefined")
            .form(&params)
            .send();
        match res {
            Ok(result) => {
                let text_response = result.text();
                let bt = bearer_token_from_json(text_response.ok().unwrap());
                self.backingfile.set_len(0)?;
                self.backingfile.rewind()?;
                self.backingfile.write_all(
                    serde_json::to_string(&bt)
                        .ok()
                        .unwrap_or(r"".to_string())
                        .as_bytes(),
                )?;
                self.backingfile.sync_all()?;
                bt
            }
            Err(e) => {
                return Err(anyhow!("{}", e.to_string()))
            }
        };

        return Ok(());
    }

    pub fn call<T: Serialize + ?Sized>(&mut self, method: reqwest::Method, endpoint: &String, headers: Option<reqwest::header::HeaderMap>, json: Option<&T>) -> Result<String> {
        let client = reqwest::blocking::Client::new();
        let url = "https://retailnext.itretail.com".to_owned() + endpoint;
        let mut builder = client
            .request(method, url);
        if let Some(headers) = headers {
            builder = builder.headers(headers)
        }
        if let Some(json) = json {
            builder = builder.json(json)
        }
        builder = builder.bearer_auth(self.bearer_token.access_token.to_string());
        let res = builder.send();
        match res {
            Ok(result) => {
                if result.status().is_success() {
                    let text_response = result.text()?;
                    Ok(text_response)
                }  else {
                    Err(anyhow!("{}", result.status().canonical_reason().unwrap_or(&format!("UNKNOWN CODE: {}", result.status().as_str()))))
                }
            }
            Err(e) => Err(anyhow!("{}", e.to_string()))
        }
    }

    pub fn post_json<T: Serialize + ?Sized>(&mut self, endpoint: &String, json: &T) -> Result<String> {
        let mut json_hdrs = reqwest::header::HeaderMap::new();
        json_hdrs.insert(CONTENT_TYPE, reqwest::header::HeaderValue::from_static("application/json"));
        self.call(reqwest::Method::POST, endpoint, Some(json_hdrs), Some(json))
    }

    pub fn get(&mut self, endpoint: &String) -> Result<String> {
        self.call::<Empty>(reqwest::Method::GET, endpoint, None, None)
    }

    pub fn get_customers(&mut self) -> Result<Vec<Customer>> {
        let results = self.get(&"/api/CustomersData/GetAllCustomers".to_string()).expect("no results from API call");
        let customers: Vec<Customer> = serde_json::from_str(&results)?;
        Ok(customers)
    }

    pub fn make_customer(&mut self, c: &MinimalCustomer) -> Result<String> {
        self.post_json(&"/api/CustomersData/Post".to_string(), c)
    }
}
