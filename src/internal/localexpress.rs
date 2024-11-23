use anyhow::{anyhow, Result};
use fancy_regex::Regex;
use chrono::{NaiveDate, NaiveDateTime, Local, Days, Months};
use home;
use log::*;
use reqwest;
use reqwest::Client;
use reqwest::header::CONTENT_TYPE;
use reqwest::cookie::Jar;

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::PathBuf;
use std::time::SystemTime;
use std::sync::Arc;

mod le_u64_string {
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse::<u64>().map_err(serde::de::Error::custom)
    }
}
mod le_date_format {
    use chrono::NaiveDate;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &str = "%Y-%m-%d";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDate::parse_from_str(&s, FORMAT)
            .map_err(serde::de::Error::custom)
    }
}
mod le_datetime_format {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &str = "%Y-%m-%d %H:%M:%S";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&s, FORMAT)
            .map_err(serde::de::Error::custom)
    }
}
#[derive(Serialize)]
#[allow(dead_code)]
struct Empty {}
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct OrdersResponse {
    message: String,
    code: String,
    data: OrdersData,
}
#[derive(Deserialize, Debug)]
struct OrdersData {
    result: Vec<Order>,
}

#[derive(Deserialize, Debug)]
pub struct ParkingSlotInfo {
    pub name: String,
    pub description: String,
}
#[derive(Deserialize, Debug)]
pub struct CurbsidePickupInfo {
    pub notes: String,
    pub parking_slot: ParkingSlotInfo,
}
#[derive(Deserialize, Debug)]
pub struct Order {
    #[serde(with = "le_u64_string")]
    pub id: u64,
    pub uniqid: String,
    #[serde(with = "le_u64_string")]
    pub store_id: u64,
    pub status: String,
    pub subtotal: String,
    pub tips: String,
    pub total: String,
    pub mode: String,
    pub payment_method: String,
    pub customer_first_name: String,
    pub customer_last_name: String,
    pub customer_phone_number: Option<String>,
    pub customer_email: Option<String>,
    #[serde(with = "le_datetime_format")]
    pub creation_date: NaiveDateTime,
    #[serde(with = "le_date_format")]
    pub delivery_date: NaiveDate,
    pub delivery_time_period: String,
    pub curbsidePickupInfo: Option<CurbsidePickupInfo>,
}

impl Order {
    pub fn active(&self) -> bool {
        self.status != "canceled" && self.status != "assembled" && self.status != "packed" && self.status != "delivering" && self.status != "delivered" && self.status != "picked_up"
    }
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

pub struct LEApi {
    backingfile: File,
    bearer_token: BearerToken,
    jar: Arc<Jar>,
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
                debug!("Error reading json: {}\nJSON: {}", err, json);
            }
            return BearerToken::default();
        }
    };
    bto
}

pub fn get_dotfile(filename: &str, writeable: bool) -> Result<File, anyhow::Error> {
    let mut token_filepath = PathBuf::new();
    match home::home_dir() {
        Some(path) => token_filepath.push(path),
        None => return Err(anyhow!("unknown home directory")),
    };
    token_filepath.push(".itretail");
    if !token_filepath.is_dir() {
        match std::fs::create_dir(&token_filepath) {
            Ok(()) => {}
            Err(err) => return Err(err.into()),
        }
    }
    token_filepath.push(filename);
    let file = if writeable {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(token_filepath)
    } else {
        OpenOptions::new()
            .read(true)
            .create(true)
            .open(token_filepath)
    };
    match file {
        Ok(f) => Ok(f),
        Err(err) => Err(err.into()),
    }
}

pub fn create_api() -> Result<LEApi> {
    let backingfile = get_dotfile("localexpress.json", true)?;
    Ok(LEApi {
        backingfile: backingfile,
        bearer_token: BearerToken::default(),
        jar: Arc::new(Jar::default()),
    })
}

impl LEApi {
    fn clear_token(&mut self) -> Result<()> {
        self.backingfile.set_len(0)?;
        self.bearer_token = BearerToken::default();
        Ok(())
    }

    fn client(&mut self, use_cookies: bool) -> Client {
        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none());
        if use_cookies {
            builder = builder
            .cookie_store(true)
            .cookie_provider(self.jar.clone())
        }
        builder.build().unwrap()
    }

    fn get_csrf_from_form(&mut self, doc: &String) -> Result<String> {
        /*
        Tried to parse the XML, but we can't use this because the XML is sloppy crap...
        */
        let re = Regex::new("<meta\\s+name=\"csrf-token\"\\s+content=\"([^\"]+)\"")?;
        if let Some(m) = re.captures(doc)? {
           return Ok(m[1].to_string());
        }
        Err(anyhow!("no CSRF in form"))
    }

    pub async fn auth(&mut self) -> Result<()> {
        let mut contents = String::new();
        self.backingfile.read_to_string(&mut contents)?;
        self.bearer_token = bearer_token_from_json(contents);

        let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(..) => u64::MAX,
        };
        if self.bearer_token.expires_at.unwrap_or(0) > now {
            debug!("Using stored token");
            return Ok(());
        }

        self.clear_token()?;

        let user = match env::var("LOCALEXPRESS_USERNAME") {
            Ok(p) => p,
            Err(..) => return Err(anyhow!("no username provided")),
        };
        let pass = match env::var("LOCALEXPRESS_PASSWORD") {
            Ok(p) => p,
            Err(..) => return Err(anyhow!("no password provided")),
        };

        debug!("Fetching token");
        let client = self.client(true);
        let res = client.get("https://partner.localexpress.io/auth/default/login").send().await;
        let tok = match res {
            Ok(result) => {
                let text = result.text().await?;
                self.get_csrf_from_form(&text)
            },
            _ => { Err(anyhow!("Failed to start login sequence")) }
        }?;
        let params = [
            ("_csrf", &tok),
            ("Login[login]", &user),
            ("Login[password]", &pass),
        ];
        let reqb = client
            .post("https://partner.localexpress.io/auth/default/login")
            .form(&params);
        let req = reqb.build().ok().unwrap();
        let res = client.execute(req).await;
        match res {
            Ok(result) => {
                let mut bt = BearerToken::default();
                for cookie in result.cookies() {
                    if cookie.name().eq("authToken") {
                        bt.token_type = "Bearer".to_string();
                        bt.access_token = cookie.value().to_string();
                        if let Some(exp) = cookie.expires() {
                            if let Ok(secs) = exp.duration_since(SystemTime::UNIX_EPOCH) {
                                bt.expires_at = Some(secs.as_secs());
                            }
                        }
                        break;
                    }
                }
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
            Err(e) => return Err(anyhow!("{}", e.to_string())),
        };

        return Ok(());
    }

    pub async fn call<T: Serialize + ?Sized>(
        &mut self,
        method: reqwest::Method,
        endpoint: &String,
        headers: Option<reqwest::header::HeaderMap>,
        json: Option<&T>,
    ) -> Result<String> {
        let client = self.client(false);
        let url = "https://api.localexpress.io".to_owned() + endpoint;
        let mut builder = client.request(method, url);
        if let Some(headers) = headers {
            builder = builder.headers(headers)
        }
        if let Some(json) = json {
            builder = builder.json(json)
        }
        builder = builder.bearer_auth(self.bearer_token.access_token.to_string());
        let res = builder.send().await;
        match res {
            Ok(result) => {
                if result.status().is_success() {
                    let text_response = result.text().await?;
                    Ok(text_response)
                } else {
                    Err(anyhow!(
                        "{}",
                        result
                            .status()
                            .canonical_reason()
                            .unwrap_or(&format!("UNKNOWN CODE: {}", result.status().as_str()))
                    ))
                }
            }
            Err(e) => Err(anyhow!("{}", e.to_string())),
        }
    }

    /*
    pub fn call_multi<T: Serialize + ?Sized>(
        &mut self,
        method: reqwest::Method,
        endpoint: &String,
        headers: Option<reqwest::header::HeaderMap>,
        form: multipart::Form,
    ) -> Result<String> {
        let client = self.client(false);
        let url = "https://localexpress.io".to_owned() + endpoint;
        let mut builder = client.request(method, url);
        if let Some(headers) = headers {
            builder = builder.headers(headers)
        }
        builder = builder.multipart(form);
        builder = builder.bearer_auth(self.bearer_token.access_token.to_string());
        let res = builder.send();
        match res {
            Ok(result) => {
                if result.status().is_success() {
                    let text_response = result.text()?;
                    Ok(text_response)
                } else {
                    Err(anyhow!(
                        "{}",
                        result
                            .status()
                            .canonical_reason()
                            .unwrap_or(&format!("UNKNOWN CODE: {}", result.status().as_str()))
                    ))
                }
            }
            Err(e) => Err(anyhow!("{}", e.to_string())),
        }
    }
    */

    pub async fn post_json<T: Serialize + ?Sized>(
        &mut self,
        endpoint: &String,
        json: &T,
    ) -> Result<String> {
        let mut json_hdrs = reqwest::header::HeaderMap::new();
        json_hdrs.insert(
            CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        self.call(reqwest::Method::POST, endpoint, Some(json_hdrs), Some(json)).await
    }

    pub async fn get_orders(&mut self) -> Result<Vec<Order>> {
        let endpoint = "/rest/v2/store/all/order?expand=productsCount,driverName&perPage=50&page=0".to_string();
        let filter = json!({});//"filter":{"status":["new","confirmed","assembling","assembled","packing","packed"]},"filterType":"basic"});
        let r = self.post_json(&endpoint, &filter).await?;
        let response: OrdersResponse = serde_json::from_str(&r)?;
        Ok(response.data.result)
    }

    pub async fn get_current_orders(&mut self) -> Result<Vec<Order>> {
        let yesterday = Local::now().date_naive().checked_sub_days(Days::new(30)).unwrap();
        let future = yesterday.checked_add_months(Months::new(3)).unwrap();
        // https://api.localexpress.io/rest/v2/store/3920/order/7444491/details?expand=assembledByEmail%2CexcludeFromCollectingThrottling%2CadditionalFees%2CcurbsidePickupInfo%2Cpacks%2Cproducts%2Cwrapping%2Ctransactions%2CappliedTaxes%2CcouponDeduction%2CproductShippingPackagingBoxes%2CshippingTransactions%2CisAgeVerificationRequired%2CisAgeVerified%2CpreSelectedShippingMessage%2CshippingRate%2Cleft_to_pay%2ChasDeliProducts%2CcouponCode%2CcouponName%2CdeliveryFeeRemoval%2CcollectingFeeRemoval%2CnotFinalizedCustomerRelatedOrders%2CorderSummary&productExpand=modification%2Cdiscounts%2Cdiscount%2CdiscountPrice%2CproductPriceUnits%2CadditionalDiscount
        let endpoint = "/rest/v2/store/all/order?expand=productsCount%2CcurbsidePickupInfo,driverName&perPage=100&page=0".to_string();
        let filter = json!({"filter":{"creation_date":[yesterday.format("%Y-%m-%d").to_string(),future.format("%Y-%m-%d").to_string()]},"filterType":"basic"});
        let r = self.post_json(&endpoint, &filter).await?;
        let response: OrdersResponse = serde_json::from_str(&r)?;
        Ok(response.data.result)
    }
}
