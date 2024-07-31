use anyhow::{anyhow, Result};
use chrono::{DateTime, Days, Local, NaiveDateTime, SecondsFormat, Timelike, Utc};
use home;
use log::*;
use reqwest;
use reqwest::multipart;
use reqwest::header::CONTENT_TYPE;

use serde::{Deserialize, Serialize};
use serde::de::{Deserializer};
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::PathBuf;
use std::time::SystemTime;
use uuid::Uuid;

pub struct PLUAssignment {
    pub upc: String,
    pub plu: u16,
}
#[derive(Serialize)]
struct Empty {}
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
    #[serde(rename = "FrequentShopper")]
    pub frequent_shopper: bool,
}
#[derive(Deserialize, Debug)]
pub struct Section {
    pub id: u32,
    pub name: String,
    #[allow(dead_code)]
    pub deleted: bool,
}
#[derive(Deserialize, Debug)]
pub struct EJTxnProductChange {
    pub upc: String
}
#[derive(Deserialize, Debug)]
pub struct EJTxnProduct {
    #[serde(rename = "Id")]
    pub id: Uuid,
    #[serde(rename = "ProductId")]
    pub product_id: Option<Uuid>,
    #[serde(rename = "Weight")]
    pub weight: Option<f64>,
    #[serde(rename = "Quantity")]
    pub quantity: f64,
    #[serde(rename = "Price")]
    pub price: f64,
    #[serde(rename = "IsVoided")]
    pub is_voided: bool,
    #[serde(rename = "IsRefunded")]
    pub is_refunded: bool,
    #[serde(rename = "LineDiscount")]
    pub line_discount: f64,
    #[serde(rename = "ProductChange")]
    pub product_change: Option<EJTxnProductChange>,
}
#[derive(Deserialize, Debug)]
pub struct EJTxn {
    #[serde(rename = "Id")]
    pub id: Uuid,
    #[allow(dead_code)]
    #[serde(rename = "CustomerLastName")]
    pub customer_last_name: Option<String>,
    #[allow(dead_code)]
    #[serde(rename = "CustomerFirstName")]
    pub customer_first_name: Option<String>,
    #[serde(rename = "CustomerId")]
    pub customer_id: Option<Uuid>,
    #[serde(rename = "Canceled")]
    pub canceled: bool,
    #[serde(rename = "Total")]
    pub total: f64,
    #[serde(rename = "TransactionDate")]
    pub transaction_date: String,
    #[serde(rename = "TransactionProducts")]
    pub transaction_products: Option<Vec<EJTxnProduct>>,
}
#[derive(Deserialize, Debug)]
struct EJTAnswer {
    value: Vec<EJTxn>,
}
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Customer {
    #[serde(rename = "Id")]
    pub id: Uuid,
    #[serde(rename = "CardNo")]
    pub card_no: Option<String>,
    #[serde(rename = "LastName")]
    pub last_name: String,
    #[serde(rename = "FirstName")]
    pub first_name: String,
    #[serde(rename = "BirthDate")]
    pub birth_date: Option<String>,
    #[serde(rename = "Phone")]
    pub phone: Option<String>,
    #[serde(rename = "Discount")]
    pub discount: Option<u8>,
    #[serde(rename = "Deleted")]
    pub deleted: bool,
    #[serde(rename = "Email")]
    pub email: Option<String>,
    #[serde(rename = "Balance")]
    pub balance: Option<f64>,
    #[serde(rename = "BalanceLimit")]
    pub balance_limit: Option<f64>,
    #[serde(rename = "LoyaltyPoints")]
    pub loyalty_points: Option<i32>,
    #[serde(rename = "ExpirationDate")]
    pub expiration_date: Option<String>,
    #[serde(rename = "InstoreChargeEnabled")]
    pub instore_charge_enabled: Option<bool>,
    #[serde(rename = "Address1")]
    pub address1: Option<String>,
    #[serde(rename = "Address2")]
    pub address2: Option<String>,
    #[serde(rename = "City")]
    pub city: Option<String>,
    #[serde(rename = "State")]
    pub state: Option<String>,
    #[serde(rename = "Zip")]
    pub zipcode: Option<String>,
    #[serde(rename = "Created")]
    pub created: Option<String>,
    #[serde(rename = "Modified")]
    pub modified: Option<String>,
    #[serde(rename = "ModifiedBy")]
    pub modified_by: Option<u32>,
    #[serde(rename = "FrequentShopper")]
    pub frequent_shopper: Option<bool>,
    #[serde(rename = "CashBack")]
    pub cash_back: Option<f64>,
    #[serde(rename = "Inc")] // WTF is this?
    pub inc: Option<u32>,
}
#[derive(Deserialize, Debug)]
pub struct CustomersAnswer {
    pub value: Vec<Customer>
}

#[derive(Deserialize, Debug)]
pub struct ITRTaxId(pub Option<i32>);
fn deserialize_itrtaxid<'de, D>(deserializer: D) -> Result<ITRTaxId, D::Error>
where D: Deserializer<'de> {
    let buf = String::deserialize(deserializer)?;
    match buf.parse::<i32>() {
        Err(_) => Ok(ITRTaxId(None)),
        Ok(v) => Ok(ITRTaxId(Some(v)))
    }
}

#[derive(Deserialize, Debug)]
pub struct Tax {
    #[serde(rename = "Id")]
    pub id: ITRTaxId,
    #[serde(rename = "Description")]
    pub description: String,
    #[serde(rename = "TaxRate")]
    pub rate: f64,
    pub squareup_id: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ITRTaxAnswer {
    value: Vec<Tax>
}

#[derive(Deserialize, Debug)]
pub struct ProductData {
    pub upc: String,
    pub description: String,
    #[serde(rename = "secondDescription")]
    pub second_description: Option<String>,
    pub normal_price: f64,
    pub special_price: Option<f64>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub scale: bool,
    pub active: bool,
    pub discountable: i32,
    #[serde(rename = "Deleted")]
    pub deleted: bool,
    #[serde(rename = "PLU")]
    pub plu: Option<String>,
    pub cert_code: Option<String>,
    #[serde(rename = "vendorId")]
    pub vendor_id: Option<i32>,
    #[serde(rename = "departmentId")]
    pub department_id: i32,
    #[serde(rename = "sectionId")]
    pub section_id: Option<i32>,
    pub wicable: Option<i32>,
    pub foodstamp: Option<bool>,
    #[serde(rename = "QuantityOnHand")]
    pub quantity_on_hand: Option<f32>,
    pub size: Option<String>,
    pub case_cost: Option<f32>,
    pub pack: Option<i32>,
    pub cost: Option<f32>,
    #[serde(deserialize_with = "deserialize_itrtaxid", rename="taxes")]
    pub taxclass: ITRTaxId,
}

impl ProductData {
    pub fn get_price_as_of(&self, whence: DateTime<Local>) -> f64 {
        // IT Retail is a distater.  It can't get timestamps on sales start/end right.
        // The DB/API has hours set, but the UI doesn't allow specifying them and they seem somewhat randomly assigned.
        // More of a crime, the hours for the end date appear to be KNOWN to be wrong and the app ignores them and pretends they are 23:59:59.
        // WTF writes code like this?

        // As such, it is on us to floor the start and ceiling the end dates for the purposes of comparison
        if self.start_date.is_some() {
            let itr_start = match NaiveDateTime::parse_from_str(self.start_date.as_ref().unwrap(), "%Y-%m-%dT%H:%M:%S") {
                Ok(utc_start) => { Ok(DateTime::<Utc>::from_naive_utc_and_offset(utc_start, Utc)
                                                        .with_hour(0).unwrap()
                                                        .with_minute(0).unwrap()
                                                        .with_second(0).unwrap()) },
                _ => Err(())
            };
            if itr_start.is_ok() && itr_start.unwrap() <= whence {

                if self.end_date.is_some() {
                    let itr_end = match NaiveDateTime::parse_from_str(self.end_date.as_ref().unwrap(), "%Y-%m-%dT%H:%M:%S") {
                        Ok(utc_end) => { Ok(DateTime::<Utc>::from_naive_utc_and_offset(utc_end, Utc)
                                                            .with_hour(23).unwrap()
                                                            .with_minute(59).unwrap()
                                                            .with_second(59).unwrap()) },
                        _ => Err(())
                    };
                    if itr_end.is_ok() && itr_end.unwrap() <= whence {
                        debug!("Product {} has sale in past ({:#?}) {} <= {}", self.description, self.end_date, itr_end.as_ref().unwrap(), whence);
                        return self.normal_price; // expired
                    }
                }
                debug!("Product {} has sale now", self.description);
                return self.special_price.unwrap_or(self.normal_price)
            } else {
                debug!("Product {} has sale in future {} > {}", self.description, itr_start.as_ref().unwrap(), whence);
            }
        }
        self.normal_price
    }
    pub fn get_price(&self) -> f64 {
        self.get_price_as_of(Local::now())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Shortcut {
    #[serde(rename = "CategoryId")]
    pub category_id: u32,
    #[serde(rename = "Id")]
    pub id: u32,
    #[serde(rename = "Keystrokes")]
    pub keystrokes: Option<String>,
    #[serde(rename = "Sort")]
    pub sort: u32,
    #[serde(rename = "Text")]
    pub text: Option<String>,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Category {
    #[serde(rename = "Id")]
    pub id: u32,
    #[serde(rename = "Sort")]
    pub sort: u32,
    #[serde(rename = "Text")]
    pub text: Option<String>,
    #[serde(rename = "ProductLookupButtons")]
    pub product_shortcuts: Vec<Shortcut>,
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
    store_id: String,
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
                warn!("Error reading json: {}\nJSON: {}", err, json);
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

pub fn create_api() -> Result<ITRApi> {
    let backingfile = get_dotfile("token.json", true)?;
    Ok(ITRApi {
        backingfile: backingfile,
        store_id: env::var("ITRETAIL_STOREID")?,
        bearer_token: BearerToken::default(),
    })
}

impl ITRApi {
    fn clear_token(&mut self) -> Result<()> {
        self.backingfile.set_len(0)?;
        self.bearer_token = BearerToken::default();
        Ok(())
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

        debug!("Fetching token");
        let client = reqwest::Client::new();
        let user = match env::var("ITRETAIL_USERNAME") {
            Ok(p) => p,
            Err(..) => return Err(anyhow!("no username provided")),
        };
        let pass = match env::var("ITRETAIL_PASSWORD") {
            Ok(p) => p,
            Err(..) => return Err(anyhow!("no password provided")),
        };
        let params = [
            ("grant_type", "password"),
            ("username", &user),
            ("password", &pass),
        ];
        let res = client
            .post("https://retailnext.itretail.com/token?accesslevel=0&securityCode=undefined")
            .form(&params)
            .send().await;
        match res {
            Ok(result) => {
                let text_response = result.text().await;
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
        let client = reqwest::Client::new();
        let url = "https://retailnext.itretail.com".to_owned() + endpoint;
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

    pub async fn call_multi<T: Serialize + ?Sized>(
        &mut self,
        method: reqwest::Method,
        endpoint: &String,
        headers: Option<reqwest::header::HeaderMap>,
        form: multipart::Form,
    ) -> Result<String> {
        let client = reqwest::Client::new();
        let url = "https://retailnext.itretail.com".to_owned() + endpoint;
        let mut builder = client.request(method, url);
        if let Some(headers) = headers {
            builder = builder.headers(headers)
        }
        builder = builder.multipart(form);
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

    pub async fn set_plu(&mut self, plus: Vec<PLUAssignment>) -> Result<String> {
        let endpoint = &"/api/ProductsData/UpdateOnly".to_string();
        let mut csvcontents = "UPC,PLU\r\n".to_string();
        csvcontents.push_str(
            &plus
                .iter()
                .map(|ass| format!("{},{}\r\n", ass.upc, ass.plu))
                .collect::<String>(),
        );
        let part = reqwest::multipart::Part::text(csvcontents)
            .file_name("plu.csv")
            .mime_str("text/plain")?;
        let form = reqwest::multipart::Form::new();
        let form = form.part("1", part);
        let form = form
            .text("2", "[\"upc\",\"PLU\"]")
            .text("3", "false")
            .text("5[0]", "198dd573-ca6e-435a-b779-98922ad0185a");
        let r = self.call_multi::<Empty>(reqwest::Method::POST, endpoint, None, form).await;
        r
    }

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

    pub async fn put_json<T: Serialize + ?Sized>(
        &mut self,
        endpoint: &String,
        json: &T,
    ) -> Result<String> {
        let mut json_hdrs = reqwest::header::HeaderMap::new();
        json_hdrs.insert(
            CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        self.call(reqwest::Method::PUT, endpoint, Some(json_hdrs), Some(json)).await
    }

    pub async fn get(&mut self, endpoint: &String) -> Result<String> {
        self.call::<Empty>(reqwest::Method::GET, endpoint, None, None).await
    }

    pub async fn get_customers(&mut self) -> Result<Vec<Customer>> {
        let results = self
            .get(&"/api/CustomersData/Get?$select=%2A".to_string())
            .await
            .expect("no results from API call");
        let answer: CustomersAnswer = serde_json::from_str(&results)?;
        Ok(answer.value)
    }

    pub async fn get_customer(&mut self, cid: &Uuid) -> Result<Option<Customer>> {
        let url = format!("/api/CustomersData/GetOne/?Id={}", cid);
        let results = self.get(&url).await.expect("no results from API call");
        if results.trim() == "null" {
            return Ok(None);
        }
        let customer: Result<Customer, serde_json::Error> = serde_json::from_str(&results);
        if customer.is_err() {
            warn!(
                "ERROR: {}\nJSON: {}",
                customer.as_ref().err().unwrap(),
                results
            );
            return Err(customer.err().unwrap().into());
        }
        Ok(Some(customer.unwrap()))
    }

    pub async fn get_sections(&mut self) -> Result<Vec<Section>> {
        let results = self
            .get(&"/api/SectionsData/GetAllSections".to_string())
            .await
            .expect("no results from API call");
        let sections: Vec<Section> = serde_json::from_str(&results)?;
        Ok(sections)
    }

    pub async fn get_products(&mut self) -> Result<Vec<ProductData>> {
        let results = self
            .get(&"/api/ProductsData/GetAllProducts".to_string())
            .await
            .expect("no results from API call");
        let products: Vec<ProductData> = serde_json::from_str(&results)?;
        Ok(products)
    }

    pub async fn get_tax(&mut self) -> Result<Vec<Tax>> {
        let results = self
            .get(&"/api/TaxesData/Get?$orderby=Id&$select=Id,Description,Identifier,TaxRate".to_string())
            .await
            .expect("no results from API call");
        let taxanswer: ITRTaxAnswer = serde_json::from_str(&results)?;
        Ok(taxanswer.value)
    }

    pub async fn get_categories(&mut self) -> Result<Vec<Category>> {
        let mut hdrs = reqwest::header::HeaderMap::new();
        hdrs.insert(
            reqwest::header::HeaderName::from_static("storeids"),
            reqwest::header::HeaderValue::from_str(&self.store_id)?,
        );
        let results = self
            .call::<Empty>(
                reqwest::Method::GET,
                &"/api/ProductLookupCategoriesData/GetOne/".to_string(),
                Some(hdrs),
                None,
            )
            .await
            .expect("no results from API call");
        let cats: Vec<Category> = serde_json::from_str(&results)?;
        Ok(cats)
    }

    pub async fn make_customer(&mut self, c: &MinimalCustomer) -> Result<String> {
        self.post_json(&"/api/CustomersData/Post".to_string(), c).await
    }

    pub async fn update_customer(&mut self, c: &Customer) -> Result<String> {
        self.put_json(&"/api/CustomersData/Put".to_string(), c).await
    }

    pub async fn get_transactions_details(&mut self, start_o: Option<&DateTime<Local>>, end_o: Option<&DateTime<Local>>) -> Result<Vec<EJTxn>> {
        let end_default = Local::now();
        let end = end_o.unwrap_or(&end_default);
        let start_default = end.checked_sub_days(Days::new(1)).unwrap();
        let start = start_o.unwrap_or(&start_default);
        // This returns a productId that is a uuid.  Nowhere else in the APIs can I find a uuid attached to
        // rows of the products, so we don't have a mapping from productid <-> upc
        let url = format!("/api/ElectronicJournalData/Get?\
            $expand=TransactionTenders($select+%3D+TenderCode,LastCardDigits),TransactionProducts($select+%3D+%2A),TransactionProducts($expand=ProductChange($select+%3D+upc))&\
            $filter=(TransactionDate+ge+{}+and++TransactionDate+lt+{})+and+(Total+ne+null)&\
            $orderby=TransactionDate&$select=Id,EmployeeId,TransactionDate,Total,Canceled,CustomerId,CustomerFirstName,CustomerLastName",
            start.to_rfc3339_opts(SecondsFormat::Secs, true),
            end.to_rfc3339_opts(SecondsFormat::Secs, true));
        let r = self.get(&url)
            .await
            .expect("Electronic Journal Output");
        let answer: EJTAnswer = serde_json::from_str(&r)?;
        Ok(answer.value)
    }
}
