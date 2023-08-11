use anyhow::{anyhow, Result};
use clap::ArgMatches;
use reqwest::{self, Method};
use std::env;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use regex::Regex;
use serde_json::json;
use log::*;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Tag {
    pub id: u32,
    pub name: String,
}
#[derive(Deserialize, Debug, Clone)]
pub struct Member {
    pub id: String,
    pub email_address: String,
    pub unique_email_id: String,
    pub contact_id: String,
    pub full_name: String,
    pub email_type: String,
    pub status: String,
    pub unsubscribe_reason: Option<String>,
    pub merge_fields: serde_json::Map<String,serde_json::Value>,
    pub interests: serde_json::Map<String,serde_json::Value>,
    pub source: String,
    pub tags: Vec<Tag>,
}
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UpdateMember {
    pub full_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merge_fields: Option<serde_json::Map<String,serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interests: Option<serde_json::Map<String,serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<Tag>>,
}

#[derive(Deserialize, Debug)]
pub struct Members {
    pub members: Vec<Member>
}
#[derive(Deserialize, Debug, Clone)]
pub struct MCList {
    pub id: String,
    pub name: String,
}
#[derive(Deserialize, Debug)]
pub struct MCLists {
    pub lists: Vec<MCList>
}

#[derive(Serialize, Debug)]
pub struct NewMember {
    pub email_address: String,
    pub status: String,
    pub email_type: String,
    pub merge_fields: serde_json::Map<String,serde_json::Value>,
}

pub fn quick_new_member(email: &String, first_name: &String, last_name: &String, phone: &String, discount: &u8) -> NewMember {
    let mut merge_fields = serde_json::Map::new();
    merge_fields.insert("FNAME".to_owned(), json!(first_name));
    merge_fields.insert("LNAME".to_owned(), json!(last_name));
    merge_fields.insert("PHONE".to_owned(), json!(phone));
    merge_fields.insert("ITDISCOUNT".to_owned(), json!(discount));
    NewMember {
        email_address: email.to_string(),
        status: "pending".to_owned(),
        email_type: "html".to_owned(),
        merge_fields: merge_fields
    }
}

pub fn normalize_phone(phone: &String) -> String {
    let re = Regex::new("[^0-9]+").unwrap();
    let mut shorter = re.replace_all(phone, "").to_string();
    if shorter.starts_with("1") {
        shorter = shorter[1..].to_string();
    }
    if shorter.len() > 10 {
        shorter = shorter[0..9].to_string();
    }
    shorter
}

pub struct MCApi {
    dc: String,
    api_token: String,
}

pub fn mailchimp_api_new(settings: &super::settings::Settings, token: Option<&String>) -> MCApi {
    MCApi{
        dc: env::var("MAILCHIMP_DC").unwrap_or(settings.mailchimp.dc.to_string()),
        api_token: match token {
            Some(string) => string.to_string(),
            None => {
                match env::var("MAILCHIMP_TOKEN") {
                    Ok(tok) => tok,
                    Err(_) => {
                        error!("No Mailchimp API token, this will not work well.");
                        "".to_string()
                    }
                }
            }
        }
    }
}

impl MCApi {
    pub fn get_list(&mut self, listid: Option<&String>) -> Result<MCList> {
        let lists_get = self.get("lists");
        if lists_get.is_err() {
            return Err(anyhow!("Failed to get mailchip lists {}", lists_get.err().unwrap()))

        }
        let lists_result = serde_json::from_str::<MCLists>(&lists_get.unwrap());
        if lists_result.is_err() {
            return Err(anyhow!("Failed to get mailchip lists {}", lists_result.err().unwrap()))
        }
        let lists = lists_result.unwrap();
        let mc_list =
            if lists.lists.len() != 1 {
                let tgt_list = listid.unwrap();
                let mut found = lists.lists.into_iter().filter(|x| { x.id.eq(tgt_list) });
                found.next()
            } else {
                match listid {
                    Some(id) => {
                        if lists.lists[0].id.eq(id) {
                            lists.lists.into_iter().next()
                        } else {
                            None
                        }
                    },
                    None => lists.lists.into_iter().next()
                }
            };
        match mc_list {
            Some(result) => { Ok(result) },
            None => Err(anyhow!("No such list"))
        }
    }

    pub fn get_subscribers(&mut self, listid: &String) -> Result<HashMap<String,Member>> {
        let mut set = HashMap::new();
        let batch_size = 500;
        let mut start = 0;
        loop {
            let url = format!("lists/{}/members?count={}&offset={}", listid, batch_size, start);
            let subs = serde_json::from_str::<Members>(&self.get(&url)?)?.members.into_iter();
            let mut count = 0;
            for sub in subs {
                set.insert(sub.email_address.to_lowercase(), sub);
                count = count + 1;
            }
            if count == 0 {
                break;
            }
            start = start + batch_size;
        }
        Ok(set)
    }
    
    pub fn get(&mut self, url: &str) -> Result<String> {
        let client = reqwest::blocking::Client::new();
        let result = client
            .get(format!("https://{}.api.mailchimp.com/3.0/{}", self.dc, url))
            .basic_auth("anything", Some(&self.api_token))
            .send()?;
        let text_response = result.text()?;
        Ok(text_response)
    }

    pub fn build_req<T: Serialize + ?Sized>(&mut self, method: Method, url: &str, json: &T) -> reqwest::blocking::RequestBuilder {
        let client = reqwest::blocking::Client::new();
        let url = format!("https://{}.api.mailchimp.com/3.0/{}", self.dc, url);
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::CONTENT_TYPE, reqwest::header::HeaderValue::from_static("application/json"));
        let builder = client
            .request(method, url)
            .basic_auth("anything", Some(&self.api_token))
            .headers(headers)
            .json(json);
        builder
    }
    pub fn do_json<T: Serialize + ?Sized>(&mut self, method: Method, url: &str, json: &T) -> Result<String> {
        let builder = self.build_req(method, url, json);
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
    pub fn post_json<T: Serialize + ?Sized>(&mut self, url: &str, json: &T) -> Result<String> {
        self.do_json(Method::POST, url, json)
    }

    pub fn update_member(&mut self, list_id: &String, member: &Member, customer: &super::api::Customer) -> Result<String> {
        let mut merge_fields = serde_json::Map::new();
        if customer.first_name.len() > 0 {
            merge_fields.insert("FNAME".to_owned(), json!(customer.first_name));
        }
        if customer.last_name.len() > 0 {
            merge_fields.insert("LNAME".to_owned(), json!(customer.last_name));
        }
        if let Some(phone) = customer.phone.as_ref() {
            if phone.len() > 0 {
                merge_fields.insert("PHONE".to_owned(), json!(customer.phone.as_ref().unwrap()));
            }
        }
        merge_fields.insert("ITDISCOUNT".to_owned(), json!(customer.discount.unwrap_or(0)));
        let interests = serde_json::Map::new();
        let tags: Vec<Tag> = vec![];
        let um = UpdateMember {
            full_name: format!("{} {}", customer.first_name, customer.last_name),
            merge_fields: Some(merge_fields),
            interests: if interests.len() > 0 { Some(interests) } else { None },
            tags: if tags.len() > 0 { Some(tags) } else { None }
        };
        let url = format!("/lists/{}/members/{}", list_id, member.id);
        self.do_json(Method::PATCH, &url, &um)
    }
}

pub fn mailchimp_sync(api: &mut super::api::ITRApi, settings: &super::settings::Settings, args: &ArgMatches) -> Result<()> {
    let mut itr_customers = HashMap::new();
    let itc_vec: Vec<super::api::Customer> = api.get_customers()?;
    for customer in itc_vec {
        if customer.email.is_some() {
            let email = customer.email.as_ref().unwrap().to_lowercase();
            if itr_customers.contains_key(&email) {
                warn!("IT Retail duplicate: {}", email);
            }
            itr_customers.insert(email, customer);
        }
    }
    debug!("Pulled {} IT Retail customers.", itr_customers.len());
    let mc_token = match args.get_one::<String>("mc_token") {
        Some(tok) => Some(tok),
        None => if settings.mailchimp.token.len() > 0 {
            Some(&settings.mailchimp.token)
        } else {
            None
        }
    };
    let mut mc_api = mailchimp_api_new(&settings, mc_token);
    let list = mc_api.get_list(args.get_one::<String>("listid"))?;
    let subscribers: HashMap<String, Member> = mc_api.get_subscribers(&list.id)?;

    debug!("Pulled {} mailchimp subscribers.", subscribers.len());
    let to_mc: Vec<&String> = itr_customers.keys().filter(|s| !subscribers.contains_key(*s)).collect();
    let to_itr: Vec<&String> = subscribers.keys().filter(|s| !itr_customers.contains_key(*s)).collect();

    let mut errors = 0;
    let mut added_to_itr = 0;
    let mut added_to_mc = 0;

    for mc_c in to_itr.iter() {
        let nc = subscribers.get(*mc_c).unwrap();
        let min_itr = super::api::MinimalCustomer {
            first_name: nc.merge_fields.get("FNAME").unwrap().as_str().unwrap().to_string(),
            last_name: nc.merge_fields.get("LNAME").unwrap().as_str().unwrap().to_string(),
            email: nc.email_address.to_string(),
            phone: normalize_phone(&nc.merge_fields.get("PHONE").unwrap().as_str().unwrap().to_string()),
            frequent_shopper: true
        };
        match api.make_customer(&min_itr) {
            Ok(_) => { added_to_itr = added_to_itr + 1; }
            Err(e) => {
                warn!("failed adding to ITRetail: {} for {:?}", e, &min_itr);
                errors = errors + 1;
            }
        }
    }    
    info!("Added {} records to IT Retail.", added_to_itr);
    for itr_c in to_mc.iter() {
        let c = itr_customers.get(*itr_c).unwrap();
        let c_phone = match &c.phone {
            Some(phone) => phone.to_string(),
            _ => "".to_owned()
        };
        let new_member = quick_new_member(&c.email.as_ref().unwrap().to_string(), &c.first_name, &c.last_name, &c_phone, &c.discount.unwrap_or(0));
        match mc_api.post_json(&format!("/lists/{}/members", &list.id), &new_member) {
            Ok(_) => { added_to_mc = added_to_mc + 1; }
            Err(e) => {
                warn!("failed adding to mailchimp: {} for {:?}", e, &new_member);
                errors += 1;
            }
        }
    }
    info!("Added {} records to Mailchimp.", added_to_mc);

    let mut updated_mc = 0;
    let mut updated_itr = 0;
    for (mc_key, mc_c) in subscribers.iter() {
        if let Some((_, itr_c)) = itr_customers.get_key_value(mc_key) {
            if mc_key.ne("jesus@lethargy.org") {
                continue;
            }
            println!("{:?}\nvs\n{:?}", mc_c, itr_c);
            let mc_first_name = mc_c.merge_fields.get("FNAME").unwrap().as_str().unwrap().to_string();
            let mc_last_name = mc_c.merge_fields.get("LNAME").unwrap().as_str().unwrap().to_string();
            let mc_phone = mc_c.merge_fields.get("PHONE").unwrap().as_str().unwrap().to_string();
            let c_phone = match &itr_c.phone {
                Some(phone) => phone.to_string(),
                _ => "".to_owned()
            };
            let mc_discount = mc_c.merge_fields.get("ITDISCOUNT").unwrap_or(&json!(0)).as_u64().unwrap_or(0) as u8;
            let c_discount = itr_c.discount.unwrap_or(0);
            if mc_first_name.ne(&itr_c.first_name) ||
               mc_last_name.ne(&itr_c.last_name) ||
               mc_phone.ne(&c_phone) ||
               mc_discount != c_discount {
                trace!("{} records differ ({:?} : {:?}).", mc_key, mc_c, itr_c);
                let r = mc_api.update_member(&list.id, mc_c, itr_c);
                if r.is_err() {
                    warn!("Failure to update {} in mailchimp: {}", mc_key, r.err().unwrap());
                    errors += 1;
                }
                else {
                    updated_mc += 1;
                }
                // We really only ever update a phone number from MC
                if mc_phone.len() > 0 && itr_c.phone.is_none() || itr_c.phone.as_ref().unwrap().len() == 0 {
                    let mut newc = api.get_customer(&itr_c.id)?;
                    newc.phone = Some(normalize_phone(&mc_phone));
                    let r = api.update_customer(&newc);
                    if r.is_err() {
                        warn!("Failure to update {} in IT Retail: {}", mc_key, r.err().unwrap());
                        errors += 1;
                    } else {
                        updated_itr += 1;
                    }
                }
            }
        }
    }
    info!("Updated {} records in Mailchimp and {} records in IT Retail.", updated_mc, updated_itr);

    if errors > 0 {
        return Err(anyhow!("There where {} syncing errors", errors))
    }
    Ok(())
}