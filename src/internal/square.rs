use anyhow::Result;
use log::*;
use std::collections::HashMap;
use std::time::Duration;
use squareup::{config::{BaseUri, Configuration}, models::ListCustomersParameters, SquareClient};
use squareup::http::{Headers, client::{HttpClientConfiguration, RetryConfiguration}};
use squareup::api::{CustomersApi};
use uuid::Uuid;
use squareup::models::{Customer};

const MD_LOYALTY_POINTS: &str = "loyalty-points";
const MD_LOYALTY_DISCOUNT: &str = "loyalty-discount";

pub struct SquareSyncResult {
    pub added_up: u64,
    pub added_down: u64,
    pub updated_up: u64,
}

pub struct SquareConnect {
    client: SquareClient,
}

pub fn square_connect_create(settings: &super::settings::Settings) -> SquareConnect {
    let (env, auth) = match settings.square.environment {
        super::settings::SquareEnvironment::Production => {
            (squareup::config::Environment::Production,
             settings.square.production_secret.to_string())
        },
        super::settings::SquareEnvironment::Sandbox => {
            (squareup::config::Environment::Sandbox,
             settings.square.sandbox_secret.to_string())
        }
    };
    let headers = Headers::new(None, None, None, None, Some(auth));
    let config = Configuration {
        environment: env,
        http_client_config: HttpClientConfiguration {
            timeout: 30,
            user_agent: String::from("itretail_automation"), // will override what's in headers
            default_headers: headers,
            retry_configuration: RetryConfiguration {
                retries_count: settings.square.max_retries,
                min_retry_interval: Duration::from_secs(1),
                max_retry_interval: Duration::from_secs(30 * 60),
                base: 3,
            },
        },
        base_uri: BaseUri::default(),
    };
    SquareConnect {
        client: SquareClient::try_new(config).unwrap()
    }
}
fn essentially_different(sc: &Customer, dc: &super::api::Customer) -> bool {
    match &sc.given_name {
        Some(a) => if a != &dc.first_name { return true; },
        None => {}
    }
    match &sc.family_name {
        Some(a) => if a != &dc.last_name { return true; },
        None => {}
    }
    match (&sc.email_address, &dc.email) {
        (Some(a), Some(b)) => if a != b { return true; },
        (Some(_), None) => { return true; },
        (None, Some(_)) => { return true; },
        (None, None) => {}
    }
    match (&sc.phone_number, &dc.phone) {
        (Some(a), Some(b)) => if a != b { return true; },
        (Some(_), None) => { return true; },
        (None, Some(_)) => { return true; },
        (None, None) => {}
    }
    match &sc.reference_id {
        Some(a) => if a != &dc.id.to_string() { return true; },
        None => { return true; }
    }
    false
}

impl SquareConnect {
    pub async fn get_customers(&self) -> Result<Vec<Customer>> {
        let customers_api: CustomersApi = CustomersApi::new(self.client.clone());
        let mut cursor: String = String::from("");
        let mut customers: Vec<Customer> = vec![];
        loop {
            let res = customers_api.list_customers(&ListCustomersParameters {
                cursor: cursor,
                count: Some(true),
                ..Default::default()
            }).await?;
            if let Some(page) = res.customers {
                for c in page {
                    customers.push(c);
                }
            }
            if res.cursor.is_none() { break; }
            cursor = res.cursor.unwrap();
        }
        Ok(customers)
    }
    pub async fn add_customer(&self, c: &super::api::Customer) -> Result<Customer> {
        let customers_api: CustomersApi = CustomersApi::new(self.client.clone());
        let customer = squareup::models::CreateCustomerRequest {
            given_name: Some(c.first_name.to_string()),
            family_name: Some(c.last_name.to_string()),
            email_address: match &c.email {
                Some(email) => Some(email.to_string()),
                None => None
            },
            phone_number: match &c.phone {
                Some(phone) => Some(phone.to_string()),
                None => None
            },
            reference_id: Some(c.id.to_string()),
            ..Default::default()
        };
        let res = customers_api.create_customer(&customer).await?;
        Ok(res.customer)
    }

    pub async fn update_customer(&self, sc: &Customer, c: &super::api::Customer, force: bool) -> Result<bool> {
        if essentially_different(sc, c) || force {
            debug!("customer needs update.");
            let customers_api: CustomersApi = CustomersApi::new(self.client.clone());
            let customer = squareup::models::UpdateCustomerRequest {
                given_name: Some(c.first_name.to_string()),
                family_name: Some(c.last_name.to_string()),
                email_address: match &c.email {
                    Some(email) => Some(email.to_string()),
                    None => None
                },
                phone_number: match &c.phone {
                    Some(phone) => Some(phone.to_string()),
                    None => None
                },
                reference_id: Some(c.id.to_string()),
                ..Default::default()
            };
            let res = customers_api.update_customer(&sc.id.as_ref().unwrap(), &customer).await?;
            error!("{:#?}", res);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn sync_customers_with_sidedb(&self, sidedb: &mut super::sidedb::SideDb) -> Result<SquareSyncResult> {
        let dbcusts = sidedb.get_customers().await?;
        let square_custs = self.get_customers().await?;
        let mut square_custs_by_itrid = HashMap::<Uuid, &Customer>::new();
        let mut square_custs_by_email = HashMap::<&String, &Customer>::new();
        let mut square_custs_by_phone = HashMap::<&String, &Customer>::new();
        for sc in &square_custs {
            if let Some(uuid_str) = &sc.reference_id {
                if let Ok(uuid) = Uuid::parse_str(uuid_str) {
                     square_custs_by_itrid.insert(uuid, sc);
                }
            }
            if let Some(email) = &sc.email_address {
                square_custs_by_email.insert(email, sc);
            }
            if let Some(phone) = &sc.phone_number {
                square_custs_by_phone.insert(phone, sc);
            }
        }
        println!("{:#?}", square_custs);
        let mut added_up: u64 = 0;
        let mut updated_up: u64 = 0;
        for dbc in &dbcusts {
            debug!("{:#?}", dbc);
            if dbc.email.is_none() ||
               (dbc.email.as_ref().unwrap() != "jesus@lethargy.org") {
               continue;
            }
            debug!("Checking for square customer: {:?}/{:?}", dbc.email, dbc.phone);
            let t_email = match &dbc.email {
                Some(e) => e.to_string(),
                None => " nope ".to_string()
            };
            let t_phone = match &dbc.phone {
                Some(p) => p.to_string(),
                None => " nope ".to_string()
            };
            if let Some(sc) = square_custs_by_itrid.get(&dbc.id) {
                debug!("found associated customer {:?} : {}", sc.id, dbc.id);
                match self.update_customer(sc, &dbc, false).await {
                    Ok(true) => {
                        debug!("updated customer");
                        updated_up += 1;
                    }
                    Ok(false) => {
                        debug!("no update needed");
                    }
                    Err(e) => {
                        error!("Failed to update customer: {:?}", e);
                    }
                }
            } else if let Some(sc) = square_custs_by_email.get(&t_email) {
                debug!("found customer by email {:?} : {}", sc.id, dbc.id);
                match sidedb.associate_customer_with_square(&dbc.id, &sc.id.as_ref().unwrap().to_string()).await {
                    Ok(true) => {
                        match self.update_customer(sc, &dbc, false).await {
                            Ok(true) => {
                                debug!("updated customer");
                                updated_up += 1;
                            }
                            Ok(false) => {
                                debug!("no update needed");
                            }
                            Err(e) => {
                                error!("failed to update customer: {:?}", e);
                            }
                        }
                    },
                    Ok(false) => { error!("could not find record association for {:?}", sc.email_address); }
                    Err(e) => { error!("could build association for {:?} {:?}", sc.email_address, e); }
                }
            } else if let Some(sc) = square_custs_by_phone.get(&t_phone) {
                debug!("found customer by phone {:?} : {}", sc.id, dbc.id);
                match sidedb.associate_customer_with_square(&dbc.id, &sc.id.as_ref().unwrap().to_string()).await {
                    Ok(true) => {
                        match self.update_customer(sc, &dbc, false).await {
                            Ok(true) => {
                                debug!("updated customer");
                                updated_up += 1;
                            }
                            Ok(false) => {
                                debug!("no update needed");
                            }
                            Err(e) => {
                                error!("failed to update customer: {:?}", e);
                            }
                        }
                    },
                    Ok(false) => { error!("could not find record association for {:?}", sc.phone_number); }
                    Err(e) => { error!("could build association for {:?} {:?}", sc.phone_number, e); }
                }
            } else {
                debug!("Creating new customer {:?}", dbc.phone);
                match self.add_customer(&dbc).await {
                    Ok(newc) => {
                        added_up += 1;
                        match sidedb.associate_customer_with_square(&dbc.id, &newc.id.unwrap().to_string()).await {
                            Ok(false) => { error!("could not find record association for {:?}", newc.email_address); },
                            Err(e) => { error!("could build association for {:?} {:?}", newc.email_address, e); },
                            Ok(true) => {}
                        }
                    },
                    Err(e) => { error!("could build association for {:?} {:?}", dbc.email, e); }
                }
            }
        }
        Ok(SquareSyncResult { added_up: added_up, added_down: 0, updated_up: updated_up})
    }

    pub async fn sync_products_with_sidedb(&self, sidedb: &mut super::sidedb::SideDb) -> Result<SquareSyncResult> {
        let mut added_up: u64 = 0;
        let mut added_down: u64 = 0;
        let mut updated_up: u64 = 0;

        Ok(SquareSyncResult { added_up: added_up, added_down: added_down, updated_up: updated_up})
    }
}
