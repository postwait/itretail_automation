use anyhow::{Error, Result};
use log::*;
use std::collections::HashMap;
use stripe::{Client, CreateCustomer, UpdateCustomer, Customer, ListCustomers};
use uuid::Uuid;



const MD_ITR_CUSTOMER: &str = "itr-customer";
const MD_LOYALTY_POINTS: &str = "loyalty-points";
const MD_LOYALTY_DISCOUNT: &str = "loyalty-discount";

pub struct StripeSyncResult {
    pub added_up: u64,
    pub added_down: u64,
    pub updated_up: u64,
}

pub struct StripeConnect {
    client: Client,
}

pub fn stripe_connect_create(_settings: &super::settings::Settings) -> StripeConnect {
    StripeConnect {
        client: Client::new(_settings.stripe.secret.to_string()),
    }
}
fn essentially_different(sc: &stripe::Customer, dc: &super::api::Customer) -> bool {
     match &sc.name {
       Some(name) => if name != &format!("{} {}", dc.first_name, dc.last_name) { return true; },
       None => {}
    }
    match (&sc.email, &dc.email) {
        (Some(a), Some(b)) => if a != b { return true; },
        (Some(_), None) => { return true; },
        (None, Some(_)) => { return true; },
        (None, None) => {}
    }
    match (&sc.phone, &dc.phone) {
        (Some(a), Some(b)) => if a != b { return true; },
        (Some(_), None) => { return true; },
        (None, Some(_)) => { return true; },
        (None, None) => {}
    }
    match &sc.metadata {
        Some(md) => {
            if let (Some(id), Some(points), Some(discount)) = (md.get(MD_ITR_CUSTOMER), md.get(MD_LOYALTY_POINTS), md.get(MD_LOYALTY_DISCOUNT)) {
                if id != &String::from(dc.id) ||
                   points != &String::from(dc.loyalty_points.unwrap_or(0).to_string()) ||
                   discount != &String::from(dc.discount.unwrap_or(0).to_string()) {
                    return true;
                }
            }
        }
        None => {}
    }
    false
}
impl StripeConnect {
       
    pub fn get_customers(&self) -> Result<Vec<Customer>> {
        let params = ListCustomers { ..Default::default() };
        let paginator = Customer::list(&self.client, &params).unwrap().paginate(params);
        match paginator.get_all(&self.client) {
            Ok(r) => Ok(r),
            Err(e) => Err(Error::from(e))
        }
    }

    pub fn add_customer(&self, c: &super::api::Customer) -> Result<Customer> {
        let customer = Customer::create(
            &self.client,
            CreateCustomer {
                name: Some(format!("{} {}", c.first_name, c.last_name).as_str()),
                email: match &c.email {
                    Some(email) => Some(email.as_str()),
                    None => None
                },
                phone: match &c.phone {
                    Some(phone) => Some(phone.as_str()),
                    None => None
                },
                tax_exempt: Some(stripe::CustomerTaxExemptFilter::None),
                metadata: Some(std::collections::HashMap::from([
                    (String::from(MD_ITR_CUSTOMER), String::from(c.id)),
                    (String::from(MD_LOYALTY_POINTS), String::from(c.loyalty_points.unwrap_or(0).to_string())),
                    (String::from(MD_LOYALTY_DISCOUNT), String::from(c.discount.unwrap_or(0).to_string())),
                    ])
                ),
               ..Default::default()
            },
        ).unwrap();
        Ok(customer)
    }

    pub fn update_customer(&self, sc: &stripe::Customer, dc: &super::api::Customer, force: bool) -> Result<bool> {
        if essentially_different(sc, dc) || force {
            Customer::update(
                &self.client,
                &sc.id,
                UpdateCustomer {
                    name: Some(format!("{} {}", dc.first_name, dc.last_name).as_str()),
                    email: match &dc.email {
                        Some(email) => Some(email.as_str()),
                        None => None
                    },
                    phone: match &dc.phone {
                        Some(phone) => Some(phone.as_str()),
                        None => None
                    },
                    tax_exempt: Some(stripe::CustomerTaxExemptFilter::None),
                    metadata: Some(std::collections::HashMap::from([
                        (String::from(MD_ITR_CUSTOMER), String::from(dc.id)),
                        (String::from(MD_LOYALTY_POINTS), String::from(dc.loyalty_points.unwrap_or(0).to_string())),
                        (String::from(MD_LOYALTY_DISCOUNT), String::from(dc.discount.unwrap_or(0).to_string())),
                        ])
                    ),
                   ..Default::default()
                },
            )?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn sync_with_sidedb(&self, sidedb: &mut super::sidedb::SideDb) -> Result<StripeSyncResult> {
        let dbcusts = sidedb.get_customers()?;
        let stripe_custs = self.get_customers()?;
        let mut stripe_custs_by_itrid = HashMap::<Uuid, &stripe::Customer>::new();
        let mut stripe_custs_by_email = HashMap::<&String, &stripe::Customer>::new();
        let mut stripe_custs_by_phone = HashMap::<&String, &stripe::Customer>::new();
        for sc in &stripe_custs {
            if let Some(md) = &sc.metadata {
                if let Some(uuid_str) = md.get(MD_ITR_CUSTOMER) {
                    if let Ok(uuid) = Uuid::parse_str(uuid_str) {
                        stripe_custs_by_itrid.insert(uuid, sc);
                    }
                }
            }
            if let Some(email) = &sc.email {
                stripe_custs_by_email.insert(email, sc);
            }
            if let Some(phone) = &sc.phone {
                stripe_custs_by_phone.insert(phone, sc);
            }
        }
        println!("{:#?}", stripe_custs);
        let mut added_up: u64 = 0;
        let mut updated_up: u64 = 0;
        for dbc in &dbcusts {
            debug!("Checking for stripe customer: {:?}/{:?}", dbc.email, dbc.phone);
            let t_email = match &dbc.email {
                Some(e) => e.to_string(),
                None => " nope ".to_string()
            };
            let t_phone = match &dbc.phone {
                Some(p) => p.to_string(),
                None => " nope ".to_string()
            };
            if let Some(sc) = stripe_custs_by_itrid.get(&dbc.id) {
                debug!("found associated customer {} : {}", sc.id, dbc.id);
                match self.update_customer(sc, &dbc, false) {
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
            } else if let Some(sc) = stripe_custs_by_email.get(&t_email) {
                debug!("found customer by email {} : {}", sc.id, dbc.id);
                match sidedb.associate_customer_with_stripe(&dbc.id, &sc.id.to_string()) {
                    Ok(true) => {
                        match self.update_customer(sc, &dbc, false) {
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
                    Ok(false) => { error!("could not find record association for {:?}", sc.email); }
                    Err(e) => { error!("could build association for {:?} {:?}", sc.email, e); }
                }
            } else if let Some(sc) = stripe_custs_by_phone.get(&t_phone) {
                debug!("found customer by phone {} : {}", sc.id, dbc.id);
                match sidedb.associate_customer_with_stripe(&dbc.id, &sc.id.to_string()) {
                    Ok(true) => {
                        match self.update_customer(sc, &dbc, false) {
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
                    Ok(false) => { error!("could not find record association for {:?}", sc.email); }
                    Err(e) => { error!("could build association for {:?} {:?}", sc.email, e); }
                }
            } else {
                debug!("Creating new customer {:?}", dbc.phone);
                match self.add_customer(&dbc) {
                    Ok(newc) => {
                        added_up += 1;
                        match sidedb.associate_customer_with_stripe(&dbc.id, &newc.id.to_string()) {
                            Ok(false) => { error!("could not find record association for {:?}", newc.email); },
                            Err(e) => { error!("could build association for {:?} {:?}", newc.email, e); },
                            Ok(true) => {}
                        }
                    },
                    Err(e) => { error!("could build association for {:?} {:?}", dbc.email, e); }
                }
            }
        }
        Ok(StripeSyncResult { added_up: added_up, added_down: 0, updated_up: updated_up})
    }
}
