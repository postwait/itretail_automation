use anyhow::{anyhow, Result};
use fancy_regex::Regex;
use log::*;
use std::{collections::HashMap, fmt::Debug};
use std::time::Duration;
use squareup::{api::LocationsApi,
              config::{BaseUri, Configuration},
              models::{enums::{CatalogItemProductType, CatalogObjectType, CatalogPricingType, Currency, InventoryChangeType, InventoryState, MeasurementUnitUnitType, MeasurementUnitWeight}, BatchChangeInventoryRequest, CatalogItem, CatalogItemVariation, CatalogMeasurementUnit, CatalogObject, DateTime, InventoryChange, InventoryPhysicalCount, ItemVariationLocationOverrides, ListCatalogParameters, ListCustomersParameters, Location, MeasurementUnit, Money, UpsertCatalogObjectRequest},
              SquareClient};
use squareup::http::{Headers, client::{HttpClientConfiguration, RetryConfiguration}};
use squareup::api::{CatalogApi, CustomerGroupsApi, CustomersApi, InventoryApi};
use uuid::Uuid;
use squareup::models::{CreateCustomerGroupRequest, Customer, CustomerGroup, ListCustomerGroupsParameters};

use super::api::ProductData;

//const MD_LOYALTY_POINTS: &str = "loyalty-points";
//const MD_LOYALTY_DISCOUNT: &str = "loyalty-discount";

pub enum TaxLocation<'a> {
    #[allow(dead_code)]
    State(String),
    Location(&'a Location),
}
#[derive(Debug)]
#[allow(dead_code)]
pub struct SquareSyncResult {
    pub added_up: u64,
    pub set_inv_up: u64,
    pub added_down: u64,
    pub updated_up: u64,
    pub deleted_up: u64,
}

#[allow(dead_code)]
pub struct SquareConnect {
    client: SquareClient,
    appid: String,
    location: String,
    state: Option<String>,
    weight_unit: MeasurementUnitWeight,
    weight_precision: i32,
}

struct MetaBuilder {
    tax_id: String,
    location_id: String,
    measurement_id: String
}
impl<'a> MetaBuilder {
    pub fn build(&self, product: &'a ProductData) -> ProductDataWithMetadata<'a> {
        ProductDataWithMetadata {
            product: product,
            tax_id: self.tax_id.clone(),
            location_id: self.location_id.clone(),
            measurement_id: self.measurement_id.clone(),
        }
    }
}
struct ProductDataWithMetadata<'a> {
    product: &'a ProductData,
    tax_id: String,
    location_id: String,
    measurement_id: String,
}

fn square_phone(maybe_trash: &Option<String>) -> Option<String> {
    if let Some(trash) = maybe_trash {
        let dig = super::customer::normalize_phone(trash);
        if dig.len() != 10 {
            None
        } else {
            Some(format!("({}) {}-{}", &dig[0..3], &dig[3..6], &dig[6..]))
        }
    } else {
        None
    }
}
fn catalogobject_getsku(co: &CatalogObject) -> Result<String> {
    if let Some(a) = &co.item_data {
        if let Some(b) = &a.variations {
            if b.len() > 0 {
                if let Some(c) = &b[0].item_variation_data {
                    if let Some(sku) = &c.sku {
                        return Ok(sku.clone())
                    }
                }
            }
        }
    }
    Err(anyhow!("no sku in CatalogItem"))
}

impl<'a> From<ProductDataWithMetadata<'a>> for CatalogObject {
    fn from(pwl: ProductDataWithMetadata) -> Self {
        let p = pwl.product;
        let tax_ids = match p.taxclass.0 {
            Some(_taxid) => Some(vec![pwl.tax_id.clone()]),
            None => None
        };
        let name = (&p.description).to_string();
        CatalogObject {
            r#type: CatalogObjectType::Item,
            id: format!("#{}", p.upc),
            is_deleted: Some(p.deleted),
            present_at_all_locations: Some(true),
            item_data: Some(CatalogItem {
                name: Some(name.to_string()),
                is_taxable: Some(true), // tax_ids controls this
                tax_ids: tax_ids,
                available_for_pickup: Some(true),
                skip_modifier_screen: Some(true),
                description_html: None,
                description_plaintext: None,
                product_type: Some(CatalogItemProductType::Regular),
                is_archived: Some(p.deleted),
                variations: Some(vec![
                    CatalogObject {
                        r#type: CatalogObjectType::ItemVariation,
                        id: format!("#{}-var1", p.upc),
                        is_deleted: Some(p.deleted),
                        present_at_all_locations: Some(true),
                        item_variation_data: Some(
                            CatalogItemVariation {
                                item_id: Some(format!("#{}", (&p.upc).to_string())),
                                name: Some("Regular".to_string()),
                                sku: p.upca(),
                                ordinal: Some(1),
                                pricing_type: Some(CatalogPricingType::FixedPricing),
                                price_money: Some(Money{
                                    amount: (p.get_price() * 100.0) as i32,
                                    currency: Currency::Usd,
                                }),
                                sellable: Some(true),
                                stockable: Some(true),
                                measurement_unit_id: if p.scale { Some(pwl.measurement_id) } else { None },
                                location_overrides: Some(vec![
                                    ItemVariationLocationOverrides{
                                        location_id: Some(pwl.location_id.clone()),
                                        track_inventory: Some(true),
                                        ..Default::default()
                                    }
                                ]),
                                ..Default::default()
                            }
                        ),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

fn get_variant_item_id(a: &CatalogObject) -> Option<String> {
    if a.r#type == CatalogObjectType::Item && a.item_data.is_some() {
        let a1 = a.item_data.as_ref().unwrap();
        if let Some(variations) = a1.variations.as_ref() {
            if variations.len() == 1 {
                return Some(variations[0].id.clone());
            }
        }
    }
    None
}

fn new_inventory_physical_count(variant_item_id: &String, oa: &DateTime, location: &String, qoh: f32) -> InventoryChange {
    InventoryChange {
        r#type: Some(InventoryChangeType::PhysicalCount),
        physical_count: Some(InventoryPhysicalCount {
            catalog_object_type: None,
            catalog_object_id: Some(variant_item_id.clone()),
            state: Some(InventoryState::InStock),
            quantity: Some(format!("{}", qoh)),
            location_id: Some(location.clone()),
            id: None,
            reference_id: None,
            source: None,
            employee_id: None,
            team_member_id: None,
            occurred_at: Some(oa.clone()),
            created_at: None,
        }),
        adjustment: None,
        transfer: None,
        measurement_unit: None,
        measurement_unit_id: None,
    }
}

fn catalogitem_needs_update(a: &CatalogObject, b: &CatalogObject) -> Result<Option<String>> {
    // verify our structure [Object[0] -> Item[1] -> Object[2] -> ItemVariation[3] -> ItemVariableLocationOverrides[4] ]
    // Object[1]
    if a.r#type != CatalogObjectType::Item || b.r#type != CatalogObjectType::Item { return Err(anyhow!("bad types (expected item)")); }
    if a.item_data.is_none() || b.item_data.is_none() { return Err(anyhow!("missing item_data")); }
    if a.is_deleted != b.is_deleted { return Ok(Some("is_deleted".to_owned())); }
    if a.present_at_all_locations != b.present_at_all_locations { return Ok(Some("present_at_all_locations".to_owned())); }
    if a.present_at_location_ids != b.present_at_location_ids { return Ok(Some("present_at_location_ids".to_owned())); }
    if a.absent_at_location_ids != b.absent_at_location_ids { return Ok(Some("present_at_location_ids".to_owned())); }
    // Item
    let (a1, b1) = (a.item_data.as_ref().unwrap(), b.item_data.as_ref().unwrap());
    if a1.name != b1.name { return Ok(Some("name".to_owned())); }
    if a1.is_taxable != b1.is_taxable { return Ok(Some("is_taxable".to_owned())); }
    if a1.tax_ids != b1.tax_ids { return Ok(Some("tax_ids".to_owned())); }
    if a1.available_for_pickup != b1.available_for_pickup { return Ok(Some("available_for_pickup".to_owned())); }
    if a1.skip_modifier_screen != b1.skip_modifier_screen { return Ok(Some("skip_modifier_screen".to_owned())); }
    if a1.description_plaintext != b1.description_plaintext { return Ok(Some("description_plaintext".to_owned())); }
    if a1.product_type != b1.product_type { return Ok(Some("product_type".to_owned())); }
    if a1.is_archived != b1.is_archived { return Ok(Some("is_archived".to_owned())); }
    // Object
    if a1.variations.is_none() || b1.variations.is_none() { return Err(anyhow!("missing variation")); }
    if a1.variations.as_ref().unwrap().len() != 1 || b1.variations.as_ref().unwrap().len() != 1 {
        return Err(anyhow!("implementation requires exactly one item variation."));
    }
    let (a2, b2) = 
        (&a1.variations.as_ref().unwrap()[0], &b1.variations.as_ref().unwrap()[0]);
    if a2.r#type != CatalogObjectType::ItemVariation || b2.r#type != CatalogObjectType::ItemVariation {
        return Err(anyhow!("bad types (expected itemvariation)"));
    }
    if a2.is_deleted != b2.is_deleted { return Ok(Some("variation.is_deleted".to_owned())); }
    if a2.present_at_all_locations != b2.present_at_all_locations { return Ok(Some("variation.present_at_all_locations".to_owned())); }
    // Variation
    if a2.item_variation_data.is_none() || b2.item_variation_data.is_none() {
        return Err(anyhow!("missing item_variation_data"));
    }
    let (a3, b3) =
        (a2.item_variation_data.as_ref().unwrap(), b2.item_variation_data.as_ref().unwrap());
    if a3.name != b3.name { return Ok(Some("variation.data.name".to_owned())); }
    if a3.sku != b3.sku { return Ok(Some("variation.data.sku".to_owned())); }
    // if a3.ordinal != b3.ordinal { return Ok(Some("variation.data.ordinal".to_owned())); }
    if a3.pricing_type != b3.pricing_type { return Ok(Some("variation.data.priciing_type".to_owned())); }
    if a3.price_money != b3.price_money { return Ok(Some("variation.data.price_money".to_owned())); }
    if a3.measurement_unit_id != b3.measurement_unit_id { return Ok(Some("variation.data.measurement_unit_id".to_owned())); }
    if a3.track_inventory != b3.track_inventory { return Ok(Some("variation.data.track_inventory".to_owned())); }
    if a3.sellable != b3.sellable { return Ok(Some("variation.data.sellable".to_owned())); }
    if a3.stockable != b3.stockable { return Ok(Some("variation.data.stockable".to_owned())); }
    // ItemVariableLocationOverrides
    if a3.location_overrides.is_none() || b3.location_overrides.is_none() { return Ok(Some("variation.data.location_overrides".to_owned())); }
    let (a4, b4) =
        (a3.location_overrides.as_ref().unwrap(), b3.location_overrides.as_ref().unwrap());
    if a4.len() != 1 || b4.len() != 1 { return Ok(Some("variation.data.location_overrides.len()".to_owned())); }
    if a4[0].track_inventory != b4[0].track_inventory { return Ok(Some("variation.data.location_overrides.track_inventory".to_owned())); }
    Ok(None)
}
fn catalogitem_adopt_ids(a: &mut CatalogObject, b: &CatalogObject) -> Result<()> {
    // This moves the id/item_id and versions into a from b.
    a.id = b.id.clone();
    a.version = b.version.clone();
    let a1 = a.item_data.as_mut().unwrap();
    let a2 = a1.variations.as_mut().unwrap();
    let b2 = &b.item_data.as_ref().unwrap().variations.as_ref().unwrap()[0];
    a2[0].id = b2.id.clone();
    a2[0].version = b2.version.clone();
    let a3 = a2[0].item_variation_data.as_mut().unwrap();
    a3.item_id = b2.item_variation_data.as_ref().unwrap().item_id.clone();
    Ok(())
}

pub fn square_connect_create(settings: &super::settings::Settings) -> SquareConnect {
    let (env, auth, appid) = match settings.square.environment {
        super::settings::SquareEnvironment::Production => {
            (squareup::config::Environment::Production,
             settings.square.production_secret.to_string(),
             settings.square.production_appid.to_string())
        },
        super::settings::SquareEnvironment::Sandbox => {
            (squareup::config::Environment::Sandbox,
             settings.square.sandbox_secret.to_string(),
             settings.square.sandbox_appid.to_string())
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
    let unit = match settings.square.weight_unit.to_lowercase().as_str() {
        "imperial_pound" => MeasurementUnitWeight::ImperialPound,
        _ => panic!("Invalid square weight_units in config: {}", settings.square.weight_unit)
    };
    SquareConnect {
        client: SquareClient::try_new(config).unwrap(),
        appid: appid,
        location: settings.square.location.to_string(),
        state: None,
        weight_unit: unit,
        weight_precision: settings.square.weight_precision,
    }
}

fn customer_needs_update(sc: &Customer, dc: &super::api::Customer) -> Option<String> {
    match &sc.given_name {
        Some(a) => if a != &dc.first_name { return Some("given_name".to_owned()); },
        None => {}
    }
    match &sc.family_name {
        Some(a) => if a != &dc.last_name { return Some("family_name".to_owned()); },
        None => {}
    }
    // The dc.email can be blank (not null), sigh.
    match (&sc.email_address, &dc.email) {
        (Some(a), Some(b)) => if a != b { return Some("email".to_owned()); },
        (Some(_), None) => { return Some("email".to_owned()); },
        (None, Some(a)) => { if a.trim() != "" { return Some("email".to_owned()); } },
        (None, None) => {}
    }
    match (&square_phone(&sc.phone_number), &square_phone(&dc.phone)) {
        (Some(a), Some(b)) => if a != b { return Some("phone".to_owned()); },
        (Some(_), None) => { return Some("phone".to_owned()); },
        (None, Some(_)) => { return Some("phone".to_owned()); },
        (None, None) => {}
    }
    match &sc.reference_id {
        Some(a) => if a != &dc.id.to_string() { return Some("reference_id".to_owned()); },
        None => { return Some("reference_id".to_owned()); }
    }
    None
}

impl SquareConnect {
    pub async fn get_customer_groups(&self, make: bool) -> Result<HashMap<u32,String>> {
        let groupapi = CustomerGroupsApi::new(self.client.clone());
        let mut cursor: Option<String> = None;
        let mut groups = HashMap::<u32,String>::new();
        let matcher = Regex::new(r"^Loyalty-Tier-(\d+)$").unwrap();
        loop {
            match groupapi.list_customer_groups(&ListCustomerGroupsParameters{cursor: cursor, limit: Some(50)}).await {
                Ok(r) => {
                    if let Some(groups_partial) = r.groups.as_ref() {
                        for group in groups_partial {
                            if let Ok(Some(mat)) = matcher.captures(group.name.as_ref()) {
                                if let Some(level_str) = mat.get(1) {
                                    let level = level_str.as_str().parse::<u32>()?;
                                    groups.insert(level, group.id.as_ref().unwrap().clone());
                                }
                            }

                        }
                    }
                    if let Some(new_cursor) = &r.cursor {
                        cursor = Some(new_cursor.clone());
                    } else {
                        break;
                    }
                },
                Err(e) => {
                    return Err(e.into())
                }
            }
        }
        if make {
            for expected in super::loyalty::valid_loyalty_levels() {
                if groups.get(&expected).is_none() {
                    match groupapi.create_customer_group(&CreateCustomerGroupRequest {
                        idempotency_key: Some(Uuid::new_v4().to_string()),
                        group: CustomerGroup {
                            name: format!("Loyalty-Tier-{}", expected),
                            ..Default::default()
                        }
                    }).await {
                        Ok(r) => {
                            if let Some(id) = &r.group.id {
                                groups.insert(expected, id.clone());
                            } else {
                                return Err(anyhow!("Group creation didn't result in id!"));
                            }
                        },
                        Err(e) => {
                            return Err(e.into());
                        }
                    }
                }
            }
        }
        Ok(groups)
    }
    async fn set_customer_loyalty(&self, capi: Option<&CustomersApi>, groups: &HashMap<u32, String>, cust: &&Customer, dbc: &super::api::Customer) -> Result<bool> {
        // There must be a better dance to make this live long enough
        let local_api = match capi {
            Some(_) => None,
            None => Some(CustomersApi::new(self.client.clone()))
        };
        let customers_api = capi.unwrap_or_else(|| { local_api.as_ref().unwrap() });
        // Fix the groups for cust
        let mut changed = false;
        let empty: Vec<String> = vec![];
        let existing_groups = cust.group_ids.as_ref().unwrap_or(&empty);
        for tier in super::loyalty::valid_loyalty_levels() {
            let want = (dbc.discount.unwrap_or(0) as u32) == tier;
            let subject = groups.get(&tier).expect(&format!("Customer Group Loyalty-Tier-{} is missing", tier));
            let mut seen = false;
            for existing in existing_groups {
                if existing == subject {
                    seen = true;
                    break;
                }
            }
            if seen && !want {
                customers_api.remove_group_from_customer(cust.id.as_ref().unwrap(), subject).await?;
                changed = true;
            } else if !seen && want {
                customers_api.add_group_to_customer(cust.id.as_ref().unwrap(), subject).await?;
                changed = true;
            };
        }
        Ok(changed)
    }

    pub async fn get_customers(&self, capi: Option<&CustomersApi>) -> Result<Vec<Customer>> {
        // There must be a better dance to make this live long enough
        let local_api = match capi {
            Some(_) => None,
            None => Some(CustomersApi::new(self.client.clone()))
        };
        let customers_api = capi.unwrap_or_else(|| { local_api.as_ref().unwrap() });
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

    pub async fn delete_customer(&self, capi: Option<&CustomersApi>, customers: Vec<String>) -> Result<u32> {
        // There must be a better dance to make this live long enough
        let local_api = match capi {
            Some(_) => None,
            None => Some(CustomersApi::new(self.client.clone()))
        };
        let customers_api = capi.unwrap_or_else(|| { local_api.as_ref().unwrap() });
        let delete_request = squareup::models::BulkDeleteCustomersRequest {
           customer_ids: customers
        };
        match customers_api.bulk_delete_customers(&delete_request).await {
            Ok(r) => {
                Ok(if let Some(map) = r.responses {
                    let mut deletes: u32 = 0;
                    for (_id, response) in map {
                        match response.errors {
                            Some(e) => { deletes += e.len() as u32; },
                            None => {},
                        }
                    }
                    deletes
                }
                else {
                    0
                })
            },
            Err(e) => {
                Err(e.into())
            }
        }
    }
    pub async fn add_customer(&self, capi: Option<&CustomersApi>, c: &super::api::Customer) -> Result<Customer> {
        // There must be a better dance to make this live long enough
        let local_api = match capi {
            Some(_) => None,
            None => Some(CustomersApi::new(self.client.clone()))
        };
        let customers_api = capi.unwrap_or_else(|| { local_api.as_ref().unwrap() });
        let customer = squareup::models::CreateCustomerRequest {
            given_name: Some(c.first_name.to_string()),
            family_name: Some(c.last_name.to_string()),
            email_address: match &c.email {
                Some(email) => if email.trim() == "" { None } else { Some(email.to_string()) },
                None => None
            },
            phone_number: square_phone(&c.phone),
            reference_id: Some(c.id.to_string()),
            ..Default::default()
        };
        let res = customers_api.create_customer(&customer).await;
        match res {
            Ok(ccr) => Ok(ccr.customer),
            Err(e) => { Err(anyhow!("{:?}/{:?} -> {:?}", c.email, c.phone, e)) }
        }
    }

    pub async fn update_customer(&self, capi: Option<&CustomersApi>, sc: &Customer, c: &super::api::Customer, force: bool) -> Result<bool> {
        // There must be a better dance to make this live long enough
        let local_api = match capi {
            Some(_) => None,
            None => Some(CustomersApi::new(self.client.clone()))
        };
        let customers_api = capi.unwrap_or_else(|| { local_api.as_ref().unwrap() });
        let maybe_change = customer_needs_update(sc, c);
        if maybe_change.is_some() || force {
            debug!("customer needs update: {}", maybe_change.unwrap());
            let customer = squareup::models::UpdateCustomerRequest {
                given_name: Some(c.first_name.to_string()),
                family_name: Some(c.last_name.to_string()),
                email_address: match &c.email {
                    Some(email) => Some(email.to_string()),
                    None => None
                },
                phone_number: square_phone(&c.phone),
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
        let customersapi = CustomersApi::new(self.client.clone());
        let groups = self.get_customer_groups(true).await?;
        let dbcusts = sidedb.get_customers_all().await?;
        let square_custs = self.get_customers(Some(&customersapi)).await?;
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
        let mut added_up: u64 = 0;
        let mut updated_up: u64 = 0;

        for dbc in &dbcusts {
            if dbc.deleted {
                continue;
            }
            let t_email = match &dbc.email {
                Some(e) => e.clone(),
                None => " nope ".to_string()
            };
            let t_phone = match square_phone(&dbc.phone) {
                Some(p) => p.clone(),
                None => " nope ".to_string()
            };
            if let Some(cust) =
            if let Some(sc) = square_custs_by_itrid.get(&dbc.id) {
                trace!("found associated customer {:?} : {}", sc.id, dbc.id);
                match self.update_customer(Some(&customersapi), sc, &dbc, false).await {
                    Ok(true) => {
                        debug!("updated customer: {:?} {:?}/{:?}", sc.id, t_email, t_phone);
                        updated_up += 1;
                    }
                    Ok(false) => {
                        trace!("noop customer: {:?} {:?}/{:?}", sc.id, t_email, t_phone);
                    }
                    Err(e) => {
                        error!("Failed to update customer: {:?}", e);
                    }
                }
                Some(sc)
            } else if let Some(sc) = square_custs_by_email.get(&t_email) {
                debug!("found customer by email {:?} : {}", sc.id, dbc.id);
                if dbc.squareup_id != sc.id {
                    match sidedb.associate_customer_with_square(&dbc.id, &sc.id.as_ref().unwrap().to_string()).await {
                        Ok(true) => {
                            match self.update_customer(Some(&customersapi), sc, &dbc, false).await {
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
                }
                Some(sc)
            } else if let Some(sc) = square_custs_by_phone.get(&t_phone) {
                debug!("found customer by phone {:?} : {}", sc.id, dbc.id);
                match sidedb.associate_customer_with_square(&dbc.id, &sc.id.as_ref().unwrap().to_string()).await {
                    Ok(true) => {
                        match self.update_customer(Some(&customersapi), sc, &dbc, false).await {
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
                Some(sc)
            } else {
                debug!("Creating new customer {:?}", dbc.phone);
                match self.add_customer(Some(&customersapi), &dbc).await {
                    Ok(newc) => {
                        added_up += 1;
                        match sidedb.associate_customer_with_square(&dbc.id, &newc.id.as_ref().unwrap().to_string()).await {
                            Ok(false) => { error!("could not find record association for {:?}", newc.email_address); },
                            Err(e) => { error!("could build association for {:?} {:?}", newc.email_address, e); },
                            Ok(true) => {}
                        };
                        // Make it live.
                        if self.set_customer_loyalty(Some(&customersapi), &groups, &&newc, dbc).await? {
                            debug!("Updated loyalty for {}", newc.id.unwrap());
                        }
                        None // can't figure out how to pass Some(&&newc) back, so fix loyalty here ^
                    },
                    Err(e) => {
                        error!("could build association for {:?} {:?}", dbc.email, e);
                        None
                    }
                }
            } {
                // Fix the groups for cust
                if self.set_customer_loyalty(Some(&customersapi), &groups, cust, dbc).await? {
                    debug!("Updated loyalty for {}", cust.id.as_ref().unwrap());
                }
            }
        }
        // Deletes
        let mut to_delete: Vec<String> = vec![];
        for dbc in &dbcusts {
            if dbc.deleted {
                if let Some(sqc) = square_custs_by_itrid.get(&dbc.id) {
                    if let Some(id) = &sqc.id {
                        to_delete.push(id.to_owned());
                    }
                }
            }
        }
        let deleted_up = if to_delete.len() > 0 {
            match self.delete_customer(Some(&customersapi), to_delete).await {
                Ok(count) => { count as u64 },
                Err(e) => {
                    error!("error removing deleted customers: {}", e.to_string());
                    0
                }
            }
        } else {
            0
        };
        Ok(SquareSyncResult { added_up: added_up, added_down: 0, updated_up: updated_up, deleted_up: deleted_up, set_inv_up: 0 })
    }

    pub async fn get_location(&self, name: String) -> Result<Location> {
        let locations = self.get_locations().await?;
        for location in locations {
            if location.name.as_ref().is_some_and(|x| x == &name) {
                if let Some(address) = location.address.as_ref() {
                    if let Some(_state) = address.administrative_district_level_1.as_ref() {
                        return Ok(location)
                    }
                }
            }
        }
        return Err(anyhow!("Cannot find state in location for {}", self.location));
    }
    pub async fn get_measurement_id(&self) -> Result<String> {
        let catalogapi = CatalogApi::new(self.client.clone());
        let mut id = "#newmeasure".to_owned();
        let mut version: Option<i64> = None;
        let response = catalogapi.list_catalog(&ListCatalogParameters{
            types: Some(vec![CatalogObjectType::MeasurementUnit]),
            ..Default::default()
        }).await?;
        if let Some(measures) = response.objects {
            for m in &measures {
                if m.is_deleted.unwrap_or(false) {
                    continue;
                }
                if let Some(mud) = &m.measurement_unit_data {
                    if let Some(mu) = &mud.measurement_unit {
                        if mu.weight_unit == Some(self.weight_unit.clone()) {
                            id = m.id.clone();
                            version = m.version.clone();
                            if mud.precision == Some(self.weight_precision) {
                                debug!("Found existing weight-based measurement: {}", m.id);
                                return Ok(m.id.to_owned())
                            }
                            debug!("Found weight-based measure at wrong precision.");
                        }
                    }
                }
            }
        }
        // Must create this.
        let response = catalogapi.upsert_catalog_object(&UpsertCatalogObjectRequest{
            idempotency_key: Uuid::new_v4().to_string(),
            object: CatalogObject {
                r#type: CatalogObjectType::MeasurementUnit,
                id: id,
                present_at_all_locations: Some(true),
                measurement_unit_data: Some(CatalogMeasurementUnit{
                    measurement_unit: Some(MeasurementUnit {
                        r#type: Some(MeasurementUnitUnitType::TypeWeight),
                        weight_unit: Some(self.weight_unit.clone()),
                        ..Default::default()
                    }),
                    precision: Some(self.weight_precision),
                }),
                version: version,
                ..Default::default()
            }
        }).await?;
        if let Some(o) = response.catalog_object {
            debug!("Created new weight-based measurement: {}", o.id);
            return Ok(o.id.clone());
        }
        Err(anyhow!("Failed to create required weight-based measurement units."))
    }

    pub async fn get_tax(&self, which: TaxLocation<'_>) -> Result<CatalogObject> {
        let state = match which {
            TaxLocation::State(pat) => pat,
            TaxLocation::Location(loc) => {
                if let Some(address) = &loc.address {
                    if let Some(state) = &address.administrative_district_level_1 {
                        state.to_string()
                    } else {
                        return Err(anyhow!("no state in address"));
                    }
                } else {
                    return Err(anyhow!("no address in location"));
                }
            }
        };
        let catalog_api = CatalogApi::new(self.client.clone());
        let mut cursor: Option<String> = None;
        let mut all: Vec<CatalogObject> = vec![];
        let mut cnt = 0;
        loop {
            let types = vec![CatalogObjectType::Tax];
            let res = catalog_api.list_catalog(&ListCatalogParameters {
                cursor: cursor,
                types: Some(types),
                ..Default::default()
            }).await?;
            if let Some(objs) = res.objects {
                for o in objs {
                    if o.r#type == CatalogObjectType::Tax &&
                       o.is_deleted.as_ref() == Some(&false) {
                        if let Some(tax) = o.tax_data.as_ref() {
                            if tax.name.as_ref().unwrap().contains(state.as_str()) {
                                all.push(o);
                                cnt += 1;
                            }
                        }
                    }
                }
            }
            if res.cursor.is_none() { break; }
            cursor = res.cursor;
        }
        if cnt != 1 {
            Err(anyhow!("{} taxes matched", 0))
        } else {
            for o in all {
                return Ok(o)
            }
            Err(anyhow!("impossible"))
        }
    }

    pub async fn get_locations(&self) -> Result<Vec<Location>> {
        let locations_api = LocationsApi::new(self.client.clone());
        let res = locations_api.list_locations().await?;
        match res.locations {
            Some(results) => Ok(results),
            None => Err(anyhow!("No locations found")),
        }
    }

    pub async fn get_products(&self) -> Result<Vec<CatalogObject>> {
        let catalog_api = CatalogApi::new(self.client.clone());
        let mut cursor: Option<String> = None;
        let mut products: Vec<CatalogObject> = vec![];
        loop {
            let types = vec![CatalogObjectType::Item];
            let res = catalog_api.list_catalog(&ListCatalogParameters {
                cursor: cursor,
                types: Some(types),
                ..Default::default()
            }).await?;
            if let Some(objs) = res.objects {
                products.extend(objs);
            }
            if res.cursor.is_none() { break; }
            cursor = res.cursor;
        }
        Ok(products)
    }
    pub async fn update_product(&self, p: CatalogObject) -> Result<CatalogObject> {
        let catalogapi = CatalogApi::new(self.client.clone());
        let response =
            catalogapi.upsert_catalog_object(&UpsertCatalogObjectRequest{
                idempotency_key: Uuid::new_v4().to_string(),
                object: p,
            }).await;
        match response {
            Err(e) => Err(e.into()),
            Ok(o) => {
                match o.catalog_object {
                    Some(o) => Ok(o),
                    None => {
                        if let Some(errors) = &o.errors {
                            if errors.len() > 0 {
                                let err = &errors[0];
                                Err(anyhow!("{:?}", err))
                            } else {
                                Err(anyhow!("unknown error from square creating object"))
                            }
                        } else {
                            Err(anyhow!("unknown error from square creating object"))
                        }
                    },
                }
            }
        }
    }
    async fn create_product(&self, p: &ProductData, builder: &MetaBuilder) -> Result<CatalogObject> {
    //tax: &CatalogObject, location: &Location)
        let newp: CatalogObject = builder.build(p).into();
        let catalogapi = CatalogApi::new(self.client.clone());
        let response =
            catalogapi.upsert_catalog_object(&UpsertCatalogObjectRequest{
                idempotency_key: Uuid::new_v4().to_string(),
                object: newp,
            }).await;
        match response {
            Err(e) => Err(e.into()),
            Ok(o) => {
                match o.catalog_object {
                    Some(o) => Ok(o),
                    None => {
                        if let Some(errors) = &o.errors {
                            if errors.len() > 0 {
                                let err = &errors[0];
                                Err(anyhow!("{:?}", err))
                            } else {
                                Err(anyhow!("unknown error from square creating object"))
                            }
                        } else {
                            Err(anyhow!("unknown error from square creating object"))
                        }
                    },
                }
            }
        }
    }

    pub async fn sync_products_with_sidedb(&self, sidedb: &mut super::sidedb::SideDb, set_inventory: bool) -> Result<SquareSyncResult> {
        let mut added_up: u64 = 0;
        let mut updated_up: u64 = 0;
        let mut inv_count: Vec<InventoryChange> = vec![];
        let now = DateTime::now();

        let location = self.get_location(self.location.to_string()).await?;
        let tax = self.get_tax(TaxLocation::Location(&location)).await?;
        let weight_measure_id = self.get_measurement_id().await?;
        let meta_builder = MetaBuilder {
            location_id: location.id.as_ref().unwrap().clone(),
            tax_id: tax.id.clone(),
            measurement_id: weight_measure_id,
        };
        let items = self.get_products().await?;
        let mut product_by_sku = HashMap::<String,&CatalogObject>::new();

        for item in &items {
            if let Some(d) = &item.item_data {
                if let Some(v) = &d.variations {
                    if v.len() == 1 {
                        if let Some(vd) = &v[0].item_variation_data {
                            if let Some(sku) = &vd.sku {
                                if let Some(_old) = product_by_sku.insert(sku.to_string(), item) {
                                    error!("SKU {} is duplicated in Square", sku);
                                }
                            }
                        }
                    }
                }
            }
        }
        let dbprods = sidedb.get_products(None).await?;
        for dbprod in &dbprods {
            let maybe_upca = dbprod.upca();
            if maybe_upca.is_none() {
                info!("IT Retail product skipped, invalid UPC {}", dbprod.upc);
                continue;
            }
            let upca = maybe_upca.unwrap();

            if let Some(variant_item_id) = if let Some(existing) = product_by_sku.get(&upca) {
                let mut updated: CatalogObject = meta_builder.build(dbprod).into();
                catalogobject_getsku(&updated)?; // NEEDS A SKU
                match catalogitem_needs_update(existing, &updated) {
                    Ok(Some(changed)) => {
                        debug!("detectect change: {}\n{:#?}\n{:#?}\n", changed, &existing, &updated);
                        match catalogitem_adopt_ids(&mut updated, &existing) {
                            Ok(_) => {
                                match self.update_product(updated).await {
                                    Ok(o) => {
                                        updated_up += 1;
                                        debug!("{:#?}", o);
                                    },
                                    Err(e) => {
                                        error!("Failed to update item in square: {}", e.to_string());
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Failed to prepare item for update in square: {}", e.to_string());
                            }
                        }
                    },
                    Ok(None) => {}
                    Err(e) => {
                       error!("Existing product {}/{} is malformed, please fix or delete it: {:?}", dbprod.upc, existing.id, e);
                    }
                }
                let maybe_variant_item_id = get_variant_item_id(existing);
                if let Some(variant_item_id) = maybe_variant_item_id {
                    if dbprod.squareup_id.is_none() || &variant_item_id != dbprod.squareup_id.as_ref().unwrap() {
                        debug!("updating sidedb association {} <-> {:?} -> {}", dbprod.upc, dbprod.squareup_id, variant_item_id);
                        match sidedb.associate_product_with_square(&dbprod.upc, &variant_item_id).await {
                            Ok(success) => debug!("successfully updated: {}", success),
                            Err(e) => debug!("failed to update: {}", e.to_string())
                        }
                    }
                    Some(variant_item_id)
                } else {
                    None
                }
            } else {
                debug!("{} needs creation as {}", dbprod.upc, upca);
                let result = self.create_product(&dbprod, &meta_builder).await;
                match result {
                    Ok(o) => {
                        catalogobject_getsku(&o)?; // NEEDS A SKU
                        if let Some(variant_item_id) = get_variant_item_id(&o) {
                            debug!("updating sidedb association {} <-> {:?} -> {}", dbprod.upc, dbprod.squareup_id, variant_item_id);
                            match sidedb.associate_product_with_square(&dbprod.upc, &variant_item_id).await {
                                Ok(success) => debug!("successfully updated: {}", success),
                                Err(e) => debug!("failed to update: {}", e.to_string())
                            }
                            debug!("created with id: {:?}", variant_item_id);
                            added_up +=1;
                            Some(variant_item_id.to_owned())
                        } else {
                            error!("error reading variant item_id from square.");
                            None
                        }
                    },
                    Err(e) => {
                        error!("error creating {}: {}", dbprod.upc, e);
                        None
                    }
                }
            } {
                error!{"inv_count adding: {}", &variant_item_id};
                inv_count.push(new_inventory_physical_count(&variant_item_id, &now, location.id.as_ref().unwrap(), dbprod.quantity_on_hand.unwrap_or(0.0)));
            }
        }
        let mut set_inv_up: u64 = 0;
        if set_inventory && inv_count.len() > 0 {
            let inventoryapi = InventoryApi::new(self.client.clone());
            let mut offset: usize= 0;
            const MAX_BATCH:usize = 100;
            let inv_count_len = inv_count.len();
            while offset < inv_count_len {
                let batch_len = std::cmp::min(MAX_BATCH, inv_count_len - offset);
                let response = inventoryapi.batch_change_inventory(&BatchChangeInventoryRequest{
                    idempotency_key: Uuid::new_v4().to_string(),
                    changes: Some(inv_count[offset..offset+batch_len].to_vec()),
                    ignore_unchanged_counts: Some(true),
                }).await;
                offset += MAX_BATCH;
                match response {
                    Ok(invr) => {
                        set_inv_up += invr.counts.unwrap_or(vec![]).len() as u64;
                        let errcnt = match invr.errors {
                            Some(errors) => {
                                for e in &errors {
                                    error!("error updating inventory: {:?}", e);
                                }
                                errors.len()
                            },
                            None => 0
                        };
                        if errcnt > 0 {
                            debug!("errors: {}", errcnt);
                        }
                    },
                    Err(e) => { return Err(e.into()) },
                }
            }
        }
        Ok(SquareSyncResult { added_up: added_up, added_down: 0, deleted_up: 0, updated_up: updated_up, set_inv_up: set_inv_up })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_phone1() {
        assert_eq!(square_phone(&Some("US+15553431212".to_owned())), Some("(555) 343-1212".to_owned()));
    }
    #[test]
    fn test_phone_long() {
        assert_eq!(square_phone(&Some("us+155534312122".to_owned())), None);
    }
    #[test]
    fn test_phone_compact() {
        assert_eq!(square_phone(&Some("5553431212".to_owned())), Some("(555) 343-1212".to_owned()));
    }
}