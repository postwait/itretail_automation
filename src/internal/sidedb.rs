use anyhow::Result;
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;
use rust_decimal::prelude::*;
use chrono::{NaiveDate, NaiveDateTime};
use log::*;
use uuid::Uuid;
use std::collections::HashMap;

use super::api::{Customer, Department, ITRTaxId, ProductData, Section, ShrinkAmount, Tax};

use squareup::models::{enums::{Currency, OrderState, PaymentSourceType, PaymentStatus}, Money};

struct SSql {}
impl SSql {
    pub fn from_order_state(o: &Option<OrderState>) -> Option<String> {
        match o {
            None => None,
            Some(ov) => Some(match ov {
                &OrderState::Canceled => "Canceled",
                &OrderState::Completed => "Completed",
                &OrderState::Draft => "Draft",
                &OrderState::Open => "Open",
            }.to_owned()),
        }
    }
    pub fn from_payment_status(o: &Option<PaymentStatus>) -> Option<String> {
        match o {
            None => None,
            Some(ov) => Some(match ov {
                &PaymentStatus::Approved => "Approved",
                &PaymentStatus::Canceled => "Canceled",
                &PaymentStatus::Completed => "Completed",
                &PaymentStatus::Failed => "Failed",
                &PaymentStatus::Pending => "Pending",
            }.to_owned()),
        }
    }
    pub fn from_payment_source_type(o: &Option<PaymentSourceType>) -> Option<String> {
        match o {
            None => None,
            Some(ov) => Some(match ov {
                &PaymentSourceType::BankAccount => "BankAccount",
                &PaymentSourceType::BuyNowPayLater => "BuyNowPayLater",
                &PaymentSourceType::Card => "Card",
                &PaymentSourceType::Cash => "Cash",
                &PaymentSourceType::External => "External",
                &PaymentSourceType::SquareAccount => "SquareAccount",
                &PaymentSourceType::Wallet => "Wallet",
            }.to_owned())
        }
    }
    pub fn from_money(o: &Option<Money>) -> Option<Decimal> {
        match o {
            None => Some(Decimal::ZERO),
            Some(ov) => {
                if ov.currency != Currency::Usd {
                    None
                } else {
                    Some(Decimal::new(ov.amount as i64, 2))
                }
            }
        }
    }
}

pub struct SideDb {
    client: tokio_postgres::Client,
    handle: JoinHandle<()>,
    shrink_reason: u32,
}

impl Drop for SideDb {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

pub async fn make_sidedb(settings: super::settings::Settings) -> Result<SideDb> {
    let (client, connection) = tokio_postgres::connect(&settings.postgres.connect_string, NoTls).await?;
    let handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });
    Ok(SideDb{client: client, handle: handle, shrink_reason: settings.itretail.external_sale_shrink_reason})
}

fn decimal_price(a: &str) -> Decimal {
    Decimal::from_str(a.strip_prefix("$").unwrap_or("0")).unwrap()
}
fn some_f32_to_some_decimal(a: &Option<f32>) -> Option<Decimal> {
    if a.is_none() { None }
    else { Decimal::from_f32(a.unwrap()) }
}
impl SideDb {
    pub async fn store_txns<'a, I>(&mut self, txns: I) -> Result<u32>
    where
        I: Iterator<Item = &'a super::api::EJTxn>
    {
        let sqltxn = self.client.transaction().await?;
        let mut cnt = 0;
        for t in txns {
            let td = NaiveDateTime::parse_from_str(&t.transaction_date, "%Y-%m-%dT%H:%M:%S%.f")?;
            let num_rows = sqltxn.execute("INSERT INTO itrejtxn (transaction_id, customer_id, transaction_date, canceled, total)
            VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
            &[&t.id, &t.customer_id, &td, &t.canceled, &Decimal::from_f64(t.total)]).await?;
            if num_rows > 0 {
                if let Some(products) = t.transaction_products.as_ref() {
                    for p in products {
                        let upc = match &p.product_change {
                            Some(pc) => Some(pc.upc.clone()),
                            None => None,
                        };
                        sqltxn.execute("INSERT INTO itrejtxn_products
                            (transaction_subid, transaction_id, product_id, upc, is_voided, is_refunded, price, line_discount, quantity, weight)
                            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING",
                        &[&p.id, &t.id, &p.product_id, &upc, &p.is_voided, &p.is_refunded,
                          &Decimal::from_f64(p.price), &Decimal::from_f64(p.line_discount), &p.quantity, &p.weight]).await?;
                    }
                }
                cnt += 1;
            }
        }
        sqltxn.commit().await?;
        Ok(cnt)
    }
    pub async fn store_customers<'a, I>(&mut self, customers: I) -> Result<u32>
    where
        I: Iterator<Item = super::api::Customer>,
    {
        let existing = { self.get_customers().await? };
        let mut to_delete: HashMap<Uuid, &Customer> = HashMap::new();
        for c in existing.iter() {
           to_delete.insert(c.id, &c);
        }
        let total_db_size = to_delete.len() as f64;

        let txn = { self.client.transaction().await? };
        let mut cnt = 0;

        for c in customers {
            debug!("copying {}", c.email.as_ref().unwrap_or(&"<unknown>".to_string()));
            to_delete.remove(&c.id);
            let bd = match c.birth_date.as_ref() {
                Some(d) => {
                    match NaiveDate::parse_from_str(&d, "%Y-%m-%d") {
                        Ok(r) => Some(r),
                        Err(_) => None,
                    }
                },
                None => None,
            };
            let ed = match c.expiration_date.as_ref() {
                Some(d) => {
                    match NaiveDateTime::parse_from_str(&d, "%Y-%m-%dT%H:%M:%S%.f") {
                        Ok(r) => Some(r),
                        Err(_) => None,
                    }
                },
                None => None,
            };
            let cd = match c.created.as_ref() {
                Some(d) => {
                    match NaiveDateTime::parse_from_str(&d, "%Y-%m-%dT%H:%M:%S%.f") {
                        Ok(r) => Some(r),
                        Err(e) => {
                            error!("Can't convert '{}': {}", d, e);
                            None
                        },
                    }
                },
                None => None,
            };
            let md = match c.modified.as_ref() {
                Some(d) => {
                    match NaiveDateTime::parse_from_str(&d, "%Y-%m-%dT%H:%M:%S%.f") {
                        Ok(r) => Some(r),
                        Err(_) => None,
                    }
                },
                None => None,
            };
            let modified_by = match c.modified_by { Some(id) => Some(id as i32), None => None };
            let inc = match c.inc { Some(id) => Some(id as i64), None => None };
            let re = txn.execute("INSERT INTO customer
                            (customer_id, card_no, first_name, last_name, birth_date, phone,
                             discount, deleted, email, balance, balance_limit, loyalty_points, expiration_date,
                             instore_charge_enabled, address1, address2, city, state, zipcode, created, modified, modified_by,
                             frequent_shopper, cash_back, inc)
                             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
                             ON CONFLICT (customer_id) DO UPDATE SET card_no = EXCLUDED.card_no, first_name = EXCLUDED.first_name,
                                last_name = EXCLUDED.last_name, birth_date = EXCLUDED.birth_date, phone = EXCLUDED.phone,
                                discount = EXCLUDED.discount, deleted = EXCLUDED.deleted, email = EXCLUDED.email,
                                balance = EXCLUDED.balance, balance_limit = EXCLUDED.balance_limit, loyalty_points = EXCLUDED.loyalty_points,
                                expiration_date = EXCLUDED.expiration_date, instore_charge_enabled = EXCLUDED.instore_charge_enabled,
                                address1 = coalesce(EXCLUDED.address1, customer.address1), address2 = coalesce(EXCLUDED.address2, customer.address2),
                                city = coalesce(EXCLUDED.city, customer.city), state = coalesce(EXCLUDED.state, customer.state),
                                zipcode = coalesce(EXCLUDED.zipcode, customer.zipcode),
                                created = coalesce(EXCLUDED.created, customer.created), modified = coalesce(EXCLUDED.modified, customer.modified),
                                modified_by = coalesce(EXCLUDED.modified_by, customer.modified_by), frequent_shopper = EXCLUDED.frequent_shopper,
                                cash_back = coalesce(EXCLUDED.cash_back, customer.cash_back), inc = coalesce(EXCLUDED.inc, customer.inc)",
                        &[&c.id, &c.card_no, &c.first_name, &c.last_name, &bd, &c.phone,
                                  &(c.discount.unwrap_or(0) as i32), &c.deleted, &c.email,
                                  &Decimal::from_f64(c.balance.unwrap_or(0.0)), &Decimal::from_f64(c.balance_limit.unwrap_or(0.0)),
                                  &c.loyalty_points.unwrap_or(0), &ed, &(c.instore_charge_enabled.unwrap_or(false)),
                                  &c.address1, &c.address2, &c.city, &c.state, &c.zipcode, &cd, &md, &modified_by,
                                  &(c.frequent_shopper.unwrap_or(false)),&Decimal::from_f64(c.cash_back.unwrap_or(0.0)),&inc]).await?;
            cnt = cnt + re as u32;
        }
        txn.commit().await?;
        if to_delete.len() as f64 / total_db_size > 0.02 {
            error!("We want to delete {} customers out of {}, that's scary high. You'll need to do that manually.",
                   to_delete.len(), total_db_size);
        }
        else {
            info!("Marking {} customers as deleted.", to_delete.len());
            for (id, c) in to_delete {
                info!("Marking {} ({} {} {} {}) as deleted.", id, c.first_name, c.last_name, c.email.as_ref().unwrap_or(&"n/a".to_string()), c.phone.as_ref().unwrap_or(&"n/a".to_string()));
                let _ = self.delete_customer(&id).await;
            }
        }
        Ok(cnt)
    }
    pub async fn associate_customer_with_square(&mut self, id: &Uuid, squareup_id: &String) -> Result<bool> {
        let txn = self.client.transaction().await?;
        let rc = txn.execute("UPDATE customer SET squareup_id=$1 WHERE customer_id = $2", &[squareup_id, id]).await?;
        txn.commit().await?;
        Ok(rc > 0)
    }
    pub async fn delete_customer(&mut self, id: &Uuid) -> Result<bool> {
        let txn = self.client.transaction().await?;
        let rc = txn.execute("UPDATE customer SET deleted=true WHERE customer_id = $1", &[id]).await?;
        txn.commit().await?;
        Ok(rc > 0)
    }
    pub async fn get_customer_household(&mut self) -> Result<Vec<(Uuid, Uuid)>> {
        let rows = self.client.query("SELECT main, resident FROM customer_house", &[]).await?;
        let rels = rows.iter().map(|x| { (x.get("main"), x.get("resident")) }).collect();
        Ok(rels)
    }
    pub async fn get_customers(&mut self) -> Result<Vec<Customer>> {
        self.get_customers_ex(false).await
    }
    pub async fn get_customers_all(&mut self) -> Result<Vec<Customer>> {
        self.get_customers_ex(true).await
    }
    pub async fn get_customers_ex(&mut self, deleted: bool) -> Result<Vec<Customer>> {
        let sql = if deleted {
            "SELECT * FROM customer"
        } else {
            "SELECT * FROM customer WHERE NOT deleted"
        };
        let rows = self.client.query(sql, &[]).await?;
        let customers = rows.iter().map(|x| {
            Customer{ id: x.get("customer_id"), card_no: x. get("card_no"),
                      last_name: x.get("last_name"), first_name: x.get("first_name"),
                      birth_date: x.get::<&str,Option<NaiveDate>>("birth_date").and_then(|x| Some(x.to_string())),
                      phone: x.get("phone"), discount: Some(x.get::<&str,i32>("discount") as u8),
                      deleted: x.get("deleted"), email: x.get("email"), balance: x.get::<&str,Option<Decimal>>("balance").and_then(|x| x.to_f64()),
                      balance_limit: x.get::<&str,Option<Decimal>>("balance_limit").and_then(|x| x.to_f64()),
                      loyalty_points: Some(x.get("loyalty_points")),
                      expiration_date: x.get::<&str,Option<NaiveDateTime>>("expiration_date").and_then(|x| Some(x.to_string())),
                      instore_charge_enabled: Some(x.get("instore_charge_enabled")),
                      address1: x.get("address1"), address2: x.get("address2"),
                      city: x.get("city"), state: x.get("state"), zipcode: x.get("zipcode"),
                      created: x.get::<&str,Option<NaiveDateTime>>("created").and_then(|x| Some(x.to_string())),
                      modified: x.get::<&str,Option<NaiveDateTime>>("modified").and_then(|x| Some(x.to_string())),
                      modified_by: x.get::<&str,Option<i32>>("modified_by").and_then(|x| Some(x as u32)),
                      frequent_shopper: x.get("frequent_shopper"),
                      cash_back: x.get::<&str,Option<Decimal>>("cash_back").and_then(|x| x.to_f64()),
                      inc: x.get::<&str,Option<i64>>("inc").and_then(|x| Some(x as u32)),
                      squareup_id: x.get("squareup_id"),
            }
        }).collect();
        Ok(customers)
    }
    pub async fn store_orders<'a, I>(&mut self, orders: I) -> Result<u32>
    where
        I: Iterator<Item = &'a super::localexpress::Order>,
    {

        let txn = self.client.transaction().await?;
        let mut cnt = 0;
        for o in orders {
            let cd = o.delivery_time_period.split(" - ").collect::<Vec<&str>>();
            let (st, et) = if cd.len() == 2 { (cd[0], cd[1]) }
            else { ("00:00","23:59") };
            let dd = o.delivery_date.format("%Y-%m-%d").to_string();
            let (sd,ed) =
                (NaiveDateTime::parse_from_str(&format!("{}T{}:00", dd, st), "%Y-%m-%dT%H:%M:%S")?,
                NaiveDateTime::parse_from_str(&format!("{}T{}:00", dd, et),"%Y-%m-%dT%H:%M:%S")?);
            let re = txn.execute("INSERT INTO leorder
                           (id, uniqid, store_id, status,
                            subtotal, tips, total,
                            mode, payment_method, customer_first_name, customer_last_name,
                            customer_phone_number, customer_email, creation_date, delivery_date, delivery_time_period)
                            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,tsrange($16,$17))
                            ON CONFLICT (uniqid) DO UPDATE SET
                            id = EXCLUDED.id, store_id = EXCLUDED.store_id, status = EXCLUDED.status,
                            subtotal = EXCLUDED.subtotal, tips = EXCLUDED.tips, total = EXCLUDED.total,
                            mode = EXCLUDED.mode, payment_method = EXCLUDED.payment_method,
                            customer_first_name = EXCLUDED.customer_first_name, customer_last_name = EXCLUDED.customer_last_name,
                            customer_phone_number = EXCLUDED.customer_phone_number, customer_email = EXCLUDED.customer_email,
                            creation_date = EXCLUDED.creation_date, delivery_date = EXCLUDED.delivery_date,
                            delivery_time_period = EXCLUDED.delivery_time_period",
                    &[&(o.id as i64), &o.uniqid, &(o.store_id as i64), &o.status,
                      &decimal_price(&o.subtotal), &decimal_price(&o.tips), &decimal_price(&o.total),
                      &o.mode, &o.payment_method, &o.customer_first_name, &o.customer_last_name,
                      &o.customer_phone_number, &o.customer_email, &o.creation_date, &o.delivery_date, &sd, &ed]).await?;
            cnt += re as u32;
        }
        txn.commit().await?;
        Ok(cnt)
    }

    pub async fn store_taxes_itr<'a, I>(&mut self, taxes: I) -> Result<u32>
    where
        I: Iterator<Item = &'a Tax>,
    {
        let txn = self.client.transaction().await?;
        let mut cnt = 0;
        for t in taxes {
            txn.execute("INSERT INTO tax (id, description, rate)
                        VALUES($1,$2,$3) ON CONFLICT (id) DO UPDATE SET description = EXCLUDED.description, rate = EXCLUDED.rate",
                        &[&t.id.0, &t.description, &Decimal::from_f64(t.rate)
                        ]).await?;
            cnt += 1;
        }
        txn.commit().await?;
        Ok(cnt)
    }

    pub async fn associate_product_with_square(&mut self, upc: &String, squareup_id: &String) -> Result<bool> {
        let txn = self.client.transaction().await?;
        let rc = txn.execute("UPDATE itrproduct SET squareup_id=$1 WHERE upc = $2", &[squareup_id, upc]).await?;
        txn.commit().await?;
        Ok(rc > 0)
    }

    pub async fn store_departments<'a, I>(&mut self, depts: I) -> Result<u32>
    where
        I: Iterator<Item = &'a super::api::Department>,
    {
        let txn = self.client.transaction().await?;
        let mut cnt = 0;
        for d in depts {
                txn.execute("INSERT INTO itrdepartment
                            (id, name) VALUES($1, $2)
                            ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name",
                            &[&d.id, &d.name]).await?;
            cnt += 1;
        }
        txn.commit().await?;
        Ok(cnt)
    }

    pub async fn get_departments(&self) -> Result<Vec<Department>> {
        let rows = self.client.query("SELECT * from itrdepartment", &[]).await?;
        Ok(rows.iter().map(|x| {
            Department {
                id: x.get("id"),
                name: x.get("name"),
                squareup_id: x.get("squareup_id")
            }
        }).collect())
    }

    pub async fn associate_department_with_square(&mut self, id: &i32, squareup_id: &String) -> Result<bool> {
        let txn = self.client.transaction().await?;
        let rc = txn.execute("UPDATE itrdepartment SET squareup_id=$1 WHERE id = $2", &[squareup_id, id]).await?;
        txn.commit().await?;
        Ok(rc > 0)
    }

    pub async fn store_sections<'a, I>(&mut self, sections: I) -> Result<u32>
    where
        I: Iterator<Item = &'a super::api::Section>,
    {
        let txn = self.client.transaction().await?;
        let mut cnt = 0;
        for s in sections {
                txn.execute("INSERT INTO itrsection
                            (id, name, department_id, deleted) VALUES($1, $2, $3, $4)
                            ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name",
                            &[&s.id, &s.name, &s.department_id, &s.deleted]).await?;
            cnt += 1;
        }
        txn.commit().await?;
        Ok(cnt)
    }

    pub async fn get_sections(&self) -> Result<Vec<Section>> {
        let rows = self.client.query("SELECT * from itrsection", &[]).await?;
        Ok(rows.iter().map(|x| {
            Section {
                id: x.get("id"),
                name: x.get("name"),
                department_id: x.get("department_id"),
                deleted: x.get("deleted"),
                squareup_id: x.get("squareup_id")
            }
        }).collect())
    }

    pub async fn associate_section_with_square(&mut self, id: &i32, squareup_id: &String) -> Result<bool> {
        let txn = self.client.transaction().await?;
        let rc = txn.execute("UPDATE itrsection SET squareup_id=$1 WHERE id = $2", &[squareup_id, id]).await?;
        txn.commit().await?;
        Ok(rc > 0)
    }

    pub async fn store_products<'a, I>(&mut self, products: I) -> Result<u32>
    where
        I: Iterator<Item = &'a super::api::ProductData>,
    {
        let txn = self.client.transaction().await?;
        let mut cnt = 0;
        txn.execute("INSERT INTO itrproduct_archive SELECT * FROM itrproduct ON CONFLICT DO NOTHING", &[]).await?;
        for p in products {
            if p.special_price.is_some() && p.start_date.is_some() && p.end_date.is_some() {
                txn.execute("INSERT INTO itrproduct
                            (upc, description, second_description, normal_price, special_price, special_date,
                             scale, active, deleted, discount, plu, cert_code, vendor_id, department_id, section_id,
                             wicable, foodstamp, quantity_on_hand, size, case_cost, pack, cost, taxclass)
                        VALUES($1,$2,$3,$4,$5,tsrange($6,$7),$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
                        ON CONFLICT (upc) DO UPDATE SET
                        upc=EXCLUDED.upc, description=EXCLUDED.description, second_description=EXCLUDED.second_description,
                        normal_price=EXCLUDED.normal_price, special_price=EXCLUDED.special_price, special_date=EXCLUDED.special_date,
                        scale=EXCLUDED.scale, active=EXCLUDED.active, deleted=EXCLUDED.deleted, discount=EXCLUDED.discount,
                        plu=EXCLUDED.plu, cert_code=EXCLUDED.cert_code, vendor_id=EXCLUDED.vendor_id, department_id=EXCLUDED.department_id,
                        section_id=EXCLUDED.section_id, wicable=EXCLUDED.wicable, foodstamp=EXCLUDED.foodstamp,
                        quantity_on_hand=EXCLUDED.quantity_on_hand, size=EXCLUDED.size, case_cost=EXCLUDED.case_cost,
                        pack=EXCLUDED.pack, cost=EXCLUDED.cost, taxclass=EXCLUDED.taxclass",
                        &[&p.upc, &p.description, &p.second_description, &Decimal::from_f64(p.normal_price),
                        &Decimal::from_f64(p.special_price.unwrap()),
                        &NaiveDateTime::parse_from_str(p.start_date.as_ref().unwrap(), "%Y-%m-%dT%H:%M:%S")?, &NaiveDateTime::parse_from_str(p.end_date.as_ref().unwrap(), "%Y-%m-%dT%H:%M:%S")?,
                        &p.scale, &p.active, &p.deleted, &(p.discountable != 0), &p.plu, &p.cert_code, &p.vendor_id, &p.department_id, &p.section_id,
                        &p.wicable, &p.foodstamp, &(p.quantity_on_hand.unwrap_or(0.0) as f64), &p.size, &some_f32_to_some_decimal(&p.case_cost), &p.pack, &some_f32_to_some_decimal(&p.cost),
                        &p.taxclass.0
                        ]).await?;

            }
            else {
                txn.execute("INSERT INTO itrproduct
                            (upc, description, second_description, normal_price,
                             scale, active, deleted, discount, plu, cert_code, vendor_id, department_id, section_id,
                             wicable, foodstamp, quantity_on_hand, size, case_cost, pack, cost, taxclass)
                        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
                        ON CONFLICT (upc) DO UPDATE SET
                        upc=EXCLUDED.upc, description=EXCLUDED.description, second_description=EXCLUDED.second_description,
                        normal_price=EXCLUDED.normal_price, special_price=EXCLUDED.special_price, special_date=EXCLUDED.special_date,
                        scale=EXCLUDED.scale, active=EXCLUDED.active, deleted=EXCLUDED.deleted, discount=EXCLUDED.discount,
                        plu=EXCLUDED.plu, cert_code=EXCLUDED.cert_code, vendor_id=EXCLUDED.vendor_id, department_id=EXCLUDED.department_id,
                        section_id=EXCLUDED.section_id, wicable=EXCLUDED.wicable, foodstamp=EXCLUDED.foodstamp,
                        quantity_on_hand=EXCLUDED.quantity_on_hand, size=EXCLUDED.size, case_cost=EXCLUDED.case_cost,
                        pack=EXCLUDED.pack, cost=EXCLUDED.cost, taxclass=EXCLUDED.taxclass",
                        &[&p.upc, &p.description, &p.second_description, &Decimal::from_f64(p.normal_price),
                        &p.scale, &p.active, &p.deleted, &(p.discountable != 0), &p.plu, &p.cert_code, &p.vendor_id, &p.department_id, &p.section_id,
                        &p.wicable, &p.foodstamp, &(p.quantity_on_hand.unwrap_or(0.0) as f64), &p.size,
                        &some_f32_to_some_decimal(&p.case_cost), &p.pack, &some_f32_to_some_decimal(&p.cost), &p.taxclass.0
                        ]).await?;
            }
            cnt += 1;
        }
        txn.commit().await?;
        Ok(cnt)
    }

    pub async fn get_products(&mut self, date: Option<&NaiveDate>) -> Result<Vec<ProductData>> {
        let rows = if date.is_some() {
            let dr = date.unwrap();
            self.client.query("SELECT *, lower(special_date) as start_date, upper(special_date) as end_date
                FROM itrproduct_archive
                WHERE NOT deleted and date(timezone('US/Eastern',recorded_at)) = $1
                ORDER BY department_id, section_id", &[dr]).await
        } else {
            self.client.query("SELECT *, lower(special_date) as start_date, upper(special_date) as end_date
                FROM itrproduct
                WHERE NOT deleted
                ORDER BY department_id, section_id", &[]).await
        }?;
        let products = rows.iter().map(|x| {
            ProductData { upc: x.get("upc"), description: x.get("description"),
                second_description: x.get("second_description"), normal_price: x.get::<&str,Decimal>("normal_price").to_f64().unwrap(),
                special_price: x.get::<&str,Option<Decimal>>("special_price").and_then(|x| x.to_f64()),
                start_date: x.get::<&str,Option<NaiveDateTime>>("start_date").and_then(|x| Some(x.to_string())),
                end_date: x.get::<&str,Option<NaiveDateTime>>("end_date").and_then(|x| Some(x.to_string())),
                scale: x.get("scale"), active: x.get("active"),
                discountable: if x.get::<&str,bool>("discount") { 1 } else { 0 }, plu: x.get("plu"),
                deleted: x.get("deleted"), cert_code: x.get("cert_code"), vendor_id: x.get("vendor_id"),
                department_id: x.get("department_id"), section_id: x.get("section_id"), wicable: x.get("wicable"),
                foodstamp: x.get("foodstamp"), quantity_on_hand: x.get::<&str,Option<f64>>("quantity_on_hand").and_then(|x| Some(x as f32)), size: x.get("size"),
                case_cost: x.get::<&str,Option<Decimal>>("case_cost").and_then(|x| x.to_f32()), pack: x.get("pack"),
                cost: x.get::<&str,Option<Decimal>>("cost").and_then(|x| x.to_f32()),
                taxclass: ITRTaxId(x.get("taxclass")), squareup_id: x.get("squareup_id"),
             }
        }).collect();
        Ok(products)
    }

    pub async fn shrink_square_products_sold(&mut self, itrapi: &mut super::api::ITRApi) -> Result<u32> {
        let txn = self.client.transaction().await?;
        let rows = txn.query("
            with toshrink as
            (update sqorderitem
            set shrink_completed = current_timestamp 
            where order_id in (select order_id
                               from sqtxn join sqorder using(order_id)
            				   where status = 'Completed' and state = 'Completed')
            	and shrink_completed is null
            returning squareup_id, quantity)
            select itrproduct.*, quantity
            from itrproduct join
                 (select squareup_id, sum(quantity) as quantity from toshrink group by squareup_id) as shrinkage
            using(squareup_id)", &[]).await?;
        let toshrink: Vec<super::api::ShrinkItem> = rows.iter().map(|x| {
            let pd = ProductData { upc: x.get("upc"), description: x.get("description"),
                second_description: x.get("second_description"), normal_price: x.get::<&str,Decimal>("normal_price").to_f64().unwrap(),
                special_price: x.get::<&str,Option<Decimal>>("special_price").and_then(|x| x.to_f64()),
                start_date: None, end_date: None,
                scale: x.get("scale"), active: x.get("active"),
                discountable: if x.get::<&str,bool>("discount") { 1 } else { 0 }, plu: x.get("plu"),
                deleted: x.get("deleted"), cert_code: x.get("cert_code"), vendor_id: x.get("vendor_id"),
                department_id: x.get("department_id"), section_id: x.get("section_id"), wicable: x.get("wicable"),
                foodstamp: x.get("foodstamp"), quantity_on_hand: x.get::<&str,Option<f64>>("quantity_on_hand").and_then(|x| Some(x as f32)), size: x.get("size"),
                case_cost: x.get::<&str,Option<Decimal>>("case_cost").and_then(|x| x.to_f32()), pack: x.get("pack"),
                cost: x.get::<&str,Option<Decimal>>("cost").and_then(|x| x.to_f32()),
                taxclass: ITRTaxId(x.get("taxclass")), squareup_id: x.get("squareup_id"),
            };
            let quantity = x.get::<&str,Decimal>("quantity").to_f32().unwrap();
            super::api::make_shrink_item(
                &pd,
                self.shrink_reason,
                if pd.scale { ShrinkAmount::Weight(quantity)} else { ShrinkAmount::Quantity(quantity as u32) }
            )
        }).collect();
        let cnt = toshrink.len() as u32;
        itrapi.shrink_product(toshrink).await?;
        txn.commit().await?;
        Ok(cnt)
    }

    pub async fn store_square_transactions(&mut self, payments: &Vec<squareup::models::Payment>) -> Result<u32> {
        let txn = self.client.transaction().await?;
        let mut cnt: u32 = 0;
        for p in payments {
            let processing_fees = Some(p.processing_fee.as_ref().unwrap_or(&vec![]).iter()
                .fold(Decimal::ZERO, |acc, e| {
                    let to_add = SSql::from_money(&e.amount_money).unwrap();
                    to_add.checked_add(acc).unwrap()
                }));
            let created_at: chrono::DateTime<chrono::Utc> = p.created_at.as_ref().unwrap().clone().into();
            let updated_at: chrono::DateTime<chrono::Utc> = p.updated_at.as_ref().unwrap().clone().into();
            let rv = txn.execute("INSERT INTO sqtxn (id, customer_id, status, order_id, source_type, amount_money,
                                                     tip_money, processing_fees, refunded_money, created_at, updated_at)
                                  VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                                  ON CONFLICT (id) DO UPDATE SET
                                  status=EXCLUDED.status, source_type=EXCLUDED.source_type, amount_money=EXCLUDED.amount_money,
                                  tip_money=EXCLUDED.tip_money, processing_Fees=EXCLUDED.processing_fees, refunded_money=EXCLUDED.refunded_money,
                                  created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at",
                                &[&p.id, &p.customer_id, &SSql::from_payment_status(&p.status),
                                &p.order_id, &SSql::from_payment_source_type(&p.source_type),
                                &SSql::from_money(&p.amount_money), &SSql::from_money(&p.tip_money), &processing_fees,
                                &SSql::from_money(&p.refunded_money), &created_at, &updated_at]).await?;
            cnt += rv as u32;
        }
        txn.commit().await?;
        Ok(cnt)
    }

    pub async fn store_square_orders(&mut self, orders: &Vec<squareup::models::Order>) -> Result<u32> {
        let txn = self.client.transaction().await?;
        let mut cnt: u32 = 0;
        for o in orders {
            let created_at: chrono::DateTime<chrono::Utc> = o.created_at.as_ref().unwrap().clone().into();
            let updated_at: chrono::DateTime<chrono::Utc> = o.updated_at.as_ref().unwrap().clone().into();
            let closed_at: Option<chrono::DateTime<chrono::Utc>> = match &o.closed_at {
                None => None,
                Some(time) => Some(time.clone().into()),
            };
            let rv = txn.execute("INSERT INTO sqorder (order_id, customer_id, state, total_money, tax_money,
                                                    discount_money, tip_money, service_charge_money, created_at, updated_at, closed_at)
                                  VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                                  ON CONFLICT (order_id) DO UPDATE SET
                                  customer_id=EXCLUDED.customer_id, state=EXCLUDED.state, total_money=EXCLUDED.total_money,
                                  tax_money=EXCLUDED.tax_money, discount_money=EXCLUDED.discount_money, tip_money=EXCLUDED.tip_money,
                                  service_charge_money=EXCLUDED.service_charge_money, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at,
                                  closed_at=EXCLUDED.closed_at",
                                &[&o.id, &o.customer_id, &SSql::from_order_state(&o.state),
                                &SSql::from_money(&o.total_money), &SSql::from_money(&o.total_tax_money), &SSql::from_money(&o.total_discount_money),
                                &SSql::from_money(&o.total_tip_money), &SSql::from_money(&o.total_service_charge_money),
                                &created_at, &updated_at, &closed_at]).await?;
            cnt += rv as u32;
            if o.state == Some(OrderState::Completed) {
                if let Some(line_items) = &o.line_items {
                    for li in line_items {
                        if li.item_type != Some(squareup::models::enums::OrderLineItemItemType::Item) {
                            continue;
                        }
                        let qty = li.quantity.parse::<f64>()?;
                        let uid = Uuid::try_parse(li.uid.as_ref().unwrap().as_str())?;
                        let rv = txn.execute("INSERT INTO sqorderitem
                            (order_id, uid, squareup_id, quantity, base_unit_price)
                            VALUES($1, $2, $3, $4, $5)
                            ON CONFLICT (order_id, uid) DO NOTHING",
                            &[&o.id, &uid, &li.catalog_object_id, &Decimal::from_f64(qty), &SSql::from_money(&li.base_price_money)]).await?;
                        cnt += rv as u32;
                    }
                }

            }
        }
        txn.commit().await?;
        Ok(cnt)
    }

    pub async fn get_spend(&mut self, days: u32) -> Result<Vec<(Uuid, Decimal)>> {
        /* This query pull total spend for customers (by customer id) from itretail and
           joins that with the total spend from localexpress with a hopeful conversion of localexpress
           email address to (preferrably undeleted) itretail customer id. */
        let rows = self.client.query("select customer_id, sum(total) as total
  from
((select customer_id, sum(total) as total
                                from itrejtxn join customer using(customer_id)
	                            
                                where canceled = false
                                  and transaction_date > current_timestamp - ($1::integer * INTERVAL '1 days')
                                  and customer_id is not null
                                group by customer_id)
union
(select customer_id, sum(total) as total
  from leorder
  join (select customer_id, email as customer_email
	      from (select row_number() over (PARTITION BY email order by deleted) as rn, *
	              from customer
	             where email is not null and length(email) > 0)
	     where rn = 1) cm
 using (customer_email)
 where status in ('picked_up','delivered') and delivery_date > current_timestamp - ($1::integer * INTERVAL '1 days')
group by customer_id))
group by customer_id",
                                &[&(days as i32)]).await?;
        let vec = rows.iter().map(|x| (x.get(0), x.get::<usize,Decimal>(1))).collect();
        Ok(vec)
    }
}