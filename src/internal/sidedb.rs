use anyhow::Result;
use postgres::{Client, NoTls};
use rust_decimal::prelude::*;
use chrono::{NaiveDate, NaiveDateTime};
use log::*;
use uuid::Uuid;
use std::collections::HashMap;

use super::api::{Customer, ProductData};

pub struct SideDb {
    client: Client,
}

pub fn make_sidedb(settings: &super::settings::Settings) -> Result<SideDb> {
    let client = Client::connect(&settings.postgres.connect_string, NoTls)?;
    Ok(SideDb{client: client})
}

fn decimal_price(a: &str) -> Decimal {
    Decimal::from_str(a.strip_prefix("$").unwrap_or("0")).unwrap()
}
fn some_f32_to_some_decimal(a: &Option<f32>) -> Option<Decimal> {
    if a.is_none() { None }
    else { Decimal::from_f32(a.unwrap()) }
}
impl SideDb {
    pub fn store_txns<'a, I>(&mut self, txns: I) -> Result<u32>
    where
        I: Iterator<Item = &'a super::api::EJTxn>
    {
        let mut sqltxn = self.client.transaction()?;
        let mut cnt = 0;
        for t in txns {
            let td = NaiveDateTime::parse_from_str(&t.transaction_date, "%Y-%m-%dT%H:%M:%S%.f")?;
            let num_rows = sqltxn.execute("INSERT INTO itrejtxn (transaction_id, customer_id, transaction_date, canceled, total)
            VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
            &[&t.id, &t.customer_id, &td, &t.canceled, &Decimal::from_f64(t.total)])?;
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
                          &Decimal::from_f64(p.price), &Decimal::from_f64(p.line_discount), &p.quantity, &p.weight])?;
                    }
                }
                cnt += 1;
            }
        }
        sqltxn.commit()?;
        Ok(cnt)
    }
    pub fn store_customers<'a, I>(&mut self, customers: I) -> Result<u32>
    where
        I: Iterator<Item = super::api::Customer>,
    {
        let existing = { self.get_customers()? };
        let mut to_delete: HashMap<Uuid, &Customer> = HashMap::new();
        for c in existing.iter() {
           to_delete.insert(c.id, &c);
        }
        let total_db_size = to_delete.len() as f64;

        let mut txn = { self.client.transaction()? };
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
                                  &(c.frequent_shopper.unwrap_or(false)),&Decimal::from_f64(c.cash_back.unwrap_or(0.0)),&inc])?;
            cnt = cnt + re as u32;
        }
        txn.commit()?;
        if to_delete.len() as f64 / total_db_size > 0.02 {
            error!("We want to delete {} customers out of {}, that's scary high. You'll need to do that manually.",
                   to_delete.len(), total_db_size);
        }
        else {
            info!("Marking {} customers as deleted.", to_delete.len());
            for (id, c) in to_delete {
                info!("Marking {} ({} {} {} {}) as deleted.", id, c.first_name, c.last_name, c.email.as_ref().unwrap_or(&"n/a".to_string()), c.phone.as_ref().unwrap_or(&"n/a".to_string()));
                self.delete_customer(&id);
            }
        }
        Ok(cnt)
    }
    pub fn delete_customer(&mut self, id: &Uuid) -> Result<bool> {
        let rc = self.client.execute("UPDATE customer SET deleted=true WHERE customer_id = $1", &[id])?;
        Ok(rc > 0)
    }
    pub fn get_customer_household(&mut self) -> Result<Vec<(Uuid, Uuid)>> {
        let rows = self.client.query("SELECT main, resident FROM customer_house", &[])?;
        let rels = rows.iter().map(|x| { (x.get("main"), x.get("resident")) }).collect();
        Ok(rels)
    }
    pub fn get_customers(&mut self) -> Result<Vec<Customer>> {
        let rows = self.client.query("SELECT * FROM customer WHERE NOT deleted", &[])?;
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
            }
        }).collect();
        Ok(customers)
    }
    pub fn store_orders<'a, I>(&mut self, orders: I) -> Result<u32>
    where
        I: Iterator<Item = &'a super::localexpress::Order>,
    {

        let mut txn = self.client.transaction()?;
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
                      &o.customer_phone_number, &o.customer_email, &o.creation_date, &o.delivery_date, &sd, &ed])?;
            cnt += re as u32;
        }
        txn.commit()?;
        Ok(cnt)
    }

    pub fn store_products<'a, I>(&mut self, products: I) -> Result<u32>
    where
        I: Iterator<Item = &'a super::api::ProductData>,
    {
        let mut txn = self.client.transaction()?;
        let mut cnt = 0;
        txn.execute("WITH D as (DELETE FROM itrproduct RETURNING *) INSERT INTO itrproduct_archive SELECT * FROM D ON CONFLICT DO NOTHING", &[])?;
        for p in products {
            if p.special_price.is_some() && p.start_date.is_some() && p.end_date.is_some() {
                txn.execute("INSERT INTO itrproduct
                            (upc, description, second_description, normal_price, special_price, special_date,
                             scale, active, deleted, discount, plu, cert_code, vendor_id, department_id, section_id,
                             wicable, foodstamp, quantity_on_hand, size, case_cost, pack, cost)
                        VALUES($1,$2,$3,$4,$5,tsrange($6,$7),$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)",
                        &[&p.upc, &p.description, &p.second_description, &Decimal::from_f64(p.normal_price),
                        &Decimal::from_f64(p.special_price.unwrap()),
                        &NaiveDateTime::parse_from_str(p.start_date.as_ref().unwrap(), "%Y-%m-%dT%H:%M:%S")?, &NaiveDateTime::parse_from_str(p.end_date.as_ref().unwrap(), "%Y-%m-%dT%H:%M:%S")?,
                        &p.scale, &p.active, &p.deleted, &(p.discountable != 0), &p.plu, &p.cert_code, &p.vendor_id, &p.department_id, &p.section_id,
                        &p.wicable, &p.foodstamp, &(p.quantity_on_hand.unwrap_or(0.0) as f64), &p.size, &some_f32_to_some_decimal(&p.case_cost), &p.pack, &some_f32_to_some_decimal(&p.cost)
                        ])?;

            }
            else {
                txn.execute("INSERT INTO itrproduct
                            (upc, description, second_description, normal_price,
                             scale, active, deleted, discount, plu, cert_code, vendor_id, department_id, section_id,
                             wicable, foodstamp, quantity_on_hand, size, case_cost, pack, cost)
                        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)",
                        &[&p.upc, &p.description, &p.second_description, &Decimal::from_f64(p.normal_price),
                        &p.scale, &p.active, &p.deleted, &(p.discountable != 0), &p.plu, &p.cert_code, &p.vendor_id, &p.department_id, &p.section_id,
                        &p.wicable, &p.foodstamp, &(p.quantity_on_hand.unwrap_or(0.0) as f64), &p.size,
                        &some_f32_to_some_decimal(&p.case_cost), &p.pack, &some_f32_to_some_decimal(&p.cost)
                        ])?;
            }
            cnt += 1;
        }
        txn.commit()?;
        Ok(cnt)
    }
    pub fn get_products(&mut self, date: Option<&NaiveDate>) -> Result<Vec<ProductData>> {
        let rows = if date.is_some() {
            let dr = date.unwrap();
            self.client.query("SELECT *, lower(special_date) as start_date, upper(special_date) as end_date
                FROM itrproduct_archive
                WHERE NOT deleted and date(timezone('US/Eastern',recorded_at)) = $1
                ORDER BY department_id, section_id", &[dr])
        } else {
            self.client.query("SELECT *, lower(special_date) as start_date, upper(special_date) as end_date
                FROM itrproduct
                WHERE NOT deleted
                ORDER BY department_id, section_id", &[])
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
                cost: x.get::<&str,Option<Decimal>>("cost").and_then(|x| x.to_f32()) }
        }).collect();
        Ok(products)
    }

    pub fn get_spend(&mut self, days: u32) -> Result<Vec<(Uuid, Decimal)>> {
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
                                &[&(days as i32)])?;
        let vec = rows.iter().map(|x| (x.get(0), x.get::<usize,Decimal>(1))).collect();
        Ok(vec)
    }
}