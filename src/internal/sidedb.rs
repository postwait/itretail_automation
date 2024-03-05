use anyhow::Result;
use postgres::{Client, NoTls};
use rust_decimal::prelude::*;
use chrono::NaiveDateTime;


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
    pub fn store_orders<'a, I>(&mut self, orders: I) -> Result<u32>
    where
        I: Iterator<Item = &'a super::localexpress::Order>,
    {

        let mut txn = self.client.transaction()?;
        let mut cnt = 0;
        txn.execute("DELETE FROM public.leorder", &[])?;
        for o in orders {
            let cd = o.delivery_time_period.split(" - ").collect::<Vec<&str>>();
            let (st, et) = if cd.len() == 2 { (cd[0], cd[1]) }
            else { ("00:00","23:59") };
            let dd = o.delivery_date.format("%Y-%m-%d").to_string();
            let (sd,ed) =
                (NaiveDateTime::parse_from_str(&format!("{}T{}:00", dd, st), "%Y-%m-%dT%H:%M:%S")?,
                NaiveDateTime::parse_from_str(&format!("{}T{}:00", dd, et),"%Y-%m-%dT%H:%M:%S")?);
            txn.execute("INSERT INTO public.leorder
                           (id, uniqid, store_id, status,
                            subtotal, tips, total,
                            mode, payment_method, customer_first_name, customer_last_name,
                            customer_phone_number, customer_email, creation_date, delivery_date, delivery_time_period)
                            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,tsrange($16,$17))",
                    &[&(o.id as i64), &o.uniqid, &(o.store_id as i64), &o.status,
                      &decimal_price(&o.subtotal), &decimal_price(&o.tips), &decimal_price(&o.total),
                      &o.mode, &o.payment_method, &o.customer_first_name, &o.customer_last_name,
                      &o.customer_phone_number, &o.customer_email, &o.creation_date, &o.delivery_date, &sd, &ed])?;
            cnt += 1;
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
                txn.execute("INSERT INTO public.itrproduct
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
                txn.execute("INSERT INTO public.itrproduct
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
}