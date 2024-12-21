use std::collections::HashMap;

use anyhow::Result;
use clap::ArgMatches;
use log::*;
use rust_decimal::prelude::*;
use uuid::Uuid;

pub fn valid_loyalty_levels() -> Vec<u32> {
    vec![3,4,5,6,7,8,9,10]
}

pub fn spend_180_to_discount(spend: f64) -> u8 {
    match spend {
        /*
        Consider these.
        t if t > 12400.0 => 14,
        t if t > 10400.0 => 12,
        t if t > 8600.0 => 11,
        */
        t if t > 7000.0 => 10,
        t if t > 5600.0 => 9,
        t if t > 4200.0 => 8,
        t if t > 3000.0 => 7,
        t if t > 2000.0 => 6,
        t if t > 1200.0 => 5,
        t if t > 600.0 => 4,
        t if t > 300.0 => 3,
        _ => 0,
    }
}

pub async fn apply_discounts(
    api: &mut super::api::ITRApi,
    sidedb: &mut super::sidedb::SideDb,
    _settings: &super::settings::Settings,
    args: &ArgMatches,
) -> Result<()> {
    let days = args.get_one::<u32>("days").unwrap();
    let customer = args.get_one::<String>("email");
    let noop = args.get_one::<bool>("noop").unwrap();
    let normalize = (*days as f64) / 180.0;
    let mut hoh_lookup: HashMap<Uuid,Uuid> = HashMap::new();
    for hoh in sidedb.get_customer_household().await? {
        hoh_lookup.insert(hoh.1, hoh.0);
    }
    let spend_vec = sidedb.get_spend(*days).await?;
    let customer_vec = sidedb.get_customers().await?;
    let mut customers = HashMap::new();
    for c in customer_vec.iter() {
        if customer.is_none() || (c.email.is_some() && c.email.as_ref().unwrap() == customer.unwrap()) {
            customers.insert(c.id.clone(), c);
        }
    }
    let mut txn_totals: HashMap<Uuid, f64> = HashMap::new();
    for t in spend_vec.iter() {
        let hoh = match hoh_lookup.get(&t.0) {
            Some(head) => head,
            None => &t.0
        };
        if *hoh != t.0 {
            info!("pushing {}'s {} to heah of household {}", t.0, t.1, hoh);
        }
        if let Some(rec) = txn_totals.get_mut(hoh) {
            *rec += t.1.to_f64().unwrap();
        } else {
            txn_totals.insert(hoh.clone(), t.1.to_f64().unwrap());
        }
    }
    let mut changes = 0;
    let mut inc = 0;
    let mut del = 0;
    for (cid, customer) in &customers {
        let hoh = match hoh_lookup.get(cid) {
            Some(head) => head,
            None => cid
        };
        let spend = txn_totals.get(hoh).unwrap_or(&0.0);
        let loyalty_points = (*spend / normalize).round() as i32;
        let discount = spend_180_to_discount(*spend / normalize);
        let existing_discount = customer.discount.unwrap_or(0);
        let existing_loyalty_points = customer.loyalty_points.unwrap_or(0);
        if existing_discount != discount || existing_loyalty_points != loyalty_points {
            if existing_discount != discount {
                changes += 1;
            }
            if discount > existing_discount {
                inc += 1;
            }
            debug!(
                "{} / {} -> ${:.02} (normalized ${:.02}) (LP: {} -> {}) ({}% -> {}%)",
                customer.id,
                customer
                    .email
                    .as_ref()
                    .unwrap_or(customer.phone.as_ref().unwrap_or(&"no id".to_owned())),
                spend,
                *spend / normalize,
                existing_loyalty_points,
                loyalty_points,
                existing_discount,
                discount
            );
            if *noop {
                continue;
            }
            if let Ok(mut newco) = api.get_customer(&customer.id).await {
                if newco.is_none() {
                    // user is deleted
                    info!(
                        "Customer {} / {} no longer in IT Retail",
                        customer.id,
                        customer
                            .email
                            .as_ref()
                            .unwrap_or(customer.phone.as_ref().unwrap_or(&"no id".to_owned()))
                    );

                    let dr = sidedb.delete_customer(&customer.id).await;
                    if dr.is_ok() && dr.unwrap() {
                        info!("Marked {} as deleted.", customer.id);
                        del += 1;
                    }
                    continue;
                }
                let newc = newco.as_mut().unwrap();
                // this is needed b/c our customer is skeletal
                newc.discount = Some(discount);
                newc.loyalty_points = Some(loyalty_points);
                let r = api.update_customer(&newc).await;
                if r.is_err() {
                    warn!(
                        "Error updating IT Retail discount for {}: {}",
                        cid,
                        r.err().unwrap()
                    );
                }
            }
        }
    }
    info!(
        "{} customers changed loyalty status, {} increased, {} deleted.",
        changes, inc, del
    );

    Ok(())
}
