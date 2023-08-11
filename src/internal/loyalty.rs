use std::collections::HashMap;

use anyhow::Result;
use clap::ArgMatches;
use log::*;

pub fn spend_180_to_discount(spend: f64) -> u8 {
    match spend {
        t if t > 12400.0 => 14,
        t if t > 10400.0 => 12,
        t if t > 8600.0 => 11,
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

pub fn apply_discounts(
    api: &mut super::api::ITRApi,
    _settings: &super::settings::Settings,
    args: &ArgMatches,
) -> Result<()> {
    let days = args.get_one::<u32>("days").unwrap();
    let normalize = (*days as f64) / 180.0;
    let txn_vec = api.get_transactions(*days as u64)?;
    let customer_vec = api.get_customers()?;
    let mut customers = HashMap::new();
    for c in customer_vec.iter() {
        customers.insert(c.id.clone(), c);
    }
    let mut txn_totals: HashMap<String, f64> = HashMap::new();
    for t in txn_vec.iter().filter(|x| !x.canceled) {
        if let Some(cid) = &t.customer_id {
            if let Some(rec) = txn_totals.get_mut(cid) {
                *rec += t.total;
            } else {
                txn_totals.insert(cid.clone(), t.total);
            }
        }
    }
    let mut changes = 0;
    let mut inc = 0;
    for (cid, customer) in &customers {
        let spend = txn_totals.get(cid).unwrap_or(&0.0);
        let discount = spend_180_to_discount(*spend / normalize);
        let existing_discount = customer.discount.unwrap_or(0);
        if existing_discount != discount {
            changes += 1;
            if discount > existing_discount {
                inc += 1;
            }
            debug!(
                "{} / {} -> ${:.02} (normalized ${:.02}) ({}% -> {}%)",
                customer.id,
                customer
                    .email
                    .as_ref()
                    .unwrap_or(customer.phone.as_ref().unwrap_or(&"no id".to_owned())),
                spend,
                *spend / normalize,
                existing_discount,
                discount
            );
            if customer
                .email
                .as_ref()
                .unwrap_or(&"".to_owned())
                .ne("jesus@lethargy.org")
            {
                continue;
            }
            let mut newc = api.get_customer(&customer.id)?; // this is needed b/c our customer is skeletal
            newc.discount = Some(discount);
            let r = api.update_customer(&newc);
            if r.is_err() {
                warn!(
                    "Error updating IT Retail discount for {}: {}",
                    cid,
                    r.err().unwrap()
                );
            }
        }
    }
    info!(
        "{} customers changed loyalty status, {} increased.",
        changes, inc
    );

    Ok(())
}
