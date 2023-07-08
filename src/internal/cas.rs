use anyhow::{Result};
use std::{collections::HashSet};
use clap::ArgMatches;
use fancy_regex::{Regex};
use rust_xlsxwriter::{Format, Workbook};
use chrono::Local;
use itertools::Itertools;

use super::api::PLUAssignment;

pub struct ScaleFile {
    scale_file: String,
}

pub fn create_scale_file(file: &String) -> ScaleFile {
  ScaleFile{
    scale_file: file.to_string()
  }
}

static INGREDIENT_LABEL_ID: u32 = 62;
const FIELDS : [&str; 19] = ["Department No", "PLU No", "Name1", "Name2", "Itemcode",
                             "Unit Price", "Origin No", "Label No"," Category No",
                             "Direct Ingredient", "Sell By Time", "Sell By Date",
                             "Packed Date", "Group No", "Unit Weight", "Nutrifact No",
                             "PLU Type", "Packed Time", "Update Date"];

fn wrong_range(item: &super::api::ProductData, plu: u16) -> bool {
    (item.description.starts_with("(I)") && plu >= 1000) ||
    (!item.description.starts_with("(I)") && plu < 1000)
}
fn next_plu(hs: &mut HashSet<u16>, item: &super::api::ProductData) -> u16 {
    let mut probe: u16 = if item.description.starts_with("(I)") {
        1
    } else {
        1001
    };
    while hs.contains(&probe) {
        probe = probe + 1;
    }
    hs.insert(probe);
    probe
}
impl ScaleFile {
    pub fn build_from_itretail_products(&mut self, api: &mut super::api::ITRApi, args: &ArgMatches) -> Result<()> {
        let dump_internal = args.get_flag("internal");
        let re = args.get_one::<String>("upc").unwrap();
        let upc_pat = Regex::new(re)?;
        let filter = |x: &super::api::ProductData| { !x.deleted && upc_pat.is_match(&x.upc).unwrap() };

        let json = api.get(&"/api/ProductsData/GetAllProducts".to_string()).expect("no results from API call");
        let mut items: Vec<super::api::ProductData> = serde_json::from_str(&json)?;
        items = items.into_iter().filter(filter).sorted_by_key(|x| x.section_id.unwrap_or(0)).collect::<Vec<super::api::ProductData>>();

        let mut existing_plu = HashSet::<u16>::new();
        let mut seen_plu = HashSet::<u16>::new();
        let mut plu_assignment: Vec<PLUAssignment> = Vec::new();
        for item in &items {
            if item.plu.is_some() {
                let plu = item.plu.as_ref().unwrap().parse::<u16>().unwrap();
                existing_plu.insert(plu);
            }
        }
        for item in &items {
            if item.plu.is_some() {
                let plu = item.plu.as_ref().unwrap().parse::<u16>().unwrap();
                if seen_plu.contains(&plu) || wrong_range(&item, plu) {
                     let new_plu = next_plu(&mut existing_plu, &item);
                     println!("PLU assigned {} bad previous was {} - {}", new_plu, plu, item.description);
                     plu_assignment.push(PLUAssignment{ upc: item.upc.to_string(), plu: new_plu });
                    seen_plu.insert(new_plu);
                } else {
                    seen_plu.insert(plu);
                }
            }
            else {
                let new_plu = next_plu(&mut existing_plu, &item);
                plu_assignment.push(PLUAssignment{ upc: item.upc.to_string(), plu: new_plu });
                println!("PLU assigned {} - {}", new_plu, item.description);
                seen_plu.insert(new_plu);
            }
        }
        if plu_assignment.len() > 0 {
            let r = api.set_plu(plu_assignment);
            if r.is_err() {
                return Err(r.err().unwrap());
            }
            println!("{}", r.unwrap());
            let json = api.get(&"/api/ProductsData/GetAllProducts".to_string()).expect("no results from API call");
            items = serde_json::from_str(&json)?;
            items = items.into_iter().filter(filter).sorted_by_key(|x| x.section_id.unwrap_or(0)).collect::<Vec<super::api::ProductData>>();
        }

        let weighed_items = items.into_iter();

        let mut workbook = Workbook::new();
        let bold_format = Format::new().set_bold();
        let decimal_format = Format::new().set_num_format("0.00");
        let date_format = Format::new().set_num_format("yyyy-mm-dd");

        let date = Local::now().naive_local();

        let worksheet = workbook.add_worksheet();
        for idx in 0..FIELDS.len()-1 {
            worksheet.write_with_format(0, idx.try_into().unwrap(), FIELDS[idx], &bold_format)?;
        }

        let mut plu_assigned: u16 = 0;
        let mut row: u32 = 1;
        for item in weighed_items {
            worksheet.write_number(row, 0, item.department_id)?;
            let plu = 
                if item.plu.is_some() {
                    item.plu.unwrap().parse::<u16>().unwrap()
                } else {
                    plu_assigned = plu_assigned + 1;
                    plu_assigned
                };
            if !dump_internal && plu < 1000 {
                continue;
            }
            worksheet.write_number(row, 1, plu)?;
            worksheet.write_string(row, 2, &item.description)?;
            // 3 Name2 (blank)
            let itemcode_str = item.upc.get(3..8);
            if itemcode_str.is_none() {
                println!("Bad UPC: {}", item.upc);
                continue;
            }
            let itemcode = itemcode_str.unwrap().trim_start_matches('0').parse::<u32>().or::<u32>(Ok(0)).unwrap();
            

            worksheet.write_number(row, 4, itemcode)?;
            worksheet.write_number_with_format(row, 5, item.normal_price, &decimal_format)?;
            worksheet.write_number(row, 6, 0)?; // Origin
            worksheet.write_number(row, 7, 0)?; // Label ID
            worksheet.write_number(row, 8, 0)?; // Category
            if item.second_description.is_some() {
                let ingredients = item.second_description.unwrap();
                if ingredients.len() > 0 {
                    worksheet.write_number(row, 7, INGREDIENT_LABEL_ID)?; // Label ID
                    worksheet.write_string(row, 9, ingredients)?; // Direct Ingredient
                }
            }
            worksheet.write_number(row, 10, 0)?; // Sell by Time
            worksheet.write_number(row, 11, 0)?; // Sell by Date
            worksheet.write_number(row, 12, 0)?; // Packed Date
            worksheet.write_number(row, 13, 0)?; // Group No
            worksheet.write_number(row, 14, 1)?; // Unit Weight (1 lb)
            worksheet.write_number(row, 15, 0)?; // Nutrifact No
            worksheet.write_number(row, 16, 1)?; // PLU Type: 1 - weighed
            worksheet.write_number(row, 17, 0)?; // Packed Time
            worksheet.write_with_format(row, 18, &date, &date_format)?;

            row = row + 1;
            println!("Writing: [{}] {} : {} : {}", plu, item.upc, item.description, item.normal_price);
        }

        workbook.save(&self.scale_file)?;

        Ok(())
    }
}