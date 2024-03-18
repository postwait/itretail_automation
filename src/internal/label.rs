use clap::ArgMatches;
use fancy_regex::{Regex, RegexBuilder};
use log::*;
use rust_xlsxwriter::{Format, Workbook};
//use std::error;
use anyhow::{anyhow, Result};

//type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

pub struct LabelFile {
    label_file: String,
}

pub fn create_label_file(file: &String) -> LabelFile {
    LabelFile {
        label_file: file.to_string(),
    }
}

impl LabelFile {
    pub fn output_from_itretail_products(&mut self, json: &String, args: &ArgMatches) -> Result<()> {
        let items: Vec<super::api::ProductData> = serde_json::from_str(json)?;
        let items_iter = items.into_iter();
        // we only want items that are not deleted and weighed (002...)
        let re = args.get_one::<String>("upc").unwrap();
        let upc_pat = Regex::new(re)?;
        let qlimit = args.get_one::<f32>("at-least").unwrap();
        let re = args.get_one::<String>("name").unwrap();
        let name_pat = RegexBuilder::new(re).build()?;
        let vendor_id = args
            .get_one::<String>("vendor")
            .unwrap()
            .parse::<i32>()
            .unwrap_or(0);
        let items = items_iter.filter(|x| {
            let wanted = !x.deleted
                && upc_pat.is_match(&x.upc).unwrap()
                && name_pat.is_match(&x.description).unwrap()
                && (vendor_id == 0 || (x.vendor_id.is_some() && vendor_id == x.vendor_id.unwrap()));
            wanted && (x.quantity_on_hand.unwrap_or(0.0) > *qlimit)
        });

        let mut row: u32 = 1;
        for item in items {
            let plu = if item.plu.is_some() {
                let _p = item.plu.unwrap().parse::<u16>().unwrap();
                _p
            } else {
                0
            };

            row = row + 1;
            println!(
                "[PLU {}] {} : {} : {}",
                plu, item.upc, item.description, item.normal_price
            );
        }

        Ok(())
    }
    pub fn build_from_itretail_products(&mut self, items: &Vec<super::api::ProductData>, args: &ArgMatches) -> Result<()> {
        let items_iter = items.into_iter();
        // we only want items that are not deleted and weighed (002...)
        let re = args.get_one::<String>("upc").unwrap();
        let upc_pat = Regex::new(re)?;
        let qlimit = args.get_one::<f32>("at-least").unwrap();
        let re = args.get_one::<String>("name").unwrap();
        let name_pat = RegexBuilder::new(re).build()?;
        let use_sheets = *args.get_one::<bool>("sheets").unwrap();
        let headers = args.get_one::<String>("headers").unwrap().split(',').collect::<Vec<&str>>();
        let vendor_id = args
            .get_one::<String>("vendor")
            .unwrap()
            .parse::<i32>()
            .unwrap_or(0);
        let items = items_iter.filter(|x| {
            let wanted = !x.deleted
                && upc_pat.is_match(&x.upc).unwrap()
                && name_pat.is_match(&x.description).unwrap()
                && (vendor_id == 0 || (x.vendor_id.is_some() && vendor_id == x.vendor_id.unwrap()));
            wanted && (x.quantity_on_hand.unwrap_or(0.0) > *qlimit)
        });

        let mut workbook = Workbook::new();
        let bold_format = Format::new().set_bold();
        let weight_format = Format::new().set_num_format("0.000");
        let price_format = Format::new().set_num_format_index(7);

        let mut worksheet = workbook.add_worksheet();
        let mut row: u32 = 1;
        let mut last_sheet = (-1, None);
        for item in items {
            let mut cidx = 0;
            if last_sheet.0 == -1 {
                last_sheet = (item.department_id,item.section_id);
            }
            if use_sheets && last_sheet != (item.department_id,item.section_id) {
                last_sheet = (item.department_id,item.section_id);
                println!("Adding worksheet: {:?}", last_sheet);
                worksheet = workbook.add_worksheet();
                row = 1;
            }
            if row == 1 {
                worksheet.set_name(&format!("{}-{}", item.department_id, item.section_id.and_then(|x| Some(x.to_string())).or(Some("None".to_string())).unwrap()))?;
                for h in headers.iter() {
                    worksheet.set_column_width(cidx,
                        match h.to_lowercase().as_str() {
                            "upc" => 16,
                            "name" => 40,
                            _ => 8
                        })?;
                    worksheet.write_with_format(0, cidx, *h, &bold_format)?;
                    cidx += 1;
                }
            }
            cidx = 0;
            debug!("{:?}", item);
            let mut plu = None;
            for h in headers.iter() {
                match h.to_lowercase().as_str() {
                    "name" => {
                        worksheet.write_string(row, cidx, &item.description)?;
                    },
                    "plu" => {
                        plu = if item.plu.is_some() {
                            let _p = item.plu.as_ref().unwrap().parse::<u16>().unwrap();
                            worksheet.write_string(row, cidx, _p.to_string())?;
                            Some(_p)
                        } else {
                            None
                        };
                    },
                    "upc" => {
                        worksheet.write_string(row, cidx, &item.upc)?;
                    },
                    "price" => {
                        worksheet.write_number_with_format(row, cidx, item.normal_price, &price_format)?;
                    },
                    "qoh" => {
                        worksheet.write_number_with_format(row, cidx, item.quantity_on_hand.unwrap_or(0.0), &weight_format)?;
                    },
                    _ => {
                        return Err(anyhow!("Unknown header: {}", h))
                    }
                }
                cidx += 1;
            }
            row = row + 1;
            debug!(
                "Writing: [{:?}] {} : {} : {}",
                plu, item.upc, item.description, item.normal_price
            );
        }

        workbook.save(&self.label_file)?;

        Ok(())
    }
}
