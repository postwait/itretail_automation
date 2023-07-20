use clap::ArgMatches;
use fancy_regex::{Regex, RegexBuilder};
use rust_xlsxwriter::{Format, Workbook};
use std::error;
use log::*;

type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

pub struct LabelFile {
    label_file: String,
}

pub fn create_label_file(file: &String) -> LabelFile {
    LabelFile {
        label_file: file.to_string(),
    }
}

impl LabelFile {
    pub fn build_from_itretail_products(&mut self, json: &String, args: &ArgMatches) -> Result<()> {
        let items: Vec<super::api::ProductData> = serde_json::from_str(json)?;
        let items_iter = items.into_iter();
        // we only want items that are not deleted and weighed (002...)
        let re = args.get_one::<String>("upc").unwrap();
        let upc_pat = Regex::new(re)?;
        let re = args.get_one::<String>("name").unwrap();
        let name_pat = RegexBuilder::new(re).build()?;
        let vendor_id = args.get_one::<String>("vendor").unwrap().parse::<i32>().unwrap_or(0);
        let items = items_iter.filter(|x| {
            !x.deleted && upc_pat.is_match(&x.upc).unwrap() && name_pat.is_match(&x.description).unwrap() &&
            (vendor_id == 0 || (x.vendor_id.is_some() && vendor_id == x.vendor_id.unwrap()))
        });

        let mut workbook = Workbook::new();
        let bold_format = Format::new().set_bold();

        let worksheet = workbook.add_worksheet();
        worksheet.write_with_format(0, 0, "Name", &bold_format)?;
        worksheet.write_with_format(0, 1, "PLU", &bold_format)?;
        worksheet.write_with_format(0, 2, "UPC", &bold_format)?;
        worksheet.write_with_format(0, 3, "Price", &bold_format)?;

        let mut plu_assigned = 0;
        let mut row: u32 = 1;
        for item in items {
            worksheet.write_string(row, 0, &item.description)?;
            let plu = if item.plu.is_some() {
                item.plu.unwrap().parse::<u16>().unwrap()
            } else {
                plu_assigned = plu_assigned + 1;
                plu_assigned
            };
            worksheet.write_string(row, 1, plu.to_string())?;
            worksheet.write_string(row, 2, &item.upc)?;
            worksheet.write_string(row, 3, format!("{:.2}", item.normal_price))?;

            row = row + 1;
            info!(
                "Writing: [{}] {} : {} : {}",
                plu, item.upc, item.description, item.normal_price
            );
        }

        workbook.save(&self.label_file)?;

        Ok(())
    }
}
