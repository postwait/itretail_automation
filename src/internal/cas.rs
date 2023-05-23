use std::error;
use rust_xlsxwriter::{Format, Workbook};
use chrono::Local;

type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

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

impl ScaleFile {
    pub fn build_from_itretail_products(&mut self, json: &String) -> Result<()> {
        let items: Vec<super::api::ProductData> = serde_json::from_str(json)?;
        let items_iter = items.into_iter();
        // we only want items that are not deleted and weighed (002...)
        let weighed_items = items_iter.filter(|x| { !x.deleted && x.upc.starts_with("002") });

        let mut workbook = Workbook::new();
        let bold_format = Format::new().set_bold();
        let decimal_format = Format::new().set_num_format("0.00");
        let date_format = Format::new().set_num_format("yyyy-mm-dd");

        let date = Local::now().naive_local();

        let worksheet = workbook.add_worksheet();
        for idx in 0..FIELDS.len()-1 {
            worksheet.write_with_format(0, idx.try_into().unwrap(), FIELDS[idx], &bold_format)?;
        }

        let mut plu_assigned = 0;
        let mut row: u32 = 1;
        for item in weighed_items {
            worksheet.write_number(row, 0, item.department_id)?;
            let plu = 
                if item.cert_code.is_some() {
                    item.cert_code.unwrap().parse::<u16>().unwrap()
                }
                else if item.plu.is_some() {
                    item.plu.unwrap().parse::<u16>().unwrap()
                } else {
                    plu_assigned = plu_assigned + 1;
                    plu_assigned
                };
            worksheet.write_number(row, 1, plu)?;
            worksheet.write_string(row, 2, &item.description)?;
            // 3 Name2 (blank)
            let itemcode_str = item.upc.get(3..8);
            if itemcode_str.is_none() {
                println!("Bad UPC: {}", item.upc);
                continue;
            }
            let itemcode = itemcode_str.unwrap().trim_start_matches('0').parse::<u32>()?;
            

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