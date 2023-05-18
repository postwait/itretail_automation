use anyhow::{Result};
use image::{Rgba};
use imageproc::drawing::{draw_text_mut, text_size};
use rusttype::{Font, Scale};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use lazy_static::lazy_static;

lazy_static! {
    static ref DEFAULT_BACKDROP: image::ImageBuffer<Rgba<u8>, Vec<u8>> = image::load_from_memory(include_bytes!("../assets/backdrop.png")).unwrap().into_rgba8();
}

pub fn make_listing(output_file: &str, json: &String) -> Result<()> {
    let items: Vec<super::api::ProductData> = serde_json::from_str(json)?;
    let items_iter = items.into_iter();
    let weighed_items = items_iter.filter(|x| { !x.deleted && x.upc.starts_with("002") });
    let mut menu_file = OpenOptions::new().write(true).create(true).truncate(true).open(output_file).expect("Could not open menu file");
    for item in weighed_items {
       menu_file.write(&format!("{} = ${:.2}/lb\r\n", item.description, item.normal_price).as_bytes()).expect("writing menu item");
    }
    menu_file.sync_all().expect("saving menu file");
    Ok(())
}
pub fn make_menu(output_file: &str, menu: &String, backdrop: Option<&String>, invert: bool) -> Result<()> {
    let path = Path::new(output_file);

    let mut image = match backdrop {
        Some(filename) => { image::open(filename).unwrap().into_rgba8() },
        None => DEFAULT_BACKDROP.clone()
    };

    let font = Vec::from(include_bytes!("C:\\Windows\\Fonts\\MAIAN.TTF") as &[u8]);
    let font = Font::try_from_vec(font).unwrap();

    let height = 60.0;
    let scale = Scale {
        x: height,
        y: height,
    };

    let dot_padding = 100;
    let image_width: i32 = image.width().try_into().unwrap();
    let image_height: i32 = image.height().try_into().unwrap();
    let gutter = 220;
    let title_outstep = 40;
    let footer = 80;
    let header = 100;
    let mut y = header;
    let dot_w = {
        let (w, _) = text_size(scale, &font, &".".repeat(10));
        w / 10 
    };
    for line in menu.lines() {
        if let Some((name, price)) = line.split_once("=") {
            let (name_w, name_h) = text_size(scale, &font, name);
            let (price_w, price_h) = text_size(scale, &font, price);
            let max_h = name_h.max(price_h);
            if y + max_h > image_height - footer {
                break;
            }
            let room = image_width - (2 * gutter + name_w + price_w + 2 * dot_padding);

            let dot_count: i32 = (room / dot_w).try_into().unwrap();
            if dot_count < 0 {
                println!("Line too long: {} ... {}", name, price);
                continue;
            }
            let dots_str = ".".repeat(dot_count as usize);
            let (dots_w, _) = text_size(scale, &font, &dots_str);
            draw_text_mut(&mut image, Rgba([0u8, 0u8, 0u8, 255u8]), gutter, y, scale, &font, name);
            draw_text_mut(&mut image, Rgba([120u8, 120u8, 120u8, 255u8]), image_width - gutter - price_w - dots_w, y, scale, &font, &dots_str);
            draw_text_mut(&mut image, Rgba([0u8, 0u8, 0u8, 255u8]), image_width - gutter - price_w, y, scale, &font, price);
        } else {
            draw_text_mut(&mut image, Rgba([0u8, 0u8, 0u8, 255u8]), gutter - title_outstep, y, scale, &font, line);
            let (w, h) = text_size(scale, &font, line);
            println!("Text size: {}x{}", w, h);
        }
        y = y + (height as i32);
    }

    if invert {
         image::imageops::colorops::invert(& mut image);
    }
    let result = image.save(path);
    if result.is_err() {
        return Err(result.err().unwrap().into())
    }
    Ok(())
}