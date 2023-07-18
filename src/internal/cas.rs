use anyhow::Result;
use std::collections::HashSet;
use std::path::{Path,PathBuf};
use std::ffi::{CStr, CString};
use clap::ArgMatches;
use fancy_regex::Regex;
use rust_xlsxwriter::{Format, Workbook};
use chrono::Local;
use itertools::Itertools;
use libloading::os::windows::Library;
use process_path::get_executable_path;
use cty;

use super::api::{PLUAssignment, ProductData};

type LPSTR = * const cty::c_uchar;
//type MUTLPSTR = * mut cty::c_uchar;
type WORD = u16;
type DWORD = u32;
type BYTE = u8;
const CL_INTERP: &[u8; 18] = b"CLInterpreter.dll\0";
const CL_JRINTERP: &[u8; 20] = b"CLJRInterpreter.dll\0";
const TD_LIB_SETCOMMLIB: &[u8; 15] = b"SetCommLibrary\0";
const TD_LIB_ADDINTERPRETER: &[u8; 15] = b"AddInterpreter\0";
const TD_LIB_ADDCONNECTIONEX: &[u8; 16] = b"AddConnectionEx\0";
const TD_LIB_CONNECT: &[u8; 8] = b"Connect\0";
const DF_ACTION_NOTIFY: u8 = 27;
const DF_COMMTYPE_TCPIP: u8 = 1;
const DF_MODULE_TCPIP: i32 = 1;
const DF_SCALE_TYPE_LP: u16 = 100;
const DF_SCALE_CL3500: u16 = 3500;
const DF_SCALE_CL5000: u16 = 5000;
const DF_SCALE_CL5000JR: u16 = 5010;
const DF_SCALE_CL5200: u16 = 5200;
const DF_SCALE_CL5500: u16 = 5500;
const DF_SCALE_CL7200: u16 = 7200;
const DF_TRANS_TIMEOUT: u16 = 3000;
const DF_TRANS_RETRYCOUNT: u16 = 3;
const DF_TRANSTYPE_PROC: u8 = 0;


#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug)]
pub struct TD_ST_TRANSDATA_V02 {
    #[allow(non_snake_case)]
    shScaleID: cty::c_short,
    #[allow(non_snake_case)]
    lpIP: LPSTR,
    #[allow(non_snake_case)]
    btCommType: BYTE,
    #[allow(non_snake_case)]
    btSendType: BYTE,
    #[allow(non_snake_case)]
    btDataType: BYTE,
    #[allow(non_snake_case)]
    wdScaleType: WORD,
    #[allow(non_snake_case)]
    wdScaleModel: WORD,
    #[allow(non_snake_case)]
    wdAction: WORD,
    #[allow(non_snake_case)]
    wdDataSize: WORD,
    #[allow(non_snake_case)]
    pData: * mut cty::c_void,
    #[allow(non_snake_case)]
    dwScaleMainVersion: DWORD,
    #[allow(non_snake_case)]
    dwScaleSubVersion: DWORD,
    #[allow(non_snake_case)]
    dwScaleCountry: DWORD,
    #[allow(non_snake_case)]
    dwScaleDataVersion: DWORD,
    #[allow(non_snake_case)]
    dwReserveVersion: DWORD,
    #[allow(non_snake_case)]
    pReserve: * mut cty::c_void
}

type FnProcRecv = extern fn (data: TD_ST_TRANSDATA_V02) -> i32;

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug)]
pub struct TD_ST_CONNECTION_V02 {
    #[allow(non_snake_case)]
    shScaleID: cty::c_short,
    #[allow(non_snake_case)]
    lpIP: LPSTR,
    #[allow(non_snake_case)]
    wdPort: WORD,
    #[allow(non_snake_case)]
    wdScaleType: WORD,
    #[allow(non_snake_case)]
    wdScaleModel: WORD,
    #[allow(non_snake_case)]
    wdTimeOut: WORD,
    #[allow(non_snake_case)]
    wdRetryCount: WORD,
    #[allow(non_snake_case)]
    btCommType: BYTE,
    #[allow(non_snake_case)]
    btTransType: BYTE,
    #[allow(non_snake_case)]
    btSocketType: BYTE,
    #[allow(non_snake_case)]
    btDataType: BYTE,
    #[allow(non_snake_case)]
    dwMsgNo: DWORD,
    #[allow(non_snake_case)]
    dwStateMsgNo: DWORD,
    #[allow(non_snake_case)]
    btLogStatus: BYTE,
    #[allow(non_snake_case)]
    lpLogFileName: LPSTR,
    #[allow(non_snake_case)]
    pRecvProc: FnProcRecv,
    #[allow(non_snake_case)]
    pStateProc: FnProcRecv,
    #[allow(non_snake_case)]
    dwScaleMainVersion: DWORD,
    #[allow(non_snake_case)]
    dwScaleSubVersion: DWORD,
    #[allow(non_snake_case)]
    dwScaleCountry: DWORD,
    #[allow(non_snake_case)]
    dwScaleDataVersion: DWORD,
    #[allow(non_snake_case)]
    dwReserveVersion: DWORD,
    #[allow(non_snake_case)]
    pReserve: * mut cty::c_void
}

type FnSetCommLibrary = libloading::os::windows::Symbol<unsafe extern fn(i32, LPSTR) -> i32>;
type FnAddInterpreter = libloading::os::windows::Symbol<unsafe extern fn(WORD, WORD, LPSTR) -> i32>;
type FnAddConnectionEx = libloading::os::windows::Symbol<unsafe extern fn(TD_ST_CONNECTION_V02) -> i32>;
type FnConnect = libloading::os::windows::Symbol<unsafe extern fn(LPSTR, cty::c_short) -> i32>;

pub struct ScaleFile {
    scale_file: String,
    lib_prtc: Library,

    set_comm_library: FnSetCommLibrary,
    add_interpreter: FnAddInterpreter,
    add_connection_ex: FnAddConnectionEx,
    connect: FnConnect,
}

pub fn get_lib(dll: &str) -> Result<(Library, PathBuf), libloading::Error> {
    let patho = get_executable_path().unwrap_or(PathBuf::from("C:\\CAS\\a.dll"));
    let parent = patho.parent().unwrap();
    let mut dllpath: PathBuf = PathBuf::from(parent);
    dllpath.push(dll);
    let lib = unsafe { Library::new(dllpath.as_path()) };
    if lib.is_ok() {
        println!("Loaded: {}", dllpath.to_string_lossy());
        return Ok((lib.unwrap(), dllpath))
    }
    dllpath = PathBuf::from("C:\\CAS");
    dllpath.push(dll);
    let lib = unsafe { Library::new(dllpath.as_path()) };
    if lib.is_ok() {
        println!("Loaded: {}", dllpath.to_string_lossy());
        return Ok((lib.unwrap(), dllpath))
    } else {
        println!("Error: {:?}", lib.as_ref().err())
    }
    return Err(lib.err().unwrap());
}
pub fn create_scale_file(file: &String) -> ScaleFile {
    let casprtc = get_lib("CASPRTC.dll");
    if casprtc.is_err() {
        println!("Error: {:?}", casprtc.as_ref().err())
    }
    let (casprtc_lib, casprtc_path) = casprtc.unwrap();
    unsafe {
        let set_comm_library: FnSetCommLibrary = casprtc_lib.get(TD_LIB_SETCOMMLIB).unwrap();
        let add_interpreter: FnAddInterpreter = casprtc_lib.get(TD_LIB_ADDINTERPRETER).unwrap();
        let add_connection_ex: FnAddConnectionEx = casprtc_lib.get(TD_LIB_ADDCONNECTIONEX).unwrap();
        let connect: FnConnect = casprtc_lib.get(TD_LIB_CONNECT).unwrap();
        
        let sf = ScaleFile{
            scale_file: file.to_string(),
            lib_prtc: casprtc_lib,
            set_comm_library: set_comm_library,
            add_interpreter: add_interpreter,
            add_connection_ex: add_connection_ex,
            connect: connect,
        };

        let mut dll = PathBuf::from(casprtc_path.parent().unwrap());
        dll.push("CASTCPIP.dll");
        let dll_str = dll.as_path().as_os_str().to_string_lossy();
        let ret = (sf.set_comm_library)(DF_MODULE_TCPIP, dll_str.as_bytes().as_ptr());
        println!("TCPIP comms ({}): {}", dll_str, ret);

        for (scale_model, interp) in [
            (DF_SCALE_CL3500, CL_INTERP.to_vec()), (DF_SCALE_CL5000, CL_INTERP.to_vec()),
            (DF_SCALE_CL5200, CL_INTERP.to_vec()), (DF_SCALE_CL5500, CL_INTERP.to_vec()),
            (DF_SCALE_CL7200, CL_INTERP.to_vec()), (DF_SCALE_CL5000JR, CL_JRINTERP.to_vec())] {
            let mut dll = PathBuf::from(casprtc_path.parent().unwrap());
            dll.push(Path::new(&String::from_utf8(interp.to_vec()).unwrap()));
            let dll_str = dll.as_path().as_os_str().to_string_lossy();
            let ret = (sf.add_interpreter)(DF_SCALE_TYPE_LP, scale_model, dll_str.as_bytes().as_ptr());
            println!("Add Interpreter {}: {}", scale_model, ret);
        }
        sf
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
pub extern "C" fn recvproc (data: TD_ST_TRANSDATA_V02) -> i32 { 
    println!("In CAS recv callback");
    0
}
pub extern "C" fn stateproc (data: TD_ST_TRANSDATA_V02) -> i32 {
    println!("In CAS state callback");
    println!("{:?}", data);
    println!("{}", unsafe { CStr::from_ptr(data.lpIP as * const i8).to_str().unwrap() });
    0
}

impl ScaleFile {
    pub fn filtered_items(&mut self, api: &mut super::api::ITRApi, args: &ArgMatches) -> Result<Vec<super::api::ProductData>> {
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
            let json = api.get(&"/api/ProductsData/GetAllProducts".to_string()).expect("no results from API call");
            items = serde_json::from_str(&json)?;
            items = items.into_iter().filter(filter).sorted_by_key(|x| x.section_id.unwrap_or(0)).collect::<Vec<super::api::ProductData>>();
        }

        items = items.into_iter().filter(|item| {
            if item.plu.is_none() { return false }
            let plu = item.plu.as_ref().unwrap().parse::<u16>();
            if plu.is_err() { return false }
            if !dump_internal && plu.unwrap() < 1000 { return false }
            if item.upc.get(3..8).is_none() { return false }
            true
        }).collect::<Vec<super::api::ProductData>>();
        Ok(items)
    }

    pub fn add_scale(&mut self, ip: &str, idx: cty::c_short) -> Option<(String, cty::c_short)> {
        let cstring_ip = CString::new(ip).unwrap();
        let cstr_ip = cstring_ip.as_c_str();
        let lpIP = cstr_ip.as_ptr() as * const u8;
        let mut td = TD_ST_CONNECTION_V02 {
            shScaleID: idx,
            lpIP: lpIP,
            wdPort: 20304,
            wdScaleType: DF_SCALE_TYPE_LP,
            wdScaleModel: DF_SCALE_CL5500,
            wdTimeOut: DF_TRANS_TIMEOUT,
            wdRetryCount: DF_TRANS_RETRYCOUNT,
            btCommType: DF_COMMTYPE_TCPIP,
            btTransType: DF_TRANSTYPE_PROC,
            btSocketType: 1,
            btDataType: DF_ACTION_NOTIFY,
            btLogStatus: 0,
            dwMsgNo: 0,
            dwStateMsgNo: 0,
            lpLogFileName: std::ptr::null_mut(),
            pRecvProc: recvproc,
            pStateProc: stateproc,
            // 2.95.7,2.0,2
            dwScaleMainVersion: 295,
            dwScaleSubVersion: 7,
            dwScaleCountry: 2,
            dwScaleDataVersion: 20,
            dwReserveVersion: 0,
            pReserve: std::ptr::null_mut()
        };
        let ret = unsafe { (self.add_connection_ex)(td) };
        if ret != 0 {
            let ret = unsafe {(self.connect)(ip.as_ptr(), idx) };
            if ret != 0 {
                return Some((ip.to_string(), idx))
            }
        }
        println!("IP: {:?}", cstr_ip);
        None
    }

    pub fn send(&mut self, api: &mut super::api::ITRApi, args: &ArgMatches) -> Result<()> {
        let weighed_items = self.filtered_items(api, args)?;
        if let Some(scales) = args.get_many::<String>("scale") {
            let mut map_of_scales = HashSet::new();
            let mut idx: cty::c_short = 1;
            for scale in scales.into_iter() {
                if let Some(added) = self.add_scale(scale, idx) {
                   map_of_scales.insert(added);
                   println!("Syncing to {}", scale);
                   idx = idx + 1;
                } else {
                   println!("Error adding scale {}", scale);
                }
            }
        }
        Ok(())
        //self.build_from_itretail_products(weighed_items)
    }

    pub fn build_from_itretail_products(&mut self, weighed_items: Vec<ProductData>) -> Result<()> {
        let mut workbook = Workbook::new();
        let bold_format = Format::new().set_bold();
        let decimal_format = Format::new().set_num_format("0.00");
        let date_format = Format::new().set_num_format("yyyy-mm-dd");

        let date = Local::now().naive_local();

        let worksheet = workbook.add_worksheet();
        for idx in 0..FIELDS.len()-1 {
            worksheet.write_with_format(0, idx.try_into().unwrap(), FIELDS[idx], &bold_format)?;
        }

        let mut row: u32 = 1;
        for item in weighed_items {
            worksheet.write_number(row, 0, item.department_id)?;
            let plu = item.plu.unwrap().parse::<u16>().unwrap();
            worksheet.write_number(row, 1, plu)?;
            worksheet.write_string(row, 2, &item.description)?;
            // 3 Name2 (blank)
            let itemcode_str = item.upc.get(3..8).unwrap();
            let itemcode = itemcode_str.trim_start_matches('0').parse::<u32>().or::<u32>(Ok(0)).unwrap();
            

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