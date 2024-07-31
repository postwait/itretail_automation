use anyhow::{anyhow, Result};
use chrono::Local;
use clap::ArgMatches;
use fancy_regex::Regex;
use itertools::Itertools;
use lazy_static::lazy_static;
use libloading::os::windows::Library;
use libloading::os::windows::Symbol;
use log::*;
use process_path::get_executable_path;
use rust_xlsxwriter::{Format, Workbook};
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{
    io::{self, Write},
    thread,
    time::{self, Instant},
};

use super::api::{PLUAssignment, ProductData};

type LPSTR = *const std::ffi::c_char;
type WORD = u16;
type DWORD = u32;
type BYTE = u8;
const CL_INTERP: &[u8; 18] = b"CLInterpreter.dll\0";
const CL_JRINTERP: &[u8; 20] = b"CLJRInterpreter.dll\0";
const TD_LIB_SETCOMMLIB: &[u8; 15] = b"SetCommLibrary\0";
const TD_LIB_ADDINTERPRETER: &[u8; 15] = b"AddInterpreter\0";
const TD_LIB_ADDCONNECTIONEX: &[u8; 16] = b"AddConnectionEx\0";
const TD_LIB_CONNECT: &[u8; 8] = b"Connect\0";
const TD_LIB_DISCONNECT: &[u8; 11] = b"Disconnect\0";
const TD_LIB_SETTIMEOUT: &[u8; 11] = b"SetTimeout\0";
const TD_LIB_SENDDATAEX: &[u8; 11] = b"SendDataEx\0";

#[repr(u16)]
#[derive(Debug, PartialEq, Copy, Clone)]
#[allow(dead_code)]
pub enum DfAction {
    NOTHING = 0,
    GETINFO = 1,
    DOWNLOAD = 3,
    DELETE = 4,
    DELETEALL = 5,
    PING = 22,
    COMPLETE = 23,
    NOTIFY = 27,
}
impl From<DfAction> for u8 {
    fn from(item: DfAction) -> Self {
        item as u8
    }
}
impl From<DfAction> for u16 {
    fn from(item: DfAction) -> Self {
        item as u16
    }
}
const DF_COMMTYPE_TCPIP: u8 = 1;

#[repr(u8)]
#[derive(Debug)]
#[allow(dead_code, non_camel_case_types, non_snake_case)]
enum DfData {
    PING = 7,
    PLU = 10,
    CUSTOM = 27,
    PLU_V06 = 98,
}
impl From<DfData> for u8 {
    fn from(item: DfData) -> Self {
        item as u8
    }
}
const DF_MODULE_TCPIP: i32 = 1;

#[repr(u16)]
#[derive(Debug)]
#[allow(dead_code, non_camel_case_types, non_snake_case)]
enum DfScaleType {
    LP = 100,
}
impl From<DfScaleType> for u16 {
    fn from(item: DfScaleType) -> Self {
        item as u16
    }
}
#[repr(u16)]
#[derive(Debug)]
#[allow(dead_code, non_camel_case_types, non_snake_case)]
enum DfScale {
    CL3500 = 3500,
    CL5000 = 5000,
    CL5000JR = 5010,
    CL5200 = 5200,
    CL5500 = 5500,
    CL7200 = 7200,
}
impl From<DfScale> for u16 {
    fn from(item: DfScale) -> Self {
        item as u16
    }
}
#[repr(u8)]
#[derive(Debug, Copy, Clone)]
#[allow(dead_code, non_camel_case_types, non_snake_case)]
enum DfSendType {
    NORMAL = 1,
    BROADCAST = 2,
}
impl From<DfSendType> for u8 {
    fn from(item: DfSendType) -> Self {
        item as u8
    }
}
const DF_TRANS_TIMEOUT: u16 = 3000;
const DF_TRANS_RETRYCOUNT: u16 = 3;
const DF_TRANSTYPE_PROC: u8 = 0;

#[repr(C)]
#[allow(non_camel_case_types, non_snake_case)]
#[derive(Debug)]
pub struct TD_ST_STATE {
    wdState: DfState,
    lpDescription: LPSTR,
}
#[repr(u16)]
#[derive(Debug, Copy, Clone)]
#[allow(dead_code, non_camel_case_types, non_snake_case)]
enum DfState {
    CONNECT = 1,
    DISCONNECT = 2,
    CLOSE = 3,
    RECV = 4,
    SEND = 5,
    OUTOFBAND = 6,
    RETRY = 7,
    RETRYOVER = 8,
    COMPLETE = 9,
    SUCCESS = 10,
    LISTEN = 11,
    ACCEPT = 12,
    INVALIDSOCKET = 13,
    SOCKETERROR = 14,
    HOSTNOTFIND = 15,
    TIMEOUT = 16,
    WSAERROR = 17,
    COMMERROR = 18,
    ADDCONNERROR = 19,
    CONNERROR = 20,
    SENDFAIL = 21,
    RECEIVETIMEOVER = 22,
    ALREADYCONNECTED = 23,
    CONNECT_FTP = 31,
    DISCONNECT_FTP = 32,
    RETRYOVER_FTP = 33,
    CONNERROR_FTP = 34,
    SENDFAIL_FTP = 35,
    RECEIVETIMEOVER_FTP = 36,
    ALREADYCONNECTED_FTP = 37,
}

#[repr(C)]
#[allow(non_camel_case_types, non_snake_case, dead_code)]
#[derive(Debug)]
pub struct TD_ST_TRANSDATA_V01 {
    shScaleID: std::ffi::c_short,
    lpIP: LPSTR,
    btCommType: BYTE,
    btSendType: DfSendType,
    btDataType: DfData,
    wdScaleType: DfScaleType,
    wdScaleModel: DfScale,
    wdAction: DfAction,
    wdDataSize: WORD,
    pData: *mut std::ffi::c_void,
}

#[repr(C)]
#[allow(non_camel_case_types, non_snake_case)]
#[derive(Debug)]
pub struct TD_ST_PINGINFO {
    nComplete: i32,
    nLoop: i32,
    nBytes: i32,
    nTime: i32,
    nTTL: i32,
    strIp: [i8; 41],
}
impl Default for TD_ST_PINGINFO {
    fn default() -> TD_ST_PINGINFO {
        TD_ST_PINGINFO {
            nComplete: 0,
            nLoop: 1,
            nBytes: 1,
            nTime: 1,
            nTTL: 255,
            strIp: [0; 41],
        }
    }
}

#[repr(C)]
#[allow(non_camel_case_types, non_snake_case)]
#[derive(Debug)]
pub struct TD_ST_TRANSDATA_V02 {
    shScaleID: std::ffi::c_short,
    lpIP: LPSTR,
    btCommType: BYTE,
    btSendType: DfSendType,
    btDataType: DfData,
    wdScaleType: DfScaleType,
    wdScaleModel: DfScale,
    wdAction: DfAction,
    wdDataSize: WORD,
    pData: *mut std::ffi::c_void,
    dwScaleMainVersion: DWORD,
    dwScaleSubVersion: DWORD,
    dwScaleCountry: DWORD,
    dwScaleDataVersion: DWORD,
    dwReserveVersion: DWORD,
    pReserve: *mut std::ffi::c_void,
}

type FnProcRecv = extern "C" fn(data: TD_ST_TRANSDATA_V02) -> i32;

#[repr(C)]
#[allow(non_camel_case_types, non_snake_case)]
#[derive(Debug)]
pub struct TD_ST_CONNECTION_V02 {
    shScaleID: std::ffi::c_short,
    lpIP: LPSTR,
    wdPort: WORD,
    wdScaleType: DfScaleType,
    wdScaleModel: DfScale,
    wdTimeOut: WORD,
    wdRetryCount: WORD,
    btCommType: BYTE,
    btTransType: BYTE,
    btSocketType: BYTE,
    btDataType: DfData,
    dwMsgNo: DWORD,
    dwStateMsgNo: DWORD,
    btLogStatus: BYTE,
    lpLogFileName: LPSTR,
    pRecvProc: FnProcRecv,
    pStateProc: FnProcRecv,
    dwScaleMainVersion: DWORD,
    dwScaleSubVersion: DWORD,
    dwScaleCountry: DWORD,
    dwScaleDataVersion: DWORD,
    dwReserveVersion: DWORD,
    pReserve: *mut std::ffi::c_void,
}

#[repr(C, packed)]
#[allow(non_camel_case_types, non_snake_case)]
#[derive(Debug)]
pub struct TD_ST_PLU_V06 {
    wdDepart: WORD,
    dwPLU: DWORD,
    btPLUType: BYTE,
    chName1: [i8; 101],
    chName2: [i8; 101],
    chName3: [i8; 101],
    wdGroup: WORD,
    chBarcodeEx: [i8; 101],
    wdLabel1: WORD,
    wdLabel2: WORD,
    wdOrigin: WORD,
    btWeightUnit: BYTE,
    dwFixWeight: DWORD,
    chPrefix: [i8; 11],
    dwItemCode: DWORD,
    wdPieces: WORD,
    btQuatSymbol: BYTE,
    btPriceType: BYTE,
    dwUnitPrice: DWORD,
    dwSpecialPrice: DWORD,
    wdTaxNo: WORD,
    dwTare: DWORD,
    wdTareNo: WORD,
    dwPerTare: DWORD,
    dwTareLimit: DWORD,
    wdBarcode1: WORD,
    wdBarcode2: WORD,
    wdPicture: WORD,
    wdProduceDate: WORD,
    wdPackDate: WORD,
    wdPackTime: WORD,
    dwSellDate: DWORD,
    wdSellTime: WORD,
    wdCookDate: WORD,
    wdIngredient: WORD,
    wdTraceability: WORD,
    wdBonus: WORD,
    wdNutrifact: WORD,
    wdSaleMSG: WORD,
    wdRefPLUDept: WORD,
    dwRefPLUNo: DWORD,
    wdCouplePLUDept: WORD,
    dwCouplePLUNo: DWORD,
    wdLinkPLUCount: WORD,
    wdLinkPLUDept1: WORD,
    dwLinkPLUNo1: DWORD,
    wdLinkPLUDept2: WORD,
    dwLinkPLUNo2: DWORD,
    btTotalFlag: BYTE,
    dwTotalCount: DWORD,
    dwTotalPrice: DWORD,
    dwTotalWeight: DWORD,
    chReserve1: [i8; 51],
    chReserve2: [i8; 51],
    chReserve3: [i8; 51],
    wdNo: WORD,
    wdDirectSize: WORD,
    chDirectIngredient: [i8; 4097],
    btPackedDateFlag: BYTE,
    btPackedTimeFlag: BYTE,
    btSellByDateFlag: BYTE,
    btSellByTimeFlag: BYTE,
    chName4: [i8; 101],
    chName5: [i8; 101],
    chName6: [i8; 101],
    chName7: [i8; 101],
    chName8: [i8; 101],
    btNameFontSize1: BYTE,
    btNameFontSize2: BYTE,
    btNameFontSize3: BYTE,
    btNameFontSize4: BYTE,
    btNameFontSize5: BYTE,
    btNameFontSize6: BYTE,
    btNameFontSize7: BYTE,
    btNameFontSize8: BYTE,
    btTraceItemFlag: BYTE,
    btDtIngredientFlag: BYTE,
    btDtSaleMsgFlag: BYTE,
    btDtNutriFactFlag: BYTE,
    btDtOriginFlag: BYTE,
    chPictureFile: [i8; 50],
}
impl Default for TD_ST_PLU_V06 {
    fn default() -> TD_ST_PLU_V06 {
        TD_ST_PLU_V06 {
            wdDepart: 0,
            dwPLU: 0,
            btPLUType: 0,
            chName1: [0; 101],
            chName2: [0; 101],
            chName3: [0; 101],
            wdGroup: 0,
            chBarcodeEx: [0; 101],
            wdLabel1: 0,
            wdLabel2: 0,
            wdOrigin: 0,
            btWeightUnit: 0,
            dwFixWeight: 0,
            chPrefix: [0; 11],
            dwItemCode: 0,
            wdPieces: 0,
            btQuatSymbol: 0,
            btPriceType: 0,
            dwUnitPrice: 0,
            dwSpecialPrice: 0,
            wdTaxNo: 0,
            dwTare: 0,
            dwPerTare: 0,
            dwTareLimit: 0,
            wdBarcode1: 0,
            wdTareNo: 0,
            wdBarcode2: 0,
            wdPicture: 0,
            wdProduceDate: 0,
            wdPackDate: 0,
            wdPackTime: 0,
            wdSellTime: 0,
            dwSellDate: 0,
            wdCookDate: 0,
            wdIngredient: 0,
            wdBonus: 0,
            wdTraceability: 0,
            wdNutrifact: 0,
            wdSaleMSG: 0,
            wdRefPLUDept: 0,
            dwRefPLUNo: 0,
            wdCouplePLUDept: 0,
            dwCouplePLUNo: 0,
            wdLinkPLUCount: 0,
            wdLinkPLUDept1: 0,
            dwLinkPLUNo1: 0,
            wdLinkPLUDept2: 0,
            dwLinkPLUNo2: 0,
            btTotalFlag: 0,
            dwTotalCount: 0,
            dwTotalPrice: 0,
            dwTotalWeight: 0,
            chReserve1: [0; 51],
            chReserve2: [0; 51],
            chReserve3: [0; 51],
            wdNo: 0,
            wdDirectSize: 0,
            chDirectIngredient: [0; 4097],
            btPackedDateFlag: 0,
            btPackedTimeFlag: 0,
            btSellByDateFlag: 0,
            btSellByTimeFlag: 0,
            chName4: [0; 101],
            chName5: [0; 101],
            chName6: [0; 101],
            chName7: [0; 101],
            chName8: [0; 101],
            btNameFontSize1: 0,
            btNameFontSize2: 0,
            btNameFontSize3: 0,
            btNameFontSize4: 0,
            btNameFontSize5: 0,
            btNameFontSize6: 0,
            btNameFontSize7: 0,
            btNameFontSize8: 0,
            btTraceItemFlag: 0,
            btDtIngredientFlag: 0,
            btDtSaleMsgFlag: 0,
            btDtNutriFactFlag: 0,
            btDtOriginFlag: 0,
            chPictureFile: [0; 50],
        }
    }
}

//const SHRINK_LABEL_ID: u16 = 51;
//const STANDARD_LABEL_ID: u16 = 61;
const INGREDIENT_LABEL_ID: u16 = 62;

fn jam(string: &String, out: &mut [i8]) {
    let bs = string.as_bytes();
    let bsr = bs.as_ptr() as *const i8;
    let copylen = if bs.len() < out.len() {
        bs.len()
    } else {
        out.len() - 1
    };
    unsafe { std::ptr::copy(bsr, out.as_mut_ptr(), copylen) };
}
impl From<&ProductData> for TD_ST_PLU_V06 {
    fn from(p: &ProductData) -> TD_ST_PLU_V06 {
        let mut cp = TD_ST_PLU_V06::default();
        cp.wdDepart = p.department_id as WORD;
        cp.dwPLU = p.plu.as_ref().unwrap().parse::<DWORD>().unwrap();
        jam(&p.description, &mut cp.chName1);
        let itemcode_str = p.upc.get(3..8).unwrap();
        let itemcode = itemcode_str
            .trim_start_matches('0')
            .parse::<u32>()
            .or::<u32>(Ok(0))
            .unwrap();
        cp.dwItemCode = itemcode;
        cp.dwUnitPrice = (p.normal_price * 100.0) as u32;
        cp.btWeightUnit = 1; // by 1 lb
        cp.wdLabel1 = 0;
        if p.second_description.is_some() {
            let ingredients = p.second_description.as_ref().unwrap();
            if ingredients.len() > 0 {
                cp.wdLabel1 = INGREDIENT_LABEL_ID; // butcher's club specific
                jam(&ingredients, &mut cp.chDirectIngredient);
            }
        }
        cp.btPLUType = 1; // weighed
        cp
    }
}

type FnSetCommLibrary = Symbol<unsafe extern "C" fn(i32, LPSTR, i32) -> i32>;
type FnAddInterpreter = Symbol<unsafe extern "C" fn(WORD, WORD, LPSTR) -> i32>;
type FnAddConnectionEx = Symbol<unsafe extern "C" fn(TD_ST_CONNECTION_V02) -> i32>;
type FnScale = Symbol<unsafe extern "C" fn(LPSTR, std::ffi::c_short) -> i32>;
type FnScaleInt = Symbol<unsafe extern "C" fn(LPSTR, std::ffi::c_short, i32) -> i32>;
type FnSendDataEx = Symbol<unsafe extern "C" fn(TD_ST_TRANSDATA_V02) -> i32>;

#[derive(Debug)]
pub struct Scale {
    ip: String,
    idx: i16,
    state: DfState,
    last_send_action: DfAction,
    last_recv_action: DfAction,
    should_delete: bool,
    delete_completed: bool,
    plus_downloaded: u32,
    product_idx: u32,
    products: Arc<Vec<ProductData>>,
    notified: bool,
}

impl Scale {
    fn new(ip: String) -> Self {
        Scale {
            ip,
            idx: -1,
            state: DfState::DISCONNECT,
            last_send_action: DfAction::NOTHING,
            last_recv_action: DfAction::NOTHING,
            should_delete: true,
            delete_completed: false,
            plus_downloaded: 0,
            product_idx: 0,
            products: Arc::new(vec![]),
            notified: false,
        }
    }
    pub fn complete(&self) -> bool {
        (self.plus_downloaded as usize == self.products.len())
            && (self.should_delete == self.delete_completed)
    }
    pub fn status_str(&self) -> String {
        if self.complete() {
            format!("{} [complete]", self.ip)
        } else if self.should_delete && !self.delete_completed {
            format!("{} [deleting]", self.ip)
        } else {
            let pcomplete = 100.0 * (self.plus_downloaded as f32 / self.products.len() as f32);
            format!("{} [{:7.2}%]", self.ip, pcomplete)
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct ScaleAPI {
    lib_prtc: Library,
    scales: HashMap<String, Arc<Mutex<Scale>>>,
    cas_set_comm_library: FnSetCommLibrary,
    cas_add_interpreter: FnAddInterpreter,
    cas_add_connection_ex: FnAddConnectionEx,
    cas_connect: FnScale,
    cas_disconnect: FnScale,
    cas_set_timeout: FnScaleInt,
    cas_senddata_ex: FnSendDataEx,
}

pub fn get_lib(dll: &str) -> Result<(Library, PathBuf), libloading::Error> {
    let patho = get_executable_path().unwrap_or(PathBuf::from("C:\\CAS\\a.dll"));
    let parent = patho.parent().unwrap();
    let mut dllpath: PathBuf = PathBuf::from(parent);
    dllpath.push(dll);
    let lib = unsafe { Library::new(dllpath.as_path()) };
    if lib.is_ok() {
        return Ok((lib.unwrap(), dllpath));
    }
    dllpath = PathBuf::from("C:\\CAS");
    dllpath.push(dll);
    let lib = unsafe { Library::new(dllpath.as_path()) };
    if lib.is_ok() {
        return Ok((lib.unwrap(), dllpath));
    }
    return Err(lib.err().unwrap());
}

fn cas_api_init() -> ScaleAPI {
    let casprtc = get_lib("CASPRTC.dll");
    if casprtc.is_err() {
        error!("Error: {:?}", casprtc.as_ref().err());
        panic!("Cannot continue without CAS support. (DLLs missing?)");
    }
    let (casprtc_lib, casprtc_path) = casprtc.unwrap();
    unsafe {
        let set_comm_library: FnSetCommLibrary = casprtc_lib.get(TD_LIB_SETCOMMLIB).unwrap();
        let add_interpreter: FnAddInterpreter = casprtc_lib.get(TD_LIB_ADDINTERPRETER).unwrap();
        let add_connection_ex: FnAddConnectionEx = casprtc_lib.get(TD_LIB_ADDCONNECTIONEX).unwrap();
        let connect: FnScale = casprtc_lib.get(TD_LIB_CONNECT).unwrap();
        let disconnect: FnScale = casprtc_lib.get(TD_LIB_DISCONNECT).unwrap();
        let set_timeout: FnScaleInt = casprtc_lib.get(TD_LIB_SETTIMEOUT).unwrap();
        let senddata_ex: FnSendDataEx = casprtc_lib.get(TD_LIB_SENDDATAEX).unwrap();

        let sf = ScaleAPI {
            lib_prtc: casprtc_lib,
            cas_set_comm_library: set_comm_library,
            cas_add_interpreter: add_interpreter,
            cas_add_connection_ex: add_connection_ex,
            cas_connect: connect,
            cas_disconnect: disconnect,
            cas_set_timeout: set_timeout,
            cas_senddata_ex: senddata_ex,
            scales: HashMap::new(),
        };
        let init: Symbol<extern "C" fn(i32) -> i32> = sf.lib_prtc.get(b"Initialize\0").unwrap();
        let rc = init(0);
        debug!("CASPRTC init -> {}", rc);

        let mut dll = PathBuf::from(casprtc_path.parent().unwrap());
        dll.push("CASTCPIP.dll");
        let dll_str = dll.as_path().as_os_str().to_string_lossy();
        let dll_cstring = CString::new(dll_str.as_ref()).unwrap();
        let ret = (sf.cas_set_comm_library)(DF_MODULE_TCPIP, dll_cstring.as_ptr(), 1);
        if ret == 0 {
            panic!("TCPIP comms ({:?}): {}", dll_str, ret);
        }
        debug!("TCPIP comms online.");

        for (scale_model, interp) in [
            (DfScale::CL3500, CL_INTERP.to_vec()),
            (DfScale::CL5000, CL_INTERP.to_vec()),
            (DfScale::CL5200, CL_INTERP.to_vec()),
            (DfScale::CL5500, CL_INTERP.to_vec()),
            (DfScale::CL7200, CL_INTERP.to_vec()),
            (DfScale::CL5000JR, CL_JRINTERP.to_vec()),
        ] {
            let mut dll = PathBuf::from(casprtc_path.parent().unwrap());
            dll.push(Path::new(&String::from_utf8(interp.to_vec()).unwrap()));
            let dll_str = dll.as_path().to_str().unwrap();
            let dll_cstring = dll_str.as_bytes();
            let out = format!("{:?}", scale_model);
            let ret = (sf.cas_add_interpreter)(
                DfScaleType::LP.into(),
                scale_model.into(),
                dll_cstring.as_ptr() as *const i8,
            );
            if ret == 0 {
                debug!("Add Interpreter {:?}: {}", out, ret);
            }
        }
        debug!("{:?}", sf);
        sf
    }
}

lazy_static! {
    static ref DLLAPI: Mutex<ScaleAPI> = Mutex::new(cas_api_init());
}

fn wrong_range(item: &super::api::ProductData, plu: u16) -> bool {
    (item.description.starts_with("(I)") && plu >= 1000)
        || (!item.description.starts_with("(I)") && plu < 1000)
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
pub extern "C" fn recvproc(data: TD_ST_TRANSDATA_V02) -> i32 {
    let ip = lpstr_to_strref(data.lpIP); // as * const i8).to_str().unwrap() };
    let cas = DLLAPI.lock().unwrap();
    let mut scale = cas.scales.get(&ip).unwrap().lock().unwrap();
    scale.last_recv_action = data.wdAction;
    match data.wdAction {
        DfAction::DELETEALL => {
            debug!("RECV: {:?}", data);
            let pdata = data.pData as *const TD_ST_PLU_V06;
            unsafe {
                let name = lpstr_to_strref(&(*pdata).chName1 as *const i8);
                if name.starts_with("W") {
                    scale.delete_completed = true;
                    let rc = cas.push_plu(&mut scale);
                    match rc {
                        Ok(_r) => {
                            scale.product_idx += 1;
                        }
                        Err(e) => {
                            error!("{} errored: {}", scale.ip, e);
                            cas.disconnect_scale(&scale);
                        }
                    }
                }
            }
        }
        DfAction::DOWNLOAD => {
            debug!("RECV: {:?}", data);
            scale.plus_downloaded += 1;
            let rc = cas.push_plu(&mut scale);
            match rc {
                Ok(_r) => {
                    scale.product_idx += 1;
                }
                Err(e) => {
                    error!("{} errored: {}", scale.ip, e);
                    cas.disconnect_scale(&scale);
                }
            }
        }
        _ => {
            debug!("RECV: {:?}", data);
        }
    }
    1
}
pub fn lpstr_to_strref(ptr: *const i8) -> String {
    if ptr == std::ptr::null() {
        "[null]".to_owned()
    } else {
        unsafe { CStr::from_ptr(ptr).to_str().unwrap() }.to_string()
    }
}
pub extern "C" fn stateproc(data: TD_ST_TRANSDATA_V02) -> i32 {
    let ip = lpstr_to_strref(data.lpIP); // as * const i8).to_str().unwrap() };
    let (state, description) = unsafe {
        let pdata = data.pData as *const TD_ST_STATE;
        ((*pdata).wdState, lpstr_to_strref((*pdata).lpDescription))
    };
    debug!("STATE {:?}", state);
    let cas = DLLAPI.lock().unwrap();
    let mut scale = {
        let wrapper = cas.scales.get(&ip);
        if wrapper.is_none() {
            warn!(
                "State message {:?}: {} from unknown scale {}",
                state, description, ip
            );
            return 0;
        }
        wrapper.unwrap().lock().unwrap()
    };
    scale.state = state;
    match state {
        DfState::CONNECT => {
            info!("{} Connected: {}", ip, description);
            if scale.should_delete {
                cas.delete_plus(&mut scale);
            } else {
                let rc = cas.push_plu(&mut scale);
                match rc {
                    Ok(r) => {
                        if r {
                            scale.product_idx = scale.product_idx + 1;
                        }
                    }
                    Err(e) => {
                        error!("Scale {}: {}", scale.ip, e);
                        cas.disconnect_scale(&scale);
                    }
                }
            }
            return 1;
        }
        DfState::RECEIVETIMEOVER => {
            return 1;
        }
        _ => {
            warn!("Unhandled state: {:?}", state);
        }
    };
    0
}

impl ScaleAPI {
    fn make_transdata(
        &self,
        ip: *const i8,
        idx: std::ffi::c_short,
        action: DfAction,
        datatype: DfData,
        data: *mut std::ffi::c_void,
        data_size: usize,
    ) -> TD_ST_TRANSDATA_V02 {
        TD_ST_TRANSDATA_V02 {
            shScaleID: idx,
            lpIP: ip,
            wdScaleType: DfScaleType::LP,
            wdScaleModel: DfScale::CL5500,
            btCommType: DF_COMMTYPE_TCPIP,
            btDataType: datatype,
            btSendType: DfSendType::NORMAL,
            wdAction: action.into(),
            wdDataSize: data_size as WORD,
            pData: data,
            dwScaleMainVersion: 295,
            dwScaleSubVersion: 7,
            dwScaleCountry: 2,
            dwScaleDataVersion: 20,
            dwReserveVersion: 0,
            pReserve: std::ptr::null_mut(),
        }
    }
    pub fn delete_plus(&self, scale: &mut Scale) -> bool {
        info!("Deleting PLUs off scale {}", scale.ip);
        match scale.state {
            DfState::CONNECT => {}
            _ => {
                error!("Scale {} in unexpected state: {:?}", scale.ip, scale.state);
                return false;
            }
        }
        let lp_ip = CString::new(scale.ip.to_string()).unwrap();
        let td = {
            self.make_transdata(
                lp_ip.as_ptr(),
                scale.idx,
                DfAction::DELETEALL,
                DfData::PLU_V06,
                std::ptr::null_mut(),
                0,
            )
        };
        unsafe {
            debug!("SEND {} <- {:?}", lpstr_to_strref(td.lpIP), td);
            scale.last_send_action = DfAction::DELETEALL;
            let ret = (self.cas_senddata_ex)(td);
            debug!("SEND ret {}", ret);
        }
        true
    }
    pub fn push_plu(&self, scale: &mut Scale) -> Result<bool> {
        if scale.product_idx as usize >= scale.products.len() {
            return Ok(false);
        }
        match scale.state {
            DfState::CONNECT => {}
            _ => {
                return Err(anyhow!(
                    "Scale {} in unexpected state: {:?}",
                    scale.ip,
                    scale.state
                ));
            }
        }
        let lp_ip = CString::new(scale.ip.to_string()).unwrap();
        let mut td = {
            self.make_transdata(
                lp_ip.as_ptr(),
                scale.idx,
                DfAction::DOWNLOAD,
                DfData::PLU_V06,
                std::ptr::null_mut(),
                0,
            )
        };

        let item = &scale.products[scale.product_idx as usize];
        let mut plu: TD_ST_PLU_V06 = item.into();
        let dw_plu = std::ptr::addr_of!(plu.dwPLU);
        debug!(
            "Pushing PLU {} to {}",
            unsafe { std::ptr::read_unaligned(dw_plu) },
            scale.ip
        );
        td.wdDataSize = std::mem::size_of::<TD_ST_PLU_V06>() as u16;
        td.pData = std::ptr::addr_of_mut!(plu) as *mut std::ffi::c_void;

        scale.last_send_action = DfAction::DOWNLOAD;
        let ret = unsafe {
            trace!("SEND {} <- {:?}", lpstr_to_strref(td.lpIP), td);
            (self.cas_senddata_ex)(td)
        };
        if ret == 0 {
            return Err(anyhow!("error sending PLU data"));
        }
        Ok(scale.products.len() > scale.product_idx as usize + 1)
    }
    pub fn add_scale(&mut self, ip: &str, idx: std::ffi::c_short, should_delete: bool) -> bool {
        let lp_ip = CString::new(ip).unwrap();
        let td = TD_ST_CONNECTION_V02 {
            shScaleID: idx,
            lpIP: lp_ip.as_ptr(),
            wdPort: 20304,
            wdScaleType: DfScaleType::LP.into(),
            wdScaleModel: DfScale::CL5500.into(),
            wdTimeOut: DF_TRANS_TIMEOUT,
            wdRetryCount: DF_TRANS_RETRYCOUNT,
            btCommType: DF_COMMTYPE_TCPIP,
            btTransType: DF_TRANSTYPE_PROC,
            btSocketType: 1,
            btDataType: DfData::CUSTOM,
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
            pReserve: std::ptr::null_mut(),
        };
        let ret = unsafe { (self.cas_add_connection_ex)(td) };
        if ret != 0 {
            debug!("Scale added: {} as {}", ip, idx);
            let mut scale = Scale::new(ip.to_string());
            scale.idx = idx;
            scale.should_delete = should_delete;
            self.scales
                .insert(ip.to_string(), Arc::new(Mutex::new(scale)));
        } else {
            warn!("Adding scale connection failed: {}", ip.to_string());
            return false;
        }
        debug!("IP: {:?}", lp_ip);
        true
    }

    /*
    pub fn ping_scale(&self, scale: &Scale) -> bool {
        let lp_ip = CString::new(scale.ip.to_string()).unwrap();
        let mut td = {
            self.make_transdata(lp_ip.as_ptr(), scale.idx, DfAction::PING, DfData::PING, std::ptr::null_mut(), 0)
        };
        let mut pinginfo = TD_ST_PINGINFO::default();
        td.wdDataSize = std::mem::size_of::<TD_ST_PINGINFO>() as u16;
        td.pData = std::ptr::addr_of_mut!(pinginfo) as *mut std::ffi::c_void;
        let ret = unsafe {(self.cas_senddata_ex)(td)};
        if ret != 0 {
            return true
        } else {
            error!("Ping scale failed: {}", scale.ip.to_string());
        }
        false
    }
    */

    pub fn disconnect_scale(&self, scale: &Scale) -> bool {
        let lp_ip = CString::new(scale.ip.to_string()).unwrap();
        let ret = unsafe { (self.cas_disconnect)(lp_ip.as_ptr(), scale.idx) };
        if ret != 0 {
            return true;
        } else {
            error!("Connect to scale failed: {}", scale.ip.to_string());
        }
        false
    }
    pub fn connect_scale(&self, scale_ip: &String) -> bool {
        let scale = self
            .scales
            .get(&scale_ip.to_string())
            .unwrap()
            .lock()
            .unwrap();
        let lp_ip = CString::new(scale.ip.to_string()).unwrap();
        let ret = unsafe { (self.cas_connect)(lp_ip.as_ptr(), scale.idx) };
        if ret != 0 {
            return true;
        } else {
            error!("Connect to scale failed: {}", scale.ip.to_string());
        }
        false
    }
}

// TBC
pub struct Scales {}

impl Scales {
    pub async fn filtered_items(
        &mut self,
        api: &mut super::api::ITRApi,
        args: &ArgMatches,
    ) -> Result<Vec<super::api::ProductData>> {
        let dump_internal = !args.get_flag("external");
        let re = args.get_one::<String>("upc").unwrap();
        let upc_pat = Regex::new(re)?;
        let filter = |x: &super::api::ProductData| !x.deleted && upc_pat.is_match(&x.upc).unwrap();

        let json = api
            .get(&"/api/ProductsData/GetAllProducts".to_string())
            .await
            .expect("no results from API call");
        let mut items: Vec<super::api::ProductData> = serde_json::from_str(&json)?;
        items = items
            .into_iter()
            .filter(filter)
            .sorted_by(|x, y| (&x.description).cmp(&y.description))
            .sorted_by_key(|x| x.section_id.unwrap_or(0))
            .collect::<Vec<super::api::ProductData>>();

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
                    info!(
                        "PLU assigned {} bad previous was {} - {}",
                        new_plu, plu, item.description
                    );
                    plu_assignment.push(PLUAssignment {
                        upc: item.upc.to_string(),
                        plu: new_plu,
                    });
                    seen_plu.insert(new_plu);
                } else {
                    seen_plu.insert(plu);
                }
            } else {
                let new_plu = next_plu(&mut existing_plu, &item);
                plu_assignment.push(PLUAssignment {
                    upc: item.upc.to_string(),
                    plu: new_plu,
                });
                info!("PLU assigned {} - {}", new_plu, item.description);
                seen_plu.insert(new_plu);
            }
        }
        if plu_assignment.len() > 0 {
            let r = api.set_plu(plu_assignment).await;
            if r.is_err() {
                return Err(r.err().unwrap());
            }
            let json = api
                .get(&"/api/ProductsData/GetAllProducts".to_string())
                .await
                .expect("no results from API call");
            items = serde_json::from_str(&json)?;
            items = items
                .into_iter()
                .filter(filter)
                .sorted_by_key(|x| x.section_id.unwrap_or(0))
                .collect::<Vec<super::api::ProductData>>();
        }

        items = items
            .into_iter()
            .filter(|item| {
                if item.plu.is_none() {
                    return false;
                }
                let plu = item.plu.as_ref().unwrap().parse::<u16>();
                if plu.is_err() {
                    return false;
                }
                if !dump_internal && plu.unwrap() < 1000 {
                    return false;
                }
                if item.upc.get(3..8).is_none() {
                    return false;
                }
                true
            })
            .collect::<Vec<super::api::ProductData>>();
        Ok(items)
    }

    pub async fn send(
        &mut self,
        api: &mut super::api::ITRApi,
        settings: &super::settings::Settings,
        args: &ArgMatches,
    ) -> Result<()> {
        let progress = args.get_flag("progress");
        let delete_plus = args.get_flag("wipe");
        let weighed_items = self.filtered_items(api, args).await?;
        let plufile = args.get_one::<String>("output").unwrap();
        self.build_plu_xlsx(api, &weighed_items, plufile, &args).await?;
        match args.get_one::<String>("scale-file") {
            Some(scalefile) => self.build_scale_xlsx(&weighed_items, scalefile)?,
            _ => (),
        }
        let weighed_items_ref = Arc::new(weighed_items);
        let timeout = match args.get_one::<u32>("timeout-seconds") {
            Some(secs) => secs,
            None => &settings.scales.timeout_seconds,
        };
        let scales: Vec<&String> = match args.get_many::<String>("scale") {
            Some(set) => set.collect::<Vec<_>>().into(),
            None => {
                if args.get_flag("no-scales") {
                    vec![]
                } else {
                    settings.scales.addresses.iter().collect::<Vec<_>>().into()
                }
            }
        };
        if scales.len() > 0 {
            let mut idx: std::ffi::c_short = 1;
            for scale in scales.into_iter() {
                let mut cas = DLLAPI.lock().unwrap();
                if cas.add_scale(scale, idx, delete_plus) {
                    idx = idx + 1;
                    debug!("Added scale: {:?}", scale);
                } else {
                    error!("Error adding scale {}", scale);
                }
            }
            let ips: Vec<String> = {
                let cas = DLLAPI.lock().unwrap();
                cas.scales.keys().map(|k| k.to_string()).collect()
            };

            for scale_ip in ips.iter() {
                let cas = DLLAPI.lock().unwrap();
                let mut scale = cas.scales.get(scale_ip).unwrap().lock().unwrap();
                scale.products = weighed_items_ref.clone();
            }

            for scale_ip in ips.iter() {
                debug!("Connecting scale: {}", scale_ip);
                let cas = DLLAPI.lock().unwrap();
                if cas.connect_scale(scale_ip) {
                } else {
                    error!("Connect to scale failed {}", scale_ip);
                }
            }

            let start = Instant::now();
            loop {
                let mut done = true;
                let mut scale_status = vec!["\rProgress".to_string()];
                for scale_ip in ips.iter() {
                    let cas = DLLAPI.lock().unwrap();
                    let mut scale = cas.scales.get(scale_ip).unwrap().lock().unwrap();
                    if scale.complete() {
                        if !scale.notified {
                            scale.notified = true;
                            info!("Scale {} is done.", scale.ip);
                        }
                    } else {
                        done = false;
                    }
                    scale_status.push(scale.status_str())
                }
                if progress {
                    print!("{}", scale_status.join(" "));
                    io::stdout().flush().unwrap();
                }
                if done {
                    break;
                }
                if *timeout != 0 && start.elapsed().as_secs() as u32 > *timeout {
                    error!(
                        "Operation timed out after {} seconds.",
                        start.elapsed().as_secs()
                    );
                    return Err(anyhow!("timeout"));
                }
                thread::sleep(time::Duration::from_secs(1));
            }
        }
        Ok(())
    }

    pub async fn build_plu_xlsx(
        &mut self,
        api: &mut super::api::ITRApi,
        weighed_items: &Vec<ProductData>,
        filename: &String,
        args: &ArgMatches,
    ) -> Result<()> {
        let qlimit = args.get_one::<f32>("at-least").unwrap();
        let by_section = args.get_flag("by-section");
        let mut workbook = Workbook::new();
        let bold_format = Format::new().set_bold();
        let decimal_format = Format::new().set_num_format("0.00");

        let sections: HashMap<i32, String> = api
            .get_sections()
            .await?
            .iter()
            .map(|s| (s.id as i32, s.name.to_owned()))
            .collect();

        const FIELDS: [&str; 3] = ["PLU", "Name", "Price"];

        struct XSection {
            name: String,
            row: u32,
        }
        let mut sheets: HashMap<i32, XSection> = HashMap::new();
        for item in weighed_items {
            if item.quantity_on_hand.unwrap_or(0.0) <= *qlimit {
                continue;
            }
            let section_id = match by_section {
                true => item.section_id.unwrap_or(-1),
                false => 0,
            };
            let unknown_section: String = match by_section {
                true => String::from("Unknown"),
                false => String::from("Products"),
            };
            let section_name = sections.get(&section_id).unwrap_or(&unknown_section);
            let section = match sheets.get_mut(&section_id) {
                Some(sheet) => sheet,
                None => {
                    let worksheet = workbook.add_worksheet().set_name(section_name)?;
                    sheets.insert(
                        section_id,
                        XSection {
                            name: worksheet.name(),
                            row: 1,
                        },
                    );
                    let section = sheets.get_mut(&section_id).unwrap();
                    for idx in 0..FIELDS.len() {
                        worksheet.write_with_format(
                            0,
                            idx.try_into().unwrap(),
                            FIELDS[idx],
                            &bold_format,
                        )?;
                    }
                    section
                }
            };
            let worksheet = workbook.worksheet_from_name(&section.name)?;
            let plu = item.plu.as_ref().unwrap().parse::<u16>().unwrap();
            worksheet.write_number(section.row, 0, plu)?;
            worksheet.write_string(section.row, 1, &item.description)?;
            worksheet.write_number_with_format(
                section.row,
                2,
                item.normal_price,
                &decimal_format,
            )?;
            section.row += 1;
        }

        workbook.save(filename)?;

        Ok(())
    }
    pub fn build_scale_xlsx(
        &mut self,
        weighed_items: &Vec<ProductData>,
        filename: &String,
    ) -> Result<()> {
        const FIELDS: [&str; 19] = [
            "Department No",
            "PLU No",
            "Name1",
            "Name2",
            "Itemcode",
            "Unit Price",
            "Origin No",
            "Label No",
            " Category No",
            "Direct Ingredient",
            "Sell By Time",
            "Sell By Date",
            "Packed Date",
            "Group No",
            "Unit Weight",
            "Nutrifact No",
            "PLU Type",
            "Packed Time",
            "Update Date",
        ];

        let mut workbook = Workbook::new();
        let bold_format = Format::new().set_bold();
        let decimal_format = Format::new().set_num_format("0.00");
        let date_format = Format::new().set_num_format("yyyy-mm-dd");

        let date = Local::now().naive_local();

        let worksheet = workbook.add_worksheet();
        for idx in 0..FIELDS.len() {
            worksheet.write_with_format(0, idx.try_into().unwrap(), FIELDS[idx], &bold_format)?;
        }

        let mut row: u32 = 1;
        for item in weighed_items {
            worksheet.write_number(row, 0, item.department_id)?;
            let plu = item.plu.as_ref().unwrap().parse::<u16>().unwrap();
            worksheet.write_number(row, 1, plu)?;
            worksheet.write_string(row, 2, &item.description)?;
            // 3 Name2 (blank)
            let itemcode_str = item.upc.get(3..8).unwrap();
            let itemcode = itemcode_str
                .trim_start_matches('0')
                .parse::<u32>()
                .or::<u32>(Ok(0))
                .unwrap();

            worksheet.write_number(row, 4, itemcode)?;
            worksheet.write_number_with_format(row, 5, item.normal_price, &decimal_format)?;
            worksheet.write_number(row, 6, 0)?; // Origin
            worksheet.write_number(row, 7, 0)?; // Label ID
            worksheet.write_number(row, 8, 0)?; // Category
            if item.second_description.is_some() {
                let ingredients = item.second_description.as_ref().unwrap();
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
            info!(
                "Writing: [{}] {} : {} : {}",
                plu, item.upc, item.description, item.normal_price
            );
        }

        workbook.save(filename)?;

        Ok(())
    }
}
