mod internal;

use tokio;
use chrono::{DateTime, Local, NaiveDateTime, NaiveDate, ParseError, TimeZone};
use clap::{Arg, ArgAction, Command};
use log::*;
use simplelog::*;
use std::fs::OpenOptions;
use std::{env, fs, thread, time};

fn parse_timestamp(arg: &str) -> Result<NaiveDateTime,ParseError> {
    let dt = NaiveDateTime::parse_from_str(arg, "%Y-%m-%dT%H:%M:%S");
    dt
}
fn parse_date(arg: &str) -> Result<NaiveDate,ParseError> {
    let dt = NaiveDate::parse_from_str(arg, "%Y-%m-%d");
    dt
}

#[cfg(windows)]
async fn scale_export(mut api: &mut internal::api::ITRApi, settings: &internal::settings::Settings, scmd: &clap::ArgMatches) {
    let mut scale_file = internal::cas::Scales {};
    let r = scale_file.send(&mut api, &settings, &scmd).await;
    if r.is_err() {
        error!("Error: {}", r.err().unwrap());
        std::process::exit(exitcode::SOFTWARE);
    }
    std::process::exit(exitcode::OK);
}

#[cfg(not(windows))]
#[allow(unused_variables)]
fn scale_export(api: &mut internal::api::ITRApi, settings: &internal::settings::Settings, scmd: &clap::ArgMatches) {
    error!("CAS Scale integration only supported on Windows.")
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::new("itretail_automation")
        .author("Theo Schlossnagle, jesus@lethargy.org")
        .version("0.0.1")
        .about("Automates certain tasks against IT Retail")
        .arg(
            Arg::new("log-level")
                .long("log-level")
                .short('v')
                .action(ArgAction::Set)
                .value_name("off,error,warn,info,debug,trace")
                .default_value("info"),
        )
        .arg(
            Arg::new("log-file")
                .long("log-file")
                .short('l')
                .action(ArgAction::Set)
                .value_name("FILE"),
        )
        .arg(Arg::new("username").long("username").short('u'))
        .arg(Arg::new("password").long("password").short('p'))
        .arg(Arg::new("leusername").long("leusername"))
        .arg(Arg::new("lepassword").long("lepassword"))
        .subcommand(
            Command::new("loyalty")
            .arg(Arg::new("days")
                    .long("days")
                    .short('d')
                    .action(ArgAction::Set)
                    .value_name("DAYS")
                    .value_parser(clap::value_parser!(u32))
                    .default_value("180"))
            .arg(Arg::new("noop")
                    .short('n')
                    .action(ArgAction::SetTrue)
                    .num_args(0))
        )
        .subcommand(
            Command::new("sidedb-sync")
                .arg(Arg::new("customers")
                         .long("customers")
                         .action(ArgAction::SetTrue)
                         .num_args(0))
                .arg(Arg::new("customers-square")
                         .long("customers-square")
                         .action(ArgAction::SetTrue)
                         .num_args(0))
                .arg(Arg::new("customers-full")
                         .long("customers-full")
                         .action(ArgAction::SetTrue)
                         .num_args(0))
                .arg(Arg::new("transactions")
                         .long("transactions")
                         .action(ArgAction::SetTrue)
                         .num_args(0))
                .arg(Arg::new("start")
                         .long("start")
                         .action(ArgAction::Set)
                         .value_name("DATETIME")
                         .value_parser(parse_timestamp))
                .arg(Arg::new("end")
                         .long("end")
                         .action(ArgAction::Set)
                         .value_name("DATETIME")
                         .value_parser(parse_timestamp))
                .arg(Arg::new("products")
                         .long("products")
                         .action(ArgAction::SetTrue)
                         .num_args(0))
                .arg(Arg::new("products-square")
                         .long("products-square")
                         .action(ArgAction::SetTrue)
                         .num_args(0))
                .arg(Arg::new("inventory-square")
                         .long("inventory-square")
                         .action(ArgAction::SetTrue)
                         .num_args(0))
                .arg(Arg::new("orders")
                         .long("orders")
                         .action(ArgAction::SetTrue)
                         .num_args(0))
                .arg(Arg::new("period")
                         .long("period")
                         .short('t')
                         .action(ArgAction::Set)
                         .value_name("SECONDS")
                         .value_parser(clap::value_parser!(u32))
                         .default_value("0"))
        )
        .subcommand(
            Command::new("le-orders")
        )
        .subcommand(
            Command::new("set-plu")
                .arg(Arg::new("upc").required(true))
                .arg(Arg::new("plu").required(true)),
        )
        .subcommand(
            Command::new("scale-export")
                .arg(
                    Arg::new("output")
                        .long("output")
                        .short('o')
                        .action(ArgAction::Set)
                        .value_name("FILE")
                        .default_value("PLU.xlsx"),
                )
                .arg(
                    Arg::new("by-section")
                        .long("by-section")
                        .action(ArgAction::SetTrue)
                        .num_args(0),
                )
                .arg(
                    Arg::new("scale-file")
                        .long("scale-file")
                        .action(ArgAction::Set)
                        .value_name("FILE"),
                )
                .arg(
                    Arg::new("internal")
                        .long("internal")
                        .conflicts_with("external")
                        .num_args(0)
                        .action(ArgAction::SetTrue)
                        .hide(true),
                )
                .arg(
                    Arg::new("external")
                        .long("external")
                        .conflicts_with("internal")
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("upc")
                        .long("upc")
                        .action(ArgAction::Set)
                        .value_name("Regex")
                        .default_value("^002"),
                )
                .arg(
                    Arg::new("scale")
                        .long("scale")
                        .conflicts_with("no-scales")
                        .action(ArgAction::Append)
                        .value_name("IP Address"),
                )
                .arg(
                    Arg::new("no-scales")
                        .long("no-scales")
                        .short('n')
                        .conflicts_with("scale")
                        .action(ArgAction::SetTrue)
                        .num_args(0),
                )
                .arg(
                    Arg::new("timeout-seconds")
                        .long("timeout-seconds")
                        .short('w')
                        .action(ArgAction::Set)
                        .value_name("seconds")
                        .value_parser(clap::value_parser!(u32)),
                )
                .arg(
                    Arg::new("wipe")
                        .long("wipe")
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("at-least")
                        .long("at-least")
                        .short('q')
                        .action(ArgAction::Set)
                        .value_name("weight/qty")
                        .value_parser(clap::value_parser!(f32))
                        .default_value("-10000000.0"),
                )
                .arg(
                    Arg::new("progress")
                        .long("progress")
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("label-export")
                .arg(Arg::new("as-of")
                         .long("as-of")
                         .action(ArgAction::Set)
                         .value_name("DATE")
                         .value_parser(parse_date))
                .arg(
                    Arg::new("output")
                        .long("output")
                        .short('o')
                        .action(ArgAction::Set)
                        .value_name("FILE")
                        .default_value("labels.xlsx"),
                )
                .arg(
                    Arg::new("sheets")
                        .long("sheets")
                        .num_args(0)
                        .action(ArgAction::SetTrue)
                )
                .arg(
                    Arg::new("upc")
                        .long("upc")
                        .action(ArgAction::Set)
                        .value_name("Regex")
                        .default_value("^(?!002)"),
                )
                .arg(
                    Arg::new("name")
                        .long("name")
                        .action(ArgAction::Set)
                        .value_name("Regex")
                        .default_value("."),
                )
                .arg(
                    Arg::new("vendor")
                        .long("vendor")
                        .action(ArgAction::Set)
                        .value_name("Number")
                        .default_value("0"),
                )
                .arg(
                    Arg::new("at-least")
                        .long("at-least")
                        .short('q')
                        .action(ArgAction::Set)
                        .value_name("weight/qty")
                        .value_parser(clap::value_parser!(f32))
                        .default_value("-10000000.0"),
                )
                .arg(
                    Arg::new("headers")
                        .long("headers")
                        .action(ArgAction::Set)
                        .default_value("name,plu,upc,price")
                ),
        )
        .subcommand(
            Command::new("get-plu")
                .arg(
                    Arg::new("upc")
                        .long("upc")
                        .action(ArgAction::Set)
                        .value_name("Regex")
                        .default_value("^002"),
                )
                .arg(
                    Arg::new("name")
                        .long("name")
                        .action(ArgAction::Set)
                        .value_name("Regex")
                        .default_value("."),
                )
                .arg(
                    Arg::new("vendor")
                        .long("vendor")
                        .action(ArgAction::Set)
                        .value_name("Number")
                        .default_value("0"),
                )
                .arg(
                    Arg::new("at-least")
                        .long("at-least")
                        .short('q')
                        .action(ArgAction::Set)
                        .value_name("weight/qty")
                        .value_parser(clap::value_parser!(f32))
                        .default_value("-10000000.0"),
                ),
        )
        .subcommand(
            Command::new("mailchimp-sync")
                .arg(
                    Arg::new("mc_token")
                        .long("mc_token")
                        .action(ArgAction::Set)
                        .value_name("API_TOKEN"),
                )
                .arg(
                    Arg::new("listid")
                        .long("listid")
                        .action(ArgAction::Set)
                        .value_name("LISTID"),
                )
                .arg(
                    Arg::new("email")
                        .long("email")
                        .action(ArgAction::Set)
                        .value_name("EMAIL")
                ),
        )
        .subcommand(
            Command::new("tvmenu")
                .arg(
                    Arg::new("backdrop")
                        .long("backdrop")
                        .action(ArgAction::Set)
                        .value_name("FILENAME"),
                )
                .arg(
                    Arg::new("title")
                        .long("title")
                        .action(ArgAction::Set)
                        .value_name("TITLE"),
                )
                .arg(
                    Arg::new("menu")
                        .long("menu")
                        .action(ArgAction::Set)
                        .value_name("FILENAME")
                        .conflicts_with("pull")
                        .default_value("menu.txt"),
                )
                .arg(
                    Arg::new("output")
                        .long("output")
                        .action(ArgAction::Set)
                        .value_name("FILENAME")
                )
                .arg(
                    Arg::new("invert")
                        .long("invert")
                        .short('i')
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("pull")
                        .long("pull")
                        .short('u')
                        .action(ArgAction::Set)
                        .conflicts_with("menu"),
                ),
        );
    let help = cmd.render_help();
    let m = cmd.get_matches();

    let res = internal::settings::Settings::new();
    if res.is_err() {
        panic!("Failed to read configuration file: {}", res.err().unwrap());
    }
    let settings = res.ok().unwrap();

    let ll = m.get_one::<String>("log-level").unwrap();
    let llevel = match ll.as_str() {
        "off" => LevelFilter::Off,
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        lvl => {
            println!("Unknown log-level {}, using warn.", lvl);
            LevelFilter::Warn
        }
    };
    let lconfig = ConfigBuilder::new()
        .set_level_color(Level::Error, Some(Color::Red))
        .set_level_color(Level::Warn, Some(Color::Magenta))
        .set_target_level(LevelFilter::Error)
        .set_time_format_rfc3339()
        .set_time_level(LevelFilter::Error)
        .set_max_level(LevelFilter::Error)
        .build();
    let mut loggers: Vec<Box<dyn SharedLogger>> = vec![];
    if let Some(logfile) = m.get_one::<String>("log-file") {
        loggers.push(WriteLogger::new(
            llevel,
            lconfig.clone(),
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(logfile)
                .unwrap(),
        ));
    } else {
        loggers.push(TermLogger::new(
            llevel,
            lconfig.clone(),
            TerminalMode::Mixed,
            ColorChoice::Always,
        ));
    }
    CombinedLogger::init(loggers).unwrap();

    if let Some(cli_lepass) = m.get_one::<String>("lepassword") {
        env::set_var("LOCALEXPRESS_PASSWORD", cli_lepass)
    } else if settings.localexpress.password.len() > 0 {
        env::set_var("LOCALEXPRESS_PASSWORD", settings.localexpress.password.to_string());
    }
    if let Some(cli_leuser) = m.get_one::<String>("leusername") {
        env::set_var("LOCALEXPRESS_USERNAME", cli_leuser)
    } else if settings.itretail.username.len() > 0 {
        env::set_var("LOCALEXPRESS_USERNAME", settings.localexpress.username.to_string());
    }

    if let Some(cli_pass) = m.get_one::<String>("password") {
        env::set_var("ITRETAIL_PASSWORD", cli_pass)
    } else if settings.itretail.password.len() > 0 {
        env::set_var("ITRETAIL_PASSWORD", settings.itretail.password.to_string());
    }
    if let Some(cli_user) = m.get_one::<String>("username") {
        env::set_var("ITRETAIL_USERNAME", cli_user)
    } else if settings.itretail.username.len() > 0 {
        env::set_var("ITRETAIL_USERNAME", settings.itretail.username.to_string());
    }
    if settings.itretail.store_id.len() > 0 {
        env::set_var("ITRETAIL_STOREID", settings.itretail.store_id.to_string());
    }

    let handle = internal::api::create_api();
    if handle.is_err() {
        panic!("{}", handle.err().unwrap())
    }
    let mut api = handle.ok().unwrap();

    let auth_result = api.auth().await;
    if let Some(err) = auth_result.err() {
        if err.to_string().contains("no username provided") {
            println!(
                r"A username is needed and is not present in the environment. Add one.

            On Windows:
                $env:ITRETAIL_USERNAME = 'user@example.com'

            On Unix:
                export ITRETAIL_USERNAME='user@example.com'
"
            )
        }
        if err.to_string().contains("no password provided") {
            println!(
                r"A password is needed and is not present in the environment. Add one.

            On Windows:
                $env:ITRETAIL_PASSWORD = 'password'

            On Unix:
                export ITRETAIL_PASSWORD='password'
"
            )
        }
        //panic!("{}", err)
    }

    match m.subcommand() {
        Some(("loyalty", scmd)) => {
            let mut sidedb = internal::sidedb::make_sidedb(settings.clone()).await.unwrap();
            let r = internal::loyalty::apply_discounts(&mut api, &mut sidedb, &settings, &scmd).await;
            if r.is_err() {
                error!("Error reading electronic journal: {}", r.err().unwrap());
                std::process::exit(exitcode::SOFTWARE);
            }
        }
        Some(("scale-export", scmd)) => { scale_export(&mut api, &settings, &scmd).await }
        Some(("get-plu", scmd)) => {
            let mut label_file = internal::label::create_label_file(&"".to_owned());
            let results = api
                .get(&"/api/ProductsData/GetAllProducts".to_string())
                .await
                .expect("no results from API call");
            let r = label_file.output_from_itretail_products(&results, &scmd);
            if r.is_err() {
                error!("{}", r.err().unwrap());
                std::process::exit(exitcode::SOFTWARE);
            }
            std::process::exit(exitcode::OK);
        }
        Some(("label-export", scmd)) => {
            let filename = scmd.get_one::<String>("output").unwrap();
            let asof = scmd.get_one::<NaiveDate>("as-of");
            let mut label_file = internal::label::create_label_file(filename);
            let mut sidedb = internal::sidedb::make_sidedb(settings).await.unwrap();
            let items = sidedb.get_products(asof).await.unwrap();
            let r = label_file.build_from_itretail_products(&items, &scmd);
            if r.is_err() {
                error!("{}", r.err().unwrap());
                std::process::exit(exitcode::SOFTWARE);
            }
            std::process::exit(exitcode::OK);
        }
        Some(("mailchimp-sync", scmd)) => {
            let r = internal::customer::mailchimp_sync(&mut api, &settings, &scmd).await;
            if r.is_err() {
                error!("{:?}", r.err().unwrap());
                std::process::exit(exitcode::SOFTWARE);
            }
            std::process::exit(exitcode::OK);
        }
        Some(("tvmenu", scmd)) => {
            let (menu_file, output_file) = match scmd.get_one::<String>("pull") {
                Some(cat) => {
                    let r = internal::tvmenu::make_listing(&mut api, &scmd).await;
                    if r.is_err() {
                        error!(
                            "Error constructing menu from IT Retail: {}",
                            r.err().unwrap()
                        );
                        std::process::exit(exitcode::SOFTWARE);
                    }
                    (r.unwrap(), scmd.get_one::<String>("output").unwrap_or(&(String::from(cat) + ".png")).to_string())
                }
                None => (
                    scmd.get_one::<String>("menu").unwrap().to_string(),
                    scmd.get_one::<String>("output").unwrap_or(&"tvscreen.png".to_string()).to_string(),
                ),
            };
            let mut menu_txt = match scmd.get_one::<String>("title") {
                Some(title) => title.to_owned() + "\n\n",
                None => "".to_string()
            };
            menu_txt.push_str(&fs::read_to_string(menu_file).expect("Could not open file."));
            let r = internal::tvmenu::make_menu(
                &output_file,
                &menu_txt,
                scmd.get_one::<String>("backdrop"),
                scmd.get_flag("invert"),
            );
            if r.is_err() {
                error!("Error creating TV menu image: {}", r.err().unwrap());
                std::process::exit(exitcode::SOFTWARE);
            }
            std::process::exit(exitcode::OK);
        }
        Some(("set-plu", scmd)) => {
            let upc = scmd.get_one::<String>("upc");
            let plus = scmd.get_one::<String>("plu");
            if upc.is_none()
                || upc.unwrap().len() != 13
                || plus.is_none()
                || plus.unwrap().len() != 4
            {
                error!("Error, upc {:?} (should be 13 digits) or plu {:?} (should be 4 digits) invalid", upc, plus);
                std::process::exit(exitcode::USAGE);
            } else {
                let plu = u16::from_str_radix(plus.unwrap(), 10).ok().unwrap();
                let plu_assignment = internal::api::PLUAssignment {
                    upc: upc.unwrap().to_string(),
                    plu: plu,
                };
                let r = api.set_plu(vec![plu_assignment]).await;
                if r.is_ok() {
                    std::process::exit(exitcode::OK);
                }
                error!("Error setting PLU: {}", r.err().unwrap());
                std::process::exit(exitcode::SOFTWARE);
            }
        }
        Some(("le-orders", _scmd)) => {
            let lehandle = internal::localexpress::create_api();
            if lehandle.is_err() {
                panic!("{}", lehandle.err().unwrap())
            }
            let mut leapi = lehandle.ok().unwrap();
            match leapi.auth().await  {
                Err(err) => {
                    error!("Error authenticating with LocalExpress: {}", err);
                },
                _ => {}
            }
            let r = leapi.get_current_orders().await;
            if r.is_ok() {
                let orders = r.unwrap();
                let new_order_cnt = orders.iter().fold(0, |a,x| { if x.status == "new" { a + 1 } else { a + 0 } });
                let today = Local::now().date_naive();
                let todays_unfinished_cnt = orders.iter().fold(0, |a, x| {
                    if x.delivery_date == today && x.active() {
                        a + 1
                    }
                    else {
                        a + 0
                    }

                });
                debug!("{:#?}", orders);
                info!("New Orders: {}", new_order_cnt);
                info!("Today's Unfinished Orders: {}", todays_unfinished_cnt);
                let mut light1 = internal::tasmota::new_light(settings.tasmota.light1);
                match light1.power(todays_unfinished_cnt > 0).await {
                    Err(e) => error!("Error actuating light1: {}", e.to_string()),
                    Ok(_) => {}
                }
                let mut light2 = internal::tasmota::new_light(settings.tasmota.light2);
                match light2.power(new_order_cnt > 0).await {
                    Err(e) => error!("Error actuating light2: {}", e.to_string()),
                    Ok(_) => {}
                }
                std::process::exit(exitcode::OK);
            }
            error!("Error fetching LocalExpress orders: {}", r.err().unwrap());
            std::process::exit(exitcode::SOFTWARE);
        }
        Some(("sidedb-sync", scmd)) => {
            let mut sidedb = internal::sidedb::make_sidedb(settings.clone()).await.unwrap();
            let period = *scmd.get_one::<u32>("period").unwrap();
            let do_products = scmd.get_flag("products");
            let do_square_products = scmd.get_flag("products-square");
            let do_square_inventory = scmd.get_flag("inventory-square");
            let do_customers = scmd.get_flag("customers");
            let do_square_customers = scmd.get_flag("customers-square");
            let full_customer = scmd.get_flag("customers-full");
            let do_txns = scmd.get_flag("transactions");
            let do_orders = scmd.get_flag("orders");
            let do_all = !do_txns && !do_orders && !do_products && !do_customers && !full_customer && !do_square_customers && !do_square_products;

            let mut progress = false;
            info!("Starting sync process.");

            loop {
                if do_customers || full_customer || do_all {
                    info!("Starting customer sync.");
                    let r= api.get_customers().await;
                    if r.is_err() {
                        error!("Error fetching IT Retail customers: {}", r.err().unwrap());
                        std::process::exit(exitcode::SOFTWARE);
                    } else {
                        let ro = 
                        if full_customer {
                            let mut full_customers: Vec<internal::api::Customer> = vec![];
                            for skel_c in &r.unwrap() {
                                if let Some(full_c) = api.get_customer(&skel_c.id).await? {
                                    full_customers.push(full_c);
                                }
                            }
                            sidedb.store_customers(full_customers.into_iter()).await
                        } else {
                            sidedb.store_customers(r.unwrap().into_iter()).await
                        };
                        if ro.is_err() {
                            error!("Failed to store IT Retail customers: {}", ro.err().unwrap());
                            std::process::exit(exitcode::SOFTWARE);
                        } else {
                            info!("Pushed {} IT Retail customers.", ro.unwrap());
                        }
                    }
                }

                if do_square_customers /* || do_all */ {
                    info!("Starting square customer sync.");
                    let r = internal::square::square_connect_create(&settings);
                    match r.sync_customers_with_sidedb(&mut sidedb).await {
                        Ok(v) => info!("{:?}", v),
                        Err(e) => error!("Square customer sync error: {}", e)
                    }
                }

                if do_products || do_all {
                    info!("Starting product sync.");
                    let r = api.get_tax().await;
                    if r.is_err() {
                        error!("Error fetching IT Retail taxes: {}", r.err().unwrap());
                        std::process::exit(exitcode::SOFTWARE);
                    } else {
                        let taxes = r.unwrap();
                        let ro = sidedb.store_taxes_itr(taxes.iter()).await;
                        if ro.is_err() {
                            error!("Failed to store IT Retail taxes: {}", ro.err().unwrap());
                            std::process::exit(exitcode::SOFTWARE);
                        } else {
                            info!("Pushed {} IT Retail taxes.", ro.unwrap());
                        }
                    }

                    let r= api.get_products().await;
                    if r.is_err() {
                        error!("Error fetching IT Retail products: {}", r.err().unwrap());
                        std::process::exit(exitcode::SOFTWARE);
                    } else {
                        let products = r.unwrap();
                        let ro = sidedb.store_products(products.iter()).await;
                        if ro.is_err() {
                            error!("Failed to store IT Retail products: {}", ro.err().unwrap());
                            std::process::exit(exitcode::SOFTWARE);
                        } else {
                            info!("Pushed {} IT Retail products.", ro.unwrap());
                        }
                    }
                    progress = true;
                }

                if do_square_products || do_square_inventory /* || do_all */ {
                    info!("Starting square product sync.");
                    let r = internal::square::square_connect_create(&settings);
                    match r.sync_products_with_sidedb(&mut sidedb, do_square_inventory || do_all).await {
                        Ok(v) => info!("{:?}", v),
                        Err(e) => error!("Square customer sync error: {}", e)
                    }
                }

                if do_txns || do_all {
                    info!("Starting transaction sync.");
                    let start_ndt = scmd.get_one::<NaiveDateTime>("start");
                    let sdtl: DateTime<Local>;
                    let start = match start_ndt {
                        Some(dt) => {
                            sdtl = Local.from_local_datetime(dt).unwrap();
                            Some(&sdtl)
                        },
                        None => None,
                    };
                    let end_ndt = scmd.get_one::<NaiveDateTime>("end");
                    let edtl: DateTime<Local>;
                    let end = match end_ndt {
                        Some(dt) => {
                            edtl = Local.from_local_datetime(dt).unwrap();
                            Some(&edtl)
                        },
                        None => None,
                    };
                    let r = api.get_transactions_details(start, end).await;
                    if r.is_err() {
                        error!("Error fetching IT Retail transactions: {}", r.err().unwrap());
                        std::process::exit(exitcode::SOFTWARE);
                    } else {
                        let txns = r.unwrap();
                        let ro = sidedb.store_txns(txns.iter()).await;
                        if ro.is_err() {
                            error!("Failed to store IT Retail transactions: {}", ro.err().unwrap());
                            std::process::exit(exitcode::SOFTWARE);
                        } else {
                            info!("Pushed {} IT Retail transactions.", ro.unwrap());
                        }
                    }
                }
    
                if do_orders || do_all {
                    info!("Starting LocalExpress orders sync.");
                    let mut auth_error = false;
                    loop {
                        let lehandle = internal::localexpress::create_api();
                        if lehandle.is_err() {
                            panic!("{}", lehandle.err().unwrap())
                        }
                        let mut leapi = lehandle.ok().unwrap();
                        match leapi.auth().await  {
                            Err(err) => {
                                error!("Error authenticating with LocalExpress: {}", err);
                                std::process::exit(exitcode::SOFTWARE);
                            },
                            _ => {}
                        }
                        let r = leapi.get_orders().await;
                        if r.is_err() {
                            if !auth_error && r.as_ref().err().unwrap().to_string().eq("Unauthorized") {
                                warn!("Reauthorizing LocalExpress: {}", r.as_ref().err().unwrap());
                                auth_error = true;
                                continue;
                            }
                            error!("Error fetching LocalExpress orders: {}", r.err().unwrap());
                            std::process::exit(exitcode::SOFTWARE);
                        } else {
                            let ro = sidedb.store_orders(r.unwrap().iter()).await;
                            if ro.is_err() {
                                error!("Failed to store LE orders: {}", ro.err().unwrap());
                                std::process::exit(exitcode::SOFTWARE);
                            } else {
                                info!("Pushed {} LE orders.", ro.unwrap());
                            }
                        }
                        break;
                    }
                    progress = true;
                }

                if period <= 0 || !progress {
                    break;
                }
                thread::sleep(time::Duration::from_secs(period.into()));
            }
            drop(sidedb);
            std::process::exit(exitcode::OK);
        }
        _ => {
            println!("{}", help);
            std::process::exit(exitcode::USAGE);
        }
    }
    Ok(())
}
