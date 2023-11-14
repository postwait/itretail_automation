mod internal;

use clap::{Arg, ArgAction, Command};
use log::*;
use simplelog::*;
use std::fs::OpenOptions;
use std::{env, fs};

fn main() {
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
        .subcommand(
            Command::new("loyalty").arg(
                Arg::new("days")
                    .long("days")
                    .short('d')
                    .action(ArgAction::Set)
                    .value_name("DAYS")
                    .value_parser(clap::value_parser!(u32))
                    .default_value("180"),
            ),
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
                .arg(
                    Arg::new("output")
                        .long("output")
                        .short('o')
                        .action(ArgAction::Set)
                        .value_name("FILE")
                        .default_value("labels.xlsx"),
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
                        .conflicts_with("pull")
                        .default_value("tvscreen.png"),
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
                        .conflicts_with("menu")
                        .conflicts_with("output"),
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

    let auth_err = api.auth().err();
    if auth_err.is_some() {
        let err = auth_err.unwrap();
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
        panic!("{}", err)
    }

    match m.subcommand() {
        Some(("loyalty", scmd)) => {
            let r = internal::loyalty::apply_discounts(&mut api, &settings, &scmd);
            if r.is_err() {
                error!("Error reading electronic journal: {}", r.err().unwrap());
                std::process::exit(exitcode::SOFTWARE);
            }
        }
        Some(("scale-export", scmd)) => {
            let mut scale_file = internal::cas::Scales {};
            let r = scale_file.send(&mut api, &settings, &scmd);
            if r.is_err() {
                error!("Error: {}", r.err().unwrap());
                std::process::exit(exitcode::SOFTWARE);
            }
            std::process::exit(exitcode::OK);
        }
        Some(("label-export", scmd)) => {
            let filename = scmd.get_one::<String>("output").unwrap();
            let mut label_file = internal::label::create_label_file(filename);
            let results = api
                .get(&"/api/ProductsData/GetAllProducts".to_string())
                .expect("no results from API call");
            let r = label_file.build_from_itretail_products(&results, &scmd);
            if r.is_err() {
                error!("{}", r.err().unwrap());
                std::process::exit(exitcode::SOFTWARE);
            }
            std::process::exit(exitcode::OK);
        }
        Some(("mailchimp-sync", scmd)) => {
            let r = internal::customer::mailchimp_sync(&mut api, &settings, &scmd);
            if r.is_err() {
                error!("{:?}", r.err().unwrap());
                std::process::exit(exitcode::SOFTWARE);
            }
            std::process::exit(exitcode::OK);
        }
        Some(("tvmenu", scmd)) => {
            let (menu_file, output_file) = match scmd.get_one::<String>("pull") {
                Some(cat) => {
                    let r = internal::tvmenu::make_listing(&mut api, &scmd);
                    if r.is_err() {
                        error!(
                            "Error constructing menu from IT Retail: {}",
                            r.err().unwrap()
                        );
                        std::process::exit(exitcode::SOFTWARE);
                    }
                    (r.unwrap(), String::from(cat) + ".png")
                }
                None => (
                    scmd.get_one::<String>("menu").unwrap().to_string(),
                    scmd.get_one::<String>("output").unwrap().to_string(),
                ),
            };
            let menu_txt = fs::read_to_string(menu_file).expect("Could not open file.");
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
                let r = api.set_plu(vec![plu_assignment]);
                if r.is_ok() {
                    std::process::exit(exitcode::OK);
                }
                error!("Error setting PLU: {}", r.err().unwrap());
                std::process::exit(exitcode::SOFTWARE);
            }
        }
        _ => {
            println!("{}", help);
            std::process::exit(exitcode::USAGE);
        }
    }
}
