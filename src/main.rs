mod internal;

use clap::{Command,Arg,ArgAction};
use std::{env, fs};

fn main() {
    let mut cmd = Command::new("itretail_automation")
        .author("Theo Schlossnagle, jesus@lethargy.org")
        .version("0.0.1")
        .about("Automates certain tasks against IT Retail")
        .arg(Arg::new("username").long("username").short('u'))
        .arg(Arg::new("password").long("password").short('p'))
        .subcommand(Command::new("set-plu")
            .arg(Arg::new("upc").required(true))
            .arg(Arg::new("plu").required(true))
        )
        .subcommand(Command::new("scale-export")
            .arg(Arg::new("output").long("output").short('o').action(ArgAction::Set).value_name("FILE").default_value("scale.xlsx"))
            .arg(Arg::new("internal").long("internal").num_args(0).action(ArgAction::SetTrue))
            .arg(Arg::new("upc").long("upc").action(ArgAction::Set).value_name("Regex").default_value("^002"))
        )
        .subcommand(Command::new("label-export")
            .arg(Arg::new("output").long("output").short('o').action(ArgAction::Set).value_name("FILE").default_value("labels.xlsx"))
            .arg(Arg::new("upc").long("upc").action(ArgAction::Set).value_name("Regex").default_value("^(?!002)"))
            .arg(Arg::new("name").long("name").action(ArgAction::Set).value_name("Regex").default_value("."))
            .arg(Arg::new("vendor").long("vendor").action(ArgAction::Set).value_name("Number").default_value("0"))
        )
        .subcommand(Command::new("mailchimp-sync")
            .arg(Arg::new("mc_token").long("mc_token").action(ArgAction::Set).value_name("API_TOKEN"))
            .arg(Arg::new("listid").long("listid").action(ArgAction::Set).value_name("LISTID"))
        )
        .subcommand(Command::new("tvmenu")
            .arg(Arg::new("backdrop").long("backdrop").action(ArgAction::Set).value_name("FILENAME"))
            .arg(Arg::new("menu").long("menu").action(ArgAction::Set).value_name("FILENAME").default_value("menu.txt"))
            .arg(Arg::new("output").long("output").action(ArgAction::Set).value_name("FILENAME").default_value("tvscreen.png"))
            .arg(Arg::new("invert").long("invert").short('i').num_args(0).action(ArgAction::SetTrue))
            .arg(Arg::new("pull").long("pull").short('u').num_args(0).action(ArgAction::SetTrue)));
    let help = cmd.render_help();
    let m = cmd.get_matches();
    
    if let Some(cli_pass) = m.get_one::<String>("password") {
        env::set_var("ITRETAIL_PASS", cli_pass)
    }
    if let Some(cli_user) = m.get_one::<String>("username") {
        env::set_var("ITRETAIL_USER", cli_user)
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
            println!(r"A username is needed and is not present in the environment. Add one.

            On Windows:
                $env:ITRETAIL_USER = 'user@example.com'

            On Unix:
                export ITRETAIL_USER='user@example.com'
")
        }
        if err.to_string().contains("no password provided") {
            println!(r"A password is needed and is not present in the environment. Add one.

            On Windows:
                $env:ITRETAIL_PASS = 'password'

            On Unix:
                export ITRETAIL_PASS='password'
")
        }
        panic!("{}", err)
    }

    match m.subcommand() {
        Some(("scale-export", scmd)) => {
            let filename = scmd.get_one::<String>("output").unwrap();
            let mut scale_file = internal::cas::create_scale_file(filename);
            let r = scale_file.build_from_itretail_products(&mut api, &scmd);
            if r.is_err() {
                println!("Error: {}", r.err().unwrap())
            }
        },
        Some(("label-export", scmd)) => {
            let filename = scmd.get_one::<String>("output").unwrap();
            let mut label_file = internal::label::create_label_file(filename);
            let results = api.get(&"/api/ProductsData/GetAllProducts".to_string()).expect("no results from API call");
            let r = label_file.build_from_itretail_products(&results, &scmd);
            if r.is_err() {
                println!("{}", r.err().unwrap())
            }
        },
        Some(("mailchimp-sync", scmd)) => {
            let r = internal::customer::mailchimp_sync(&mut api, &scmd);
            if r.is_err() {
                println!("{:?}", r.err().unwrap())
            }
        },
        Some(("tvmenu", scmd)) => {
            let menu_file = scmd.get_one::<String>("menu").unwrap();
            if scmd.get_flag("pull") {
                let results = api.get(&"/api/ProductsData/GetAllProducts".to_string()).expect("no results from API call");
                let r = internal::tvmenu::make_listing(menu_file, &results);
                if r.is_err() {
                    println!("Error constructing menu from IT Retail: {}", r.err().unwrap());
                }
            }
            let menu_txt = fs::read_to_string(menu_file).expect("Could not open file.");
            let r = internal::tvmenu::make_menu(scmd.get_one::<String>("output").unwrap(),
                                                                   &menu_txt,
                                                                   scmd.get_one::<String>("backdrop"),
                                                                   scmd.get_flag("invert"));
            if r.is_err() {
                println!("Error creating TV menu image: {}", r.err().unwrap());
            }
        }
        Some(("set-plu", scmd)) => {
            let upc = scmd.get_one::<String>("upc");
            let plus = scmd.get_one::<String>("plu");
            if upc.is_none() || upc.unwrap().len() != 13 || plus.is_none() || plus.unwrap().len() != 4 {
                println!("Error, upc {:?} (should be 13 digits) or plu {:?} (should be 4 digits) invalid", upc, plus);
            } else {
                let plu = u16::from_str_radix(plus.unwrap(), 10).ok().unwrap();
                let plu_assignment = internal::api::PLUAssignment { upc: upc.unwrap().to_string(), plu: plu };
                let r = api.set_plu(vec!(plu_assignment));
                if r.is_err() {
                    println!("Error setting PLU: {}", r.err().unwrap())
                }
            }
        }
        _ => {
            println!("{}", help)
        }
    }
}
