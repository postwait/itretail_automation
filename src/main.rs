mod internal;

use clap::{Command,Arg,ArgAction};
use std::env;

fn main() {
    let mut cmd = Command::new("itretail_automation")
        .author("Theo Schlossnagle, jesus@lethargy.org")
        .version("0.0.1")
        .about("Automates certain tasks against IT Retail")
        .arg(Arg::new("username").long("username").short('u'))
        .arg(Arg::new("password").long("password").short('p'))
        .subcommand(Command::new("scale-export")
            .arg(Arg::new("output").long("output").short('o').action(ArgAction::Set).value_name("FILE").default_value("scale.xlsx"))
        )
        .subcommand(Command::new("label-export")
            .arg(Arg::new("output").long("output").short('o').action(ArgAction::Set).value_name("FILE").default_value("labels.xlsx"))
            .arg(Arg::new("upc").long("upc").action(ArgAction::Set).value_name("Regex").default_value("^00[^2]"))
            .arg(Arg::new("name").long("name").action(ArgAction::Set).value_name("Regex").default_value("."))
        );
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
            let results = api.call(&"/api/ProductsData/GetAllProducts".to_string()).expect("no results from API call");
            let r = scale_file.build_from_itretail_products(&results);
            if r.is_err() {
                println!("{}", r.err().unwrap())
            }
        },
        Some(("label-export", scmd)) => {
            let filename = scmd.get_one::<String>("output").unwrap();
            let mut label_file = internal::label::create_label_file(filename);
            let results = api.call(&"/api/ProductsData/GetAllProducts".to_string()).expect("no results from API call");
            let r = label_file.build_from_itretail_products(&results, &scmd);
            if r.is_err() {
                println!("{}", r.err().unwrap())
            }
        },
        _ => {
            println!("{}", help)
        }
    }
}
