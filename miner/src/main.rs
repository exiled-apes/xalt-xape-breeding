use borsh::de::BorshDeserialize;
use gumdrop::Options;
use mpl_token_metadata::state::Metadata;
use rusqlite::{params, Connection};
use serde::Deserialize;
use serde_json::Value;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{account::ReadableAccount, pubkey::Pubkey};
use std::{
    error::Error,
    fs::File,
    io::{BufRead, BufReader},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse_args_default_or_exit();
    match args.clone().command {
        None => todo!(),
        Some(command) => match command {
            Command::MineXalts(opts) => mine_xalts(args, opts).await,
            Command::MineXapes(opts) => mine_xapes(args, opts).await,
            Command::Summarize(opts) => summarize(args, opts).await,
        },
    }
}

async fn mine_xalts(args: Args, opts: MineXalts) -> Result<(), Box<dyn Error>> {
    let rpc = RpcClient::new(args.rpc);
    let db = Connection::open(args.db)?;

    db.execute(
        "CREATE TABLE IF NOT EXISTS xalts (
            mint_address text primary key,
            metadata_address text unique,
            metadata_data_name text,
            metadata_data_uri text,
            metadata_json_name text,
            metadata_json_image text
        )",
        params![],
    )?;
    db.execute(
        "CREATE TABLE IF NOT EXISTS xalt_atts (
            mint_address text,
            trait_type text,
            value text
        )",
        params![],
    )?;

    let mints_file = File::open(opts.mints_file)?;
    let mints_reader = BufReader::new(mints_file);
    for line in mints_reader.lines() {
        let mint_address: Pubkey = line?.parse()?;
        let count: Result<u8, rusqlite::Error> = db.query_row(
            "SELECT COUNT(*) FROM xalts where mint_address = ?1",
            params![mint_address.to_string()],
            |row| row.get(0),
        );
        if count? == 1u8 {
            eprint!("{}", "-");
            continue;
        }

        let meta_address = find_metadata_address(mint_address);
        let meta = rpc.get_account(&meta_address)?;
        let meta = Metadata::deserialize(&mut meta.data())?;
        let json = reqwest::get(meta.data.clone().uri)
            .await?
            .json::<JsonMeta>()
            .await?;

        db.execute(
            "INSERT INTO xalts (
                mint_address,
                metadata_address,
                metadata_data_name,
                metadata_data_uri,
                metadata_json_name,
                metadata_json_image
            ) values (
                ?1,
                ?2,
                ?3,
                ?4,
                ?5,
                ?6
            )",
            params![
                mint_address.clone().to_string(),
                meta_address.to_string(),
                meta.data.name.trim_matches(char::from(0)),
                meta.data.uri.trim_matches(char::from(0)),
                json.name,
                json.image,
            ],
        )?;

        for attribute in json.attributes {
            match attribute.value {
                Value::Null => todo!(),
                Value::Bool(_) => todo!(),
                Value::Number(value) => {
                    let value = format!("{}", value);
                    db.execute(
                        "INSERT INTO xalt_atts (mint_address, trait_type, value) values (?1, ?2, ?3)",
                        params![mint_address.to_string(), attribute.trait_type, value],
                    )?;
                }
                Value::String(value) => {
                    db.execute(
                        "INSERT INTO xalt_atts (mint_address, trait_type, value) values (?1, ?2, ?3)",
                        params![mint_address.to_string(), attribute.trait_type, value],
                    )?;
                }
                Value::Array(_) => todo!(),
                Value::Object(_) => todo!(),
            }
        }
        eprint!("{}", "+");
    }
    Ok(())
}

async fn mine_xapes(args: Args, opts: MineXapes) -> Result<(), Box<dyn Error>> {
    let rpc = RpcClient::new(args.rpc);
    let db = Connection::open(args.db)?;

    db.execute(
        "CREATE TABLE IF NOT EXISTS xapes (
            mint_address text primary key,
            metadata_address text unique,
            metadata_data_name text,
            metadata_data_uri text,
            metadata_json_name text,
            metadata_json_image text
        )",
        params![],
    )?;
    db.execute(
        "CREATE TABLE IF NOT EXISTS xape_atts (
            mint_address text,
            trait_type text,
            value text
        )",
        params![],
    )?;

    let mints_file = File::open(opts.mints_file)?;
    let mints_reader = BufReader::new(mints_file);
    for line in mints_reader.lines() {
        let mint_address: Pubkey = line?.parse()?;
        let count: Result<u8, rusqlite::Error> = db.query_row(
            "SELECT COUNT(*) FROM xapes where mint_address = ?1",
            params![mint_address.to_string()],
            |row| row.get(0),
        );
        if count? == 1u8 {
            eprint!("{}", "-");
            continue;
        }

        let meta_address = find_metadata_address(mint_address);
        let meta = rpc.get_account(&meta_address)?;
        let meta = Metadata::deserialize(&mut meta.data())?;
        let json = reqwest::get(meta.data.clone().uri)
            .await?
            .json::<JsonMeta>()
            .await?;

        db.execute(
            "INSERT INTO xapes (
                mint_address,
                metadata_address,
                metadata_data_name,
                metadata_data_uri,
                metadata_json_name,
                metadata_json_image
            ) values (
                ?1,
                ?2,
                ?3,
                ?4,
                ?5,
                ?6
            )",
            params![
                mint_address.clone().to_string(),
                meta_address.to_string(),
                meta.data.name.trim_matches(char::from(0)),
                meta.data.uri.trim_matches(char::from(0)),
                json.name,
                json.image,
            ],
        )?;

        for attribute in json.attributes {
            match attribute.value {
                Value::Null => todo!(),
                Value::Bool(_) => todo!(),
                Value::Number(value) => {
                    let value = format!("{}", value);
                    db.execute(
                        "INSERT INTO xape_atts (mint_address, trait_type, value) values (?1, ?2, ?3)",
                        params![mint_address.to_string(), attribute.trait_type, value],
                    )?;
                }
                Value::String(value) => {
                    db.execute(
                        "INSERT INTO xape_atts (mint_address, trait_type, value) values (?1, ?2, ?3)",
                        params![mint_address.to_string(), attribute.trait_type, value],
                    )?;
                }
                Value::Array(_) => todo!(),
                Value::Object(_) => todo!(),
            }
        }
        eprint!("{}", "+");
    }
    Ok(())
}

async fn summarize(args: Args, _opts: Summarize) -> Result<(), Box<dyn Error>> {
    let db = Connection::open(args.db)?;

    let mut trait_types = db.prepare("select distinct(trait_type) from xalt_atts order by 1")?;
    let trait_types = trait_types.query_map([], |row| Ok(TraitType { name: row.get(0)? }))?;
    println!("XALT Traits");
    for trait_type in trait_types {
        let trait_type = trait_type?;
        println!("  {}", trait_type.name.clone());

        // let counters =
        //     "SELECT value, count(*) FROM xalt_atts WHERE trait_type = ? GROUP BY 1 ORDER BY 2";
        // let mut counters = db.prepare(counters)?;
        // let counters = counters.query_map([trait_type.name], |row| {
        //     Ok(Counter {
        //         name: row.get(0)?,
        //         count: row.get(1)?,
        //     })
        // })?;
        // for counter in counters {
        //     let counter = counter?;
        //     println!("    {: <34} {: >3}", counter.name, counter.count);
        // }
    }
    println!("");

    let mut trait_types = db.prepare("select distinct(trait_type) from xape_atts order by 1")?;
    let trait_types = trait_types.query_map([], |row| Ok(TraitType { name: row.get(0)? }))?;
    println!("XAPE Traits");
    for trait_type in trait_types {
        let trait_type = trait_type?;
        if trait_type.name == "Inmate number" {
            continue;
        }
        println!("  {}", trait_type.name);

        // let counters =
        //     "SELECT value, count(*) FROM xape_atts WHERE trait_type = ? GROUP BY 1 ORDER BY 2";
        // let mut counters = db.prepare(counters)?;
        // let counters = counters.query_map([trait_type.name], |row| {
        //     Ok(Counter {
        //         name: row.get(0)?,
        //         count: row.get(1)?,
        //     })
        // })?;
        // for counter in counters {
        //     let counter = counter?;
        //     println!("    {: <34} {: >3}", counter.name, counter.count);
        // }
    }
    println!("");

    Ok(())
}

#[derive(Clone, Debug, Options)]
struct Args {
    #[options(help = "db path", default_expr = "default_db_path()", meta = "d")]
    db: String,
    #[options(help = "rpc server", default_expr = "default_rpc_url()", meta = "r")]
    rpc: String,
    #[options(command)]
    command: Option<Command>,
}

fn default_db_path() -> String {
    "../data/mine.db".to_owned()
}

fn default_rpc_url() -> String {
    "https://api.mainnet-beta.solana.com".to_owned()
}

#[derive(Clone, Debug, Options)]
enum Command {
    MineXapes(MineXapes),
    MineXalts(MineXalts),
    Summarize(Summarize),
}

#[derive(Clone, Debug, Options)]
struct MineXalts {
    #[options(help = "xalt mints file", default_expr = "default_xalt_mints_file()")]
    mints_file: String,
}

fn default_xalt_mints_file() -> String {
    "../data/xalt-mints".to_owned()
}

#[derive(Clone, Debug, Options)]
struct MineXapes {
    #[options(help = "xape mints file", default_expr = "default_xape_mints_file()")]
    mints_file: String,
}

fn default_xape_mints_file() -> String {
    "../data/xape-mints".to_owned()
}
#[derive(Clone, Debug, Options)]
struct Summarize {}

#[derive(Clone, Debug, Deserialize)]
struct JsonMeta {
    name: String,
    image: String,
    attributes: Vec<JsonAttribute>,
}

#[derive(Clone, Debug, Deserialize)]
struct JsonAttribute {
    trait_type: String,
    value: Value,
}

struct TraitType {
    name: String,
}

struct Counter {
    name: String,
    count: u32,
}

fn find_metadata_address(mint: Pubkey) -> Pubkey {
    let (address, _bump) = Pubkey::find_program_address(
        &[
            mpl_token_metadata::state::PREFIX.as_bytes(),
            mpl_token_metadata::id().as_ref(),
            mint.as_ref(),
        ],
        &mpl_token_metadata::id(),
    );
    address
}
