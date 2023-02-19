mod analyzer;
mod buffer;
mod catalog;
mod common;
mod executors;
mod parser;
mod planner;
mod printer;
mod storage;
mod tuple;

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;

use analyzer::Analyzer;
use anyhow::{Context, Result};
use buffer::buffer_manager::BufferManager;
use catalog::Catalog;
use clap::{Arg, Command, Parser};
use executors::ExecutorFactory;
use parser::ast::Statement;
use parser::parse_sql;
use planner::Planner;
use printer::Printer;
use storage::file_manager::FileManager;

#[derive(Parser)]
struct ServerConfig {
    #[arg(long, help = "Directory where data is stored")]
    data: String,

    #[arg(
        long,
        help = "If enabled, it assumes that data directory is empty and needs to be initialized"
    )]
    new: bool,

    #[arg(long, default_value_t = 42666)]
    port: u16,

    #[arg(long, default_value_t = 8, help = "Size of buffer pool")]
    pool_size: usize,
}

fn metacommand() -> Command {
    Command::new("erdb")
        .subcommand_required(true)
        .disable_help_flag(true)
        .disable_help_subcommand(true)
        .help_template("{all-args}")
        .multicall(true)
        .subcommand(Command::new(".tables").about("Prints all existing tables"))
        .subcommand(
            Command::new(".columns")
                .arg(Arg::new("table_name").required(true))
                .about("Prints all columns of a table"),
        )
        .subcommand(Command::new(".exit").about("Closes the connection"))
}

/// Handles a meta command (like, .tables or .columns). Returns true if the meta command was .exit
fn handle_metacommand(
    writer: &mut BufWriter<&TcpStream>,
    command: &str,
    catalog: &Catalog,
) -> Result<bool> {
    let mut cmd = metacommand();

    match cmd.try_get_matches_from_mut(command.split_whitespace()) {
        Ok(matches) => match matches.subcommand() {
            Some((".tables", _matches)) => {
                let mut tables = catalog.list_tables();
                tables.sort();
                writer.write_all(tables.join("\n").as_bytes())?;
            }
            Some((".columns", matches)) => {
                let table = match matches
                    .get_raw("table_name")
                    .unwrap()
                    .next()
                    .unwrap()
                    .to_str()
                {
                    Some(s) => s,
                    None => {
                        writer.write_all("Invalid table name".as_bytes())?;
                        return Ok(false);
                    }
                };
                match catalog.get_schema(table) {
                    Some(schema) => {
                        for column in schema.columns() {
                            writer.write_all(format!("{:?}\n", column).as_bytes())?;
                        }
                    }
                    None => writer.write_all("Could not find table".as_bytes())?,
                }
            }
            Some((".exit", _matches)) => return Ok(true),
            _ => (),
        },
        Err(e) => {
            writer.write_all(e.to_string().as_bytes())?;
            writer.write_all(format!("{}", cmd.render_help()).as_bytes())?;
        }
    }

    Ok(false)
}

fn handle_sql_statement(
    writer: &mut BufWriter<&TcpStream>,
    sql: &str,
    buffer_manager: &BufferManager,
    catalog: &Catalog,
) -> Result<()> {
    let statement = parse_sql(sql)?;
    match statement {
        Statement::CreateTable { name, columns } => {
            let columns = columns.into_iter().map(|col| col.into()).collect();
            catalog.create_table(&name, columns)?;
            writer.write_all("Table created".as_bytes())?;
        }
        query => {
            let analyzer = Analyzer::new(catalog);
            let query = analyzer.analyze(query)?;
            let planner = Planner::new();
            let plan = planner.plan_query(query);
            let mut executor_factory = ExecutorFactory::new(buffer_manager);
            let executor = executor_factory.create_executor(plan)?;
            let mut printer = Printer::new(executor);
            printer.print_all_tuples(writer)?;
        }
    }
    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    catalog: &Catalog,
    buffer_manager: &BufferManager,
) -> Result<()> {
    stream.write_all("Welcome to erdb".as_bytes())?;
    stream.write_all("\n> ".as_bytes())?;
    stream.flush()?;

    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let mut line = String::new();
    let mut statement = String::new();

    loop {
        line.clear();
        writer.flush()?;
        reader.read_line(&mut line)?;

        if line.as_bytes().is_empty() {
            // Client didn't send anything. Connection lost?
            return Ok(());
        }
        if line.starts_with('.') && statement.trim().is_empty() {
            if handle_metacommand(&mut writer, &line, catalog)? {
                break;
            }
            line.clear();
            writer.write_all("\n> ".as_bytes())?;
        } else {
            statement.push_str(&line);

            // execute a statement when it ends with a semicolon
            if statement.trim_end().ends_with(';') {
                match handle_sql_statement(&mut writer, &statement, buffer_manager, catalog) {
                    Ok(()) => (),
                    Err(e) => {
                        writer.write_all(format!("Error: {}", e).as_bytes())?;
                    }
                }
                statement.clear();
            }
            if statement.trim().is_empty() {
                writer.write_all("\n> ".as_bytes())?;
            }
            line.clear();
        }

        writer.flush()?;
    }

    stream.shutdown(Shutdown::Both)?;
    Ok(())
}

fn main() -> Result<()> {
    println!("Welcome to erdb.");
    let config = ServerConfig::parse();

    let file_manager = FileManager::new(config.data)?;
    let buffer_manager = BufferManager::new(file_manager, config.pool_size);

    let catalog = Catalog::new(&buffer_manager, config.new)
        .with_context(|| "Failed to create catalog".to_string())?;
    let listener = TcpListener::bind(("localhost", config.port))?;

    thread::scope(|scope| {
        let buffer_manager = &buffer_manager;
        let catalog = &catalog;

        scope.spawn(|| {
            println!("Press enter to flush all buffers");
            let mut buffer = String::new();
            loop {
                match std::io::stdin().read_line(&mut buffer) {
                    Ok(_) => {
                        println!("Flushing all buffers...");
                        match buffer_manager.flush_all_buffers() {
                            Ok(()) => println!("Done"),
                            Err(e) => println!("Failed. Reason: {}", e),
                        }
                    }
                    Err(e) => {
                        println!("Could not read user input. Reason: {}", e);
                    }
                }
            }
        });
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    scope.spawn(
                        move || match handle_client(stream, catalog, buffer_manager) {
                            Ok(()) => (),
                            Err(e) => println!("Failed to handle client. Cause: {e}"),
                        },
                    );
                }
                Err(e) => println!("Could not get tcp stream: {e}"),
            }
        }
    });

    Ok(())
}
