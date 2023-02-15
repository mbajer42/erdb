use std::io::{BufWriter, Write};
use std::net::TcpStream;

use anyhow::Result;

use crate::catalog::schema::TypeId;
use crate::executors::Executor;

pub struct Printer<'a> {
    executor: Box<dyn Executor + 'a>,
    column_widths: Vec<usize>,
}

impl<'a> Printer<'a> {
    pub fn new(executor: Box<dyn Executor + 'a>) -> Self {
        let schema = executor.schema();
        let mut column_widths = vec![];
        for col in schema.columns() {
            let col_name_size = col.column_name().chars().count();
            let col_width = match col.type_id() {
                TypeId::Boolean => col_name_size.max("false".chars().count()),
                TypeId::Integer => col_name_size.max(10),
                TypeId::Text => col_name_size.max(25),
            };
            column_widths.push(col_width);
        }

        Self {
            executor,
            column_widths,
        }
    }

    fn print_header(&self, writer: &mut BufWriter<&TcpStream>) -> Result<()> {
        let col_names = self
            .executor
            .schema()
            .columns()
            .iter()
            .map(|col| col.column_name());
        let header = self
            .column_widths
            .iter()
            .zip(col_names)
            .map(|(width, name)| format!("{:>1$}", name, *width))
            .collect::<Vec<String>>()
            .join("|");
        writer.write_all(header.as_bytes())?;
        writer.write_all("\n".as_bytes())?;
        let separator_line = self
            .column_widths
            .iter()
            .map(|width| format!("{:-^1$}", '-', width))
            .collect::<Vec<String>>()
            .join("+");
        writer.write_all(separator_line.as_bytes())?;
        writer.write_all("\n".as_bytes())?;

        Ok(())
    }

    pub fn print_all_tuples(&mut self, writer: &mut BufWriter<&TcpStream>) -> Result<()> {
        self.print_header(writer)?;

        while let Some(tuple) = self.executor.next() {
            let tuple = tuple?;
            let values = tuple.values();
            let line = self
                .column_widths
                .iter()
                .zip(values)
                .map(|(width, value)| format!("{:>1$}", value, *width))
                .collect::<Vec<String>>()
                .join("|");
            writer.write_all(line.as_bytes())?;
            writer.write_all("\n".as_bytes())?;
        }

        Ok(())
    }
}
