use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

use rand::RngExt;
use serde::Deserialize;

pub fn generate_mock_data(path: &str, target_size_gb: usize) -> std::io::Result<()> {
    // file for file op, writer for writer ops
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    // this is must be mutable because it changes the state every time it generates a new number!
    let mut rng = rand::rng();
    let target_bytes = target_size_gb * 1024 * 1024 * 1024;
    let mut written_bytes = 0;
    let mut record_count = 0;
    let mut malformed_records = 0;

    // good old header for starters
    writeln!(writer, "id|from_id|to_id|amount")?;

    while written_bytes < target_bytes {
        let id = format!("TXN{:010}", record_count);
        let from_id = format!("ACC{:08}", rng.random_range(1000000..9999999));
        let to_id = format!("ACC{:08}", rng.random_range(1000000..9999999));
        let amount = rng.random_range(1.0..100000.0);

        // Inject corruption in ~0.001% of records (lowercase L instead of pipe)
        let delimiter = if rng.random_range(0..1000000) == 0 {
            malformed_records += 1;
            "l"
        } else {
            "|"
        };

        let line = format!("{id}|{from_id}{delimiter}{to_id}|{amount:.2}\n");
        writer.write_all(line.as_bytes())?;

        written_bytes += line.len();
        record_count += 1;

        if record_count % 1_000_000 == 0 {
            println!(
                "Generated {} million records, ~{:.2} GB\nMalformed records: {}",
                record_count / 1_000_000,
                written_bytes as f64 / 1e9,
                malformed_records
            );
        }
    }

    writer.flush()?;
    println!(
        "Generated {record_count} records, total size: {:.2} GB",
        written_bytes as f64 / 1e9
    );
    Ok(())
}

pub mod data_ingestion {
    use super::*;
    use std::io::BufRead;

    #[derive(Debug, Deserialize, Clone)]
    pub struct ClientData {
        pub id: String,
        pub from_id: String,
        pub to_id: String,
        pub amount: f64,
    }

    pub fn open_file(path: &str) -> Result<Vec<ClientData>, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut records = Vec::new();

        for (line_num, line) in reader.lines().enumerate() {
            let line = line?;

            // skipping header
            if line_num == 0 {
                continue;
            }

            // try to parse the line
            match parse_line(&line) {
                Ok(record) => records.push(record),

                // we decided to skip broken records
                Err(e) => eprintln!(
                    "Warning: Failed to parse line {}: {} - {}",
                    line_num + 1,
                    line,
                    e
                ),
            }
        }

        Ok(records)
    }

    pub fn parse_line(line: &str) -> Result<ClientData, Box<dyn std::error::Error>> {
        let parts: Vec<&str> = line.split('|').collect();

        if parts.len() < 4 {
            return Err("Not enough fields (expected 4)!".into());
        }

        let (id, from_id, to_id, amount) = if parts.len() == 4 {
            // Well-formed record
            (parts[0], parts[1], parts[2], parts[3])
        } else {
            return Err(format!("Malformed record: expected 4 fields, got {}", parts.len()).into());
        };

        let record = ClientData {
            id: id.trim().to_string(),
            from_id: from_id.trim().to_string(),
            to_id: to_id.trim().to_string(),
            amount: amount.trim().parse()?,
        };

        Ok(record)
    }
}

pub mod analysis {
    use super::data_ingestion::ClientData;

    /// Greedy approach: for loop with mutable accumulator
    pub fn analyze_greedy(records: &[ClientData]) -> (f64, usize) {
        let mut total_amount = 0.0;
        let mut count = 0;

        for record in records {
            total_amount += record.amount;
            count += 1;
        }

        (total_amount, count)
    }

    /// Functional approach: map with reduce
    pub fn analyze_functional(records: &[ClientData]) -> (f64, usize) {
        let total_amount: f64 = records.iter().map(|r| r.amount).sum();
        let count = records.len();

        (total_amount, count)
    }

    /// Rayon parallel approach: parallel iterators
    #[cfg(feature = "rayon")]
    pub fn analyze_rayon(records: &[ClientData]) -> (f64, usize) {
        use rayon::prelude::*;

        let total_amount: f64 = records.par_iter().map(|r| r.amount).sum();
        let count = records.len();

        (total_amount, count)
    }

    /// Polars DataFrame approach
    #[cfg(feature = "polars")]
    pub fn analyze_polars(
        records: &[ClientData],
    ) -> Result<(f64, usize), polars::error::PolarsError> {
        use polars::prelude::*;

        // Create series from our data
        let ids: Vec<&str> = records.iter().map(|r| r.id.as_str()).collect();
        let from_ids: Vec<&str> = records.iter().map(|r| r.from_id.as_str()).collect();
        let to_ids: Vec<&str> = records.iter().map(|r| r.to_id.as_str()).collect();
        let amounts: Vec<f64> = records.iter().map(|r| r.amount).collect();

        let df = DataFrame::new(vec![
            Column::Series(Series::new("id".into(), ids)),
            Column::Series(Series::new("from_id".into(), from_ids)),
            Column::Series(Series::new("to_id".into(), to_ids)),
            Column::Series(Series::new("amount".into(), amounts)),
        ])?;

        // Perform aggregations - convert Column to Series for sum
        let amount_col = df.column("amount")?;
        let amount_series = amount_col.as_materialized_series();
        let total_amount = amount_series.sum::<f64>().unwrap_or(0.0);
        let count = df.height();

        Ok((total_amount, count))
    }
}
