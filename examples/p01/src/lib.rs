use std::fs::File;
use std::io::{BufWriter, Write};

use rand::RngExt;

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

        // Inject corruption in ~0.1% of records (lowercase L instead of pipe)
        let delimiter = if rng.random_range(0..1000) == 0 {
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
