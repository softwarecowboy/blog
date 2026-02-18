use p01::data_ingestion::*;
use p01::generate_mock_data;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    generate_mock_data("data.csv", 1)?;
    let data = open_file("data.csv")?;

    Ok(())
}
