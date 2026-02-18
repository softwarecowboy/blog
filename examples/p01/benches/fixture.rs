use std::path::Path;
use std::sync::OnceLock;

static DATA_FILE: OnceLock<String> = OnceLock::new();

pub fn init_fixture() -> &'static str {
    DATA_FILE.get_or_init(|| {
        let file_path = "bench_data.csv";

        if !Path::new(file_path).exists() {
            println!("Generating benchmark data...");
            p01::generate_mock_data(file_path, 1).expect("Failed to generate benchmark data");
        }

        file_path.to_string()
    })
}
