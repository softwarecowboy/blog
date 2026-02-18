use p01::analysis::*;
use p01::data_ingestion::open_file;
use std::time::Instant;

mod fixture;
use fixture::init_fixture;

fn main() {
    println!("=== Benchmark: Data Analysis Approaches ===\n");

    let file_path = init_fixture();
    let records = open_file(file_path).expect("Failed to load data");

    println!("--- Greedy Approach (for loop with mut) ---");
    let iterations = 100;
    let start = Instant::now();
    let mut sum_check = 0.0;

    for _ in 0..iterations {
        let (total, _) = analyze_greedy(&records);
        sum_check += total;
    }

    let elapsed = start.elapsed();
    let avg_time = elapsed / iterations;

    println!("Total time: {:?}", elapsed);
    println!("Average: {:?} per iteration", avg_time);
    println!(
        "Throughput: {:.2} million records/sec",
        (records.len() as f64 / avg_time.as_secs_f64()) / 1_000_000.0
    );
    println!("(checksum: {:.2})\n", sum_check);

    println!("--- Functional Approach (map + sum) ---");
    let start = Instant::now();
    let mut sum_check = 0.0;

    for _ in 0..iterations {
        let (total, _) = analyze_functional(&records);
        sum_check += total;
    }

    let elapsed = start.elapsed();
    let avg_time = elapsed / iterations;
    println!("Total time: {:?}", elapsed);
    println!("Average: {:?} per iteration", avg_time);
    println!(
        "Throughput: {:.2} million records/sec",
        (records.len() as f64 / avg_time.as_secs_f64()) / 1_000_000.0
    );
    println!("(checksum: {:.2})\n", sum_check);

    /// bench 3: rayon parallel approach (enable with --features rayon or --features all)
    #[cfg(feature = "rayon")]
    {
        println!("--- Rayon Parallel Approach ---");
        let start = Instant::now();
        let mut sum_check = 0.0;

        for _ in 0..iterations {
            let (total, _) = analyze_rayon(&records);
            sum_check += total;
        }

        let elapsed = start.elapsed();
        let avg_time = elapsed / iterations;
        println!("Total time: {:?}", elapsed);
        println!("Average: {:?} per iteration", avg_time);
        println!(
            "Throughput: {:.2} million records/sec",
            (records.len() as f64 / avg_time.as_secs_f64()) / 1_000_000.0
        );
        println!("(checksum: {:.2})\n", sum_check);
    }

    // bench 4: polars df approach
    #[cfg(feature = "polars")]
    {
        println!("--- Polars DataFrame Approach ---");

        // Polars has conversion overhead, so run fewer iterations
        let polars_iterations = 10;
        let start = Instant::now();
        let mut sum_check = 0.0;

        for _ in 0..polars_iterations {
            let (total, _) = analyze_polars(&records).expect("Polars analysis failed");
            sum_check += total;
        }

        let elapsed = start.elapsed();
        let avg_time = elapsed / polars_iterations;
        println!("Total time: {:?}", elapsed);
        println!("Average: {:?} per iteration", avg_time);
        println!(
            "Throughput: {:.2} million records/sec",
            (records.len() as f64 / avg_time.as_secs_f64()) / 1_000_000.0
        );
        println!("(checksum: {:.2})\n", sum_check);
    }

    println!("--- Verification ---");
    let (greedy_sum, greedy_count) = analyze_greedy(&records);
    let (func_sum, func_count) = analyze_functional(&records);

    println!("Greedy:     sum={:.2}, count={}", greedy_sum, greedy_count);
    println!("Functional: sum={:.2}, count={}", func_sum, func_count);

    #[cfg(feature = "rayon")]
    {
        let (rayon_sum, rayon_count) = analyze_rayon(&records);
        println!("Rayon:      sum={:.2}, count={}", rayon_sum, rayon_count);
    }

    #[cfg(feature = "polars")]
    {
        let (polars_sum, polars_count) = analyze_polars(&records).unwrap();
        println!("Polars:     sum={:.2}, count={}", polars_sum, polars_count);
    }
}
