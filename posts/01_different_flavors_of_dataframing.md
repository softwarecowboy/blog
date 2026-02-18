---
title: "Data quality and performance: benchmarking aggregations across approaches"
slug: dq-and-perf-benchmarking-aggregations-across-aproaches
tags: ["rust", "data", "serde", "polars", "rayon", "benchmark"]
date: 2026-02-18
---
*I think most of the people working with software profesionally take into account that things may go left.*

# Ingesting data

If you're entering Rust ecosystem as a seasoned backend veteran, you'll be delighted with the `serde` crate. It takes so much off your back due to Serializer and Deserializer traits.

You need to parse yaml files? `cargo add serde serde-yaml` and just wrap your structures as per your yaml scheme

You need to parse json? replace `serde-yaml` with `serde-json`.

CSV? Pickle? Avro? URL? Parquet? Just find according crate for serialization, rinse and repeat.

# Don't expect that everything will be as expected

Data quality matters. It costs as well.

The cost of data quality comes not only in development, but also in resources, and it can be resource heavy. Consider yourself lucky if your data is standardized, tabular and data schema never changes.


## Data example

```rust
struct VeryImportantClientData {
    id: String,
    from_id: String,
    to_id: String,
    amount: f64,
}
```

We're given a .csv that comes every now and then on our server, but since "amount" is separated with comma, the shareholder sends the data with pipe-separated values.

Adding more drama to the context - there's a human in the loop, or poorly developed Computer Vision model.

Said model mistakes l (lowercase L) with | (pipe), so one or more records are corrupt.

## What now?  

Based on the requirement, you have several options.

1. Ending the ingestion - you experienced malformed records, therefore you don't trust the source anymore.
This works, but then you have to adapt, investigate the malformed resources, talk back and forth and possibly, come to a conclusion with data producer. I wouldn't say that's conventional, but where data quality matters and you have shitload of sources, this might come in handy.


2. Skipping the malformed records
Wrapping a record, struct, or however you like to call it in an Result comes in handy. If serde produces an error during serialization, you can trace the amount of malformed records, log what malformed data caused the issue and continue with the next one.

3. Get funky with it - Manual delimiter healing
If you know your schema has exactly 4 fields, you can manually parse and validate each row. Count the delimiters - if you get 3 pipes, great! If you get more, assume some are corrupted lowercase L's that got OCR'd wrong. Use a sliding window to find the most likely 3 split points based on field validation rules (e.g., `amount` should parse as f64, IDs should be alphanumeric). Reconstruct the row with the correct delimiters.

```rust
fn heal_corrupted_row(raw_row: &str) -> Option<VeryImportantClientData> {
    let parts: Vec<&str> = raw_row.split('|').collect();
    
    // if we have exactly 4 fields, try normal parse
    if parts.len() == 4 {
        return try_parse_record(&parts);
    }
    
    // Too many delimiters - try combinations to find valid parse
    if parts.len() > 4 {
        for combination in find_valid_4_field_combinations(&parts) {
            if let Some(record) = try_parse_record(&combination) {
                return Some(record);
            }
        }
    }
    
    None
}
```

This is expensive (O(n²) in worst case), but if you're dealing with high-value data and relatively few corruptions, the trade-off might be worth it.

4. Probabilistic correction with edit distance

Use Levenshtein distance or similar fuzzy matching against known good patterns. If you have a reference dataset of valid IDs, you can check corrupted fields against it. For the amount field specifically, scan for patterns that look like numbers with an errant pipe in the middle (like "12|34.56") and merge them. Log confidence scores for audit trails. 

Or don't. Just blame the data source.

```rust
use edit_distance::edit_distance;

fn correct_with_confidence(field: &str, valid_ids: &[String]) -> (String, f32) {
    let closest = valid_ids
        .iter()
        .map(|valid| (valid, edit_distance(field, valid)))
        .min_by_key(|(_, dist)| *dist)
        .unwrap();
    
    let confidence = 1.0 - (closest.1 as f32 / field.len() as f32);
    (closest.0.clone(), confidence)
}
```

This approach works well when you have reference data and can accept some uncertainty with proper logging.


# Benching common approaches to data analysis in Rust

Data engineering is fun, because there are tools that simplify the work.

I created a function that mocks a dataset in the blog repository under examples.

```rust
use rand::Rng;
use std::fs::File;
use std::io::{BufWriter, Write};

fn generate_mock_data(path: &str, target_size_gb: usize) -> std::io::Result<()> {
    
    // file for file op, writer for writer ops
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    // this is must be mutable because it changes the state every time it generates a new number!
    let mut rng = rand::thread_rng();
    let target_bytes = target_size_gb * 1024 * 1024 * 1024;
    let mut written_bytes = 0;
    let mut record_count = 0;
    
    // good old header for starters
    writeln!(writer, "id|from_id|to_id|amount")?;
    
    while written_bytes < target_bytes {
        let id = format!("TXN{:010}", record_count);
        let from_id = format!("ACC{:08}", rng.gen_range(1000000..9999999));
        let to_id = format!("ACC{:08}", rng.gen_range(1000000..9999999));
        let amount = rng.gen_range(1.0..100000.0);
        
        // inject corruption in fraction of records (lowercase L instead of pipe)
        let delimiter = if rng.gen_range(0..100000) == 0 { "l" } else { "|" };
        
        let line = format!("{id}|{from_id}{delimiter}{to_id}|{amount:.2}\n");
        writer.write_all(line.as_bytes())?;
        
        written_bytes += line.len();
        record_count += 1;
        
        if record_count % 1_000_000 == 0 {
            println!("Generated {} million records, ~{:.2} GB", 
                     record_count / 1_000_000, written_bytes as f64 / 1e9);
        }
    }
    
    writer.flush()?;
    println!("Generated {record_count} records, total size: {:.2} GB", 
             written_bytes as f64 / 1e9);
    Ok(())
}

generate_mock_data("customer_data.psv", 5)?;
```

This generates roughly 50 million records at ~100 bytes per line, with occasional corrupted delimiters to simulate real-world OCR errors.

You can pick your poison, but let's say we already have the records serialized, in a Vec. We're going to leverage timing and tracing, since this is the per se production standard.

## Imperative approach

The most straightforward approach - iterate through records and accumulate results in a mutable variable. This is typically the fastest for simple operations since there's minimal abstraction overhead.

```rust
use std::time::Instant;

fn analyze_imperative(records: &[VeryImportantClientData]) -> (f64, usize) {
    let start = Instant::now();
    
    let mut total_amount = 0.0;
    let mut count = 0;
    
    for record in records {
        total_amount += record.amount;
        count += 1;
    }
    
    let duration = start.elapsed();
    println!("Imperative approach: {:?}", duration);
    
    (total_amount, count)
}
```

**Benchmark (22.9M records):** 2.60s total, **881.91M records/sec**



## Functional approach

Using functional programming patterns - map to extract values, then reduce to aggregate. More idiomatic Rust, and performs on par with the imperative approach thanks to Rust's excellent iterator optimizations.

```rust
use std::time::Instant;

fn analyze_functional(records: &[VeryImportantClientData]) -> (f64, usize) {
    let start = Instant::now();
    
    let total_amount: f64 = records
        .iter()
        .map(|r| r.amount)
        .sum();
    
    let count = records.len();
    
    let duration = start.elapsed();
    println!("Functional approach: {:?}", duration);
    
    (total_amount, count)
}
```

**Benchmark (22.9M records):** 2.59s total, **883.09M records/sec** - virtually identical to the imperative approach!



## Rayon parallel approach

Rayon provides data parallelism with minimal code changes. Simply swap `.iter()` for `.par_iter()` and Rayon handles the thread pool and work distribution automatically. The simplicity and the results with rayon is just incredible. It's a must have tool in every rustacean backpack.

```rust
use rayon::prelude::*;
use std::time::Instant;

fn analyze_rayon(records: &[VeryImportantClientData]) -> (f64, usize) {
    let start = Instant::now();
    
    let total_amount: f64 = records
        .par_iter()
        .map(|r| r.amount)
        .sum();
    
    let count = records.len();
    
    let duration = start.elapsed();
    println!("Rayon parallel approach: {:?}", duration);
    
    (total_amount, count)
}
```

**Benchmark (22.9M records):** 1.66s total, **1379.23M records/sec** - **1.56x faster** than sequential approaches with zero algorithmic complexity!


## Polars dataframe

Polars is a blazingly fast dataframe library for Rust (and Python). 

If you ever worked with Apache Spark, pandas or alike, you'll feel just like at home. Or work?

Built on Apache Arrow, it provides lazy evaluation and parallel execution out of the box. Perfect for complex analytics workloads where you need more than simple aggregations.

```rust
use polars::prelude::*;
use std::time::Instant;

fn analyze_polars(records: &[VeryImportantClientData]) -> Result<(f64, usize), PolarsError> {
    let start = Instant::now();
    
    // Create series from our data
    let ids: Vec<&str> = records.iter().map(|r| r.id.as_str()).collect();
    let from_ids: Vec<&str> = records.iter().map(|r| r.from_id.as_str()).collect();
    let to_ids: Vec<&str> = records.iter().map(|r| r.to_id.as_str()).collect();
    let amounts: Vec<f64> = records.iter().map(|r| r.amount).collect();
    
    let df = DataFrame::new(vec![
        Series::new("id", ids),
        Series::new("from_id", from_ids),
        Series::new("to_id", to_ids),
        Series::new("amount", amounts),
    ])?;
    
    // Perform aggregations
    let total_amount = df.column("amount")?.sum::<f64>().unwrap();
    let count = df.height();
    
    let duration = start.elapsed();
    println!("Polars approach: {:?}", duration);
    
    Ok((total_amount, count))
}
```

**Benchmark (22.9M records):** 3.19s total, **71.80M records/sec**

Surprisingly, Polars is **12x slower** than the functional approach for this simple aggregation. The overhead of constructing the DataFrame from our structs dominates the execution time. Polars shines when you're doing complex operations (joins, groupbys, window functions) where its query optimizer and parallel execution really pay off.

But since we're comparing the plain runtime performance, it's worth noting that polars takes some time to initialize the said dataframe.

## Benchmark Results Summary

Testing on **22.9 million records** across 100 iterations on M4 chip on macbook air: 

| Approach | Total Time | Throughput | vs Fastest |
|----------|------------|------------|------------|
| **Rayon Parallel** | 1.66s | **1379M rec/s** | **1.00x** |
| Functional (map+sum) | 2.59s | 883M rec/s | 0.64x |
| Imperative (for loop) | 2.60s | 882M rec/s | 0.64x |
| Polars DataFrame | 3.19s | 72M rec/s | 0.05x |

**Key Insights:**

1. **Rayon is the clear winner** - 56% faster than sequential approaches with a trivial code change
2. **FP ≈ for looping** - Rust's iterator optimizations are excellent; write idiomatic code without performance guilt
3. **Polars pays setup cost** - DataFrame construction overhead makes it impractical for simple aggregations on in-memory structs. It excels when data is already in columnar format, you need complex operations (joins, groupby, window functions etc.) OR you're chaining multiple transformations where query optimization matters. 

4. **All methods produce consistent results** (within floating-point precision), validating correctness





## Conclusion and ending thoughts

Rust's ecosystem offers remarkable flexibility for data processing. The benchmarks reveal clear patterns:

**For simple aggregations on in-memory data:**
1. **Use Rayon** - trivial parallelization gives you 1.5x speedup for free
2. **Functional ≈ Imperative** - Rust's iterator optimizations make functional code just as fast as for loops, so write what's more maintainable
3. **Skip dataframe overhead** - Polars/DataFusion add 10-20x overhead when you don't need their features

**When dataframes make sense:**
- Reading directly from columnar formats (Parquet, Arrow)
- Complex operations: joins, groupby aggregations, window functions  
- Query optimization across multiple transformations
- When ergonomics matter more than raw speed


883M records/second with clean, functional Rust is plenty fast for most use cases. Optimize for correctness and maintainability first. Rayon gives you easy parallelism when you need it. Complex analytics frameworks are powerful but come with overhead - choose them for their features, not default performance assumptions.

All code used for benchmarking, file creation, analysis, parsing etc can be found - [HERE](https://github.com/softwarecowboy/blog/tree/main/examples/p01)

Remember: premature optimization is the root of all evil, but so is accepting garbage data without question. 

Measure, then optimize what matters.

stay curious