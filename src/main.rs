#![warn(clippy::all)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release builds

use parqbench::{Arguments, DataFilters, ParqBenchApp, ParquetData};

/*
clear && cargo test -- --nocapture
clear && cargo run -- --help
cargo doc --open
cargo b -r && cargo install --path=.
*/

#[cfg(not(target_arch = "wasm32"))]
fn main() -> eframe::Result<()> {
    // Initialize the logger.
    tracing_subscriber::fmt::init();

    // Parse command-line arguments.
    let args = Arguments::build();

    // Configure the native window options.
    let options = eframe::NativeOptions {
        centered: true,
        persist_window: true,
        ..Default::default()
    };

    // Run the eframe application.
    eframe::run_native(
        "ParqBench",
        options,
        Box::new(move |cc| {
            // Create the ParqBench application based on whether a filename is provided.
            Ok(Box::new(match &args.filename {
                Some(filename) => {
                    // Debug the data filters.
                    DataFilters::debug(&args);

                    // Load Parquet data from the specified file.
                    let future = ParquetData::load(filename.to_string());

                    // Create the ParqBench application with the data loading future.
                    ParqBenchApp::new_with_future(cc, Box::new(Box::pin(future)))
                }
                None => {
                    // Create a default ParqBench application if no filename is provided.
                    ParqBenchApp::new(cc)
                }
            }))
        }),
    )
}
