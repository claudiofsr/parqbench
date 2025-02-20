#![warn(clippy::all)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use parqbench::{Arguments, DataFilters, ParqBenchApp, ParquetData};

/*
clear && cargo test -- --nocapture
clear && cargo run -- --help
cargo doc --open
cargo b -r && cargo install --path=.
*/

#[cfg(not(target_arch = "wasm32"))]
fn main() -> eframe::Result<()> {
    // Initialize the tracing subscriber for logging.
    tracing_subscriber::fmt::init();

    // Parse command-line arguments.
    let args = Arguments::build();

    // Configure the native options for the eframe application.
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
            // Create a new ParqBenchApp. If a filename is provided, load the data.
            Ok(Box::new(match &args.filename {
                Some(filename) => {
                    // Log debug information about the data filters.
                    DataFilters::debug(&args);

                    // Load the Parquet data from the specified filename.
                    let future = ParquetData::load(filename.to_string());

                    // Create a new ParqBenchApp with the data loading future.
                    ParqBenchApp::new_with_future(cc, Box::new(Box::pin(future)))
                }
                None => ParqBenchApp::new(cc), // Create a new ParqBenchApp without loading data.
            }))
        }),
    )
}
