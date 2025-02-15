#![warn(clippy::all)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use parqbench::{Arguments, DataFilters, ParqBenchApp, ParquetData};

/*
    clear && cargo test -- --nocapture
    clear && cargo run -- --help
    cargo b -r && cargo install --path=.
*/

#[cfg(not(target_arch = "wasm32"))]
fn main() -> eframe::Result<()> {
    // Log to stdout (if you run with `RUST_LOG=debug`).
    tracing_subscriber::fmt::init();

    let args = Arguments::build();

    let options = eframe::NativeOptions {
        centered: true,
        persist_window: true,
        ..Default::default()
    };

    eframe::run_native(
        "ParqBench",
        options,
        Box::new(move |cc| {
            Ok(Box::new(match &args.filename {
                Some(filename) => {
                    DataFilters::debug(&args);
                    let future = ParquetData::load(filename.to_string());
                    ParqBenchApp::new_with_future(cc, Box::new(Box::pin(future)))
                }
                None => ParqBenchApp::new(cc),
            }))
        }),
    )
}
