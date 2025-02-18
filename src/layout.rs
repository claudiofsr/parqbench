use egui::{
    menu,
    style::Visuals,
    warn_if_debug_build, widgets, CentralPanel, Context,
    FontFamily::Proportional,
    FontId, RichText, ScrollArea, SidePanel,
    TextStyle::{Body, Button, Heading, Monospace, Small},
    TopBottomPanel, ViewportCommand,
};

use std::sync::Arc;
use tokio::sync::oneshot::{self, error::TryRecvError};

use crate::components::{file_dialog, Error, FileMetadata, Popover, QueryPane, Settings};
use crate::data::{DataFilters, DataFuture, ParquetData};

/// The main application struct for ParqBench.
pub struct ParqBenchApp {
    /// An `Arc` to the loaded Parquet data. Using `Arc` for shared ownership and thread-safe access.
    pub table: Arc<Option<ParquetData>>,
    /// The query pane component for filtering and querying data.
    pub query_pane: QueryPane,
    /// Metadata associated with the loaded Parquet file.
    pub metadata: Option<FileMetadata>,
    /// An optional popover for displaying errors, settings, or other information.
    pub popover: Option<Box<dyn Popover>>,

    /// The Tokio runtime for asynchronous operations.
    runtime: tokio::runtime::Runtime,
    /// A channel for receiving the result of asynchronous data loading.
    pipe: Option<tokio::sync::oneshot::Receiver<Result<ParquetData, String>>>,

    /// A vector of tasks to keep track of multiple concurrent operations.
    /// This solves the FIXME about using a vector of tasks instead of a single one.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Default for ParqBenchApp {
    fn default() -> Self {
        Self {
            table: Arc::new(None),
            query_pane: QueryPane::new(None, DataFilters::default()),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to build Tokio runtime"),
            pipe: None,
            popover: None,
            metadata: None,
            tasks: Vec::new(),
        }
    }
}

/// A trait for applying custom styling to the egui context.
trait MyStyle {
    /// Sets the initial style for the egui context.
    fn set_style_init(&self);
}

impl MyStyle for Context {
    /// Specifies the look and feel of egui.
    ///
    /// <https://docs.rs/egui/latest/egui/style/struct.Style.html>
    fn set_style_init(&self) {
        // Get current context style
        let mut style = (*self.style()).clone();

        // Redefine text_styles
        style.text_styles = [
            (Small, FontId::new(12.0, Proportional)),
            (Body, FontId::new(16.0, Proportional)),
            (Monospace, FontId::new(14.0, Proportional)),
            (Button, FontId::new(14.0, Proportional)),
            (Heading, FontId::new(14.0, Proportional)),
        ]
        .into();

        style.spacing.item_spacing.x = 8.0;
        style.spacing.item_spacing.y = 6.0;

        // Mutate global style with above changes
        self.set_style(style);
    }
}

impl ParqBenchApp {
    /// Creates a new `ParqBenchApp`.
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        cc.egui_ctx.set_visuals(Visuals::dark());
        cc.egui_ctx.set_style_init();
        Default::default()
    }

    /// Creates a new `ParqBenchApp` with a pre-existing `DataFuture`.
    pub fn new_with_future(cc: &eframe::CreationContext<'_>, future: DataFuture) -> Self {
        let mut app: Self = Default::default();
        cc.egui_ctx.set_visuals(Visuals::dark());
        cc.egui_ctx.set_style_init();
        app.run_data_future(future, &cc.egui_ctx);
        app
    }

    /// Checks if a popover is active and displays it.  If the popover is closed, it is removed.
    pub fn check_popover(&mut self, ctx: &Context) {
        if let Some(popover) = &mut self.popover {
            if !popover.show(ctx) {
                self.popover = None;
            }
        }
    }

    /// Checks if there is data loading pending.
    ///
    /// Returns `true` if data is still loading, `false` otherwise.
    pub fn check_data_pending(&mut self) -> bool {
        // Takes the value out of the self.pipe: Option<value>, leaving a None in its place.
        let Some(mut output) = self.pipe.take() else {
            return false;
        };

        match output.try_recv() {
            Ok(data) => match data {
                Ok(data) => {
                    self.query_pane =
                        QueryPane::new(Some(data.filename.clone()), data.filters.clone());
                    self.metadata = FileMetadata::from_filename(data.filename.as_str()).ok();
                    self.table = Arc::new(Some(data));
                    false
                }
                Err(msg) => {
                    self.popover = Some(Box::new(Error { message: msg }));
                    false
                }
            },
            Err(error) => match error {
                TryRecvError::Empty => {
                    // If the channel is empty, put the receiver back.
                    self.pipe = Some(output);
                    true
                }
                TryRecvError::Closed => {
                    self.popover = Some(Box::new(Error {
                        message: "Data operation terminated without response.".to_string(),
                    }));
                    false
                }
            },
        }
    }

    /// Runs a `DataFuture` to load Parquet data asynchronously.
    pub fn run_data_future(&mut self, future: DataFuture, ctx: &Context) {
        // Before scheduling a new future, ensure no tasks are stuck
        self.tasks.retain(|task| !task.is_finished());

        let (tx, rx) = oneshot::channel::<Result<ParquetData, String>>();
        self.pipe = Some(rx);

        // Clone the context for use within the asynchronous task.
        let ctx_clone = ctx.clone();

        // Spawn an async task to load the data.
        let handle = self.runtime.spawn(async move {
            let data = future.await;
            if tx.send(data).is_err() {
                eprintln!("Receiver dropped before data could be sent.");
            }
            ctx_clone.request_repaint(); // Request repaint to update the UI.
        });

        self.tasks.push(handle);
    }
}

// See
// https://github.com/emilk/egui/blob/master/examples/custom_window_frame/src/main.rs
// https://rodneylab.com/trying-egui/

impl eframe::App for ParqBenchApp {
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        // Frame setup. Check if various interactions are in progress and resolve them
        self.check_popover(ctx);

        // Handle dropped files.
        if let Some(dropped_file) = ctx.input(|i| i.raw.dropped_files.last().cloned()) {
            let filename: String = dropped_file
                .path
                .into_iter()
                .map(|p| p.display().to_string())
                .collect();

            self.run_data_future(Box::new(Box::pin(ParquetData::load(filename))), ctx);
        }

        // Main UI layout.
        //
        // Using static layout until I put together a TabTree that can make this dynamic
        //
        //  | menu_bar      widgets |
        //  -------------------------
        //  |       |               |
        //  | query |     main      |
        //  | info  |     table     |
        //  |       |               |
        //  -------------------------
        //  |  notification footer  |

        TopBottomPanel::top("top_panel").show(ctx, |ui| {
            menu::bar(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.menu_button("File", |ui| {
                        if ui.button("Open").clicked() {
                            if let Ok(filename) = self.runtime.block_on(file_dialog()) {
                                self.run_data_future(
                                    Box::new(Box::pin(ParquetData::load(filename))),
                                    ctx,
                                );
                            }
                            ui.close_menu();
                        }

                        if ui.button("Settings").clicked() {
                            self.popover = Some(Box::new(Settings {}));
                            ui.close_menu();
                        }

                        ui.menu_button("About", |ui| {
                            let version = env!("CARGO_PKG_VERSION");
                            let authors = env!("CARGO_PKG_AUTHORS");
                            ui.label(RichText::new("ParqBench").font(FontId::proportional(20.0)));
                            ui.label(format!("Version: {version}"));
                            ui.label(format!("Author: {authors}"));
                            ui.label("Built with egui");
                        });

                        if ui.button("Quit").clicked() {
                            ui.ctx().send_viewport_cmd(ViewportCommand::Close);
                        }
                    });

                    // Add spacing to align theme switch to the right.
                    let delta = ui.available_width() - 15.0;
                    if delta > 0.0 {
                        ui.add_space(delta);
                        widgets::global_theme_preference_switch(ui);
                    }
                });
            });
        });

        SidePanel::left("side_panel")
            .resizable(true)
            .show(ctx, |ui| {
                ScrollArea::vertical().show(ui, |ui| {
                    // add Metadata
                    if let Some(metadata) = &self.metadata {
                        ui.collapsing("Metadata", |ui| {
                            metadata.render_metadata(ui);
                        });
                    }

                    // add Query
                    ui.collapsing("Query", |ui| {
                        let filters = self.query_pane.render(ui);
                        if let Some((filename, filters)) = filters {
                            self.run_data_future(
                                Box::new(Box::pin(ParquetData::load_with_query(filename, filters))),
                                ctx,
                            );
                        }
                    });

                    // add Schema
                    if let Some(metadata) = &self.metadata {
                        ui.collapsing("Schema", |ui| {
                            metadata.render_schema(ui);
                        });
                    }
                });
            });

        TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            ui.horizontal(|ui| match &*self.table {
                Some(table) => {
                    ui.label(format!("{:#?}", table.filename));
                }
                None => {
                    ui.label("no file set");
                }
            });
        });

        // Main table
        // https://whoisryosuke.com/blog/2023/getting-started-with-egui-in-rust
        // https://github.com/emilk/egui/issues/1376
        // https://github.com/emilk/egui/discussions/3069
        // https://github.com/lucasmerlin/hello_egui/blob/main/crates/egui_dnd/examples/horizontal.rs
        // https://github.com/vvv/egui-table-click/blob/table-row-framing/src/lib.rs
        // https://github.com/emilk/eframe_template/blob/4f613f5d6266f0f0888544df4555e6bc77a5d079/src/app.rs

        CentralPanel::default().show(ctx, |ui| {
            warn_if_debug_build(ui);

            match self.table.as_ref().clone() {
                Some(parquet_data) if parquet_data.data.num_columns() > 0 => {
                    ScrollArea::horizontal().show(ui, |ui| {
                        let opt_filters = parquet_data.render_table(ui);
                        if let Some(filters) = opt_filters {
                            let future = parquet_data.sort(Some(filters));
                            self.run_data_future(Box::new(Box::pin(future)), ctx);
                        }
                    });
                }
                _ => {
                    ui.centered_and_justified(|ui| {
                        ui.label("Drag and drop parquet file here.");
                    });
                }
            };

            if self.check_data_pending() {
                ui.disable();
                if self.table.as_ref().is_none() {
                    ui.centered_and_justified(|ui| {
                        // Show spinner while loading initial data.
                        ui.spinner();
                    });
                }
            }
        });
    }
}
