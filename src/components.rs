use crate::data::{DataFilters, ParquetData, SortState};

use datafusion::arrow::util::display::array_value_to_string;
use egui::{Context, Layout, Response, TextStyle, Ui, WidgetText};
use egui_extras::{Column, TableBuilder, TableRow};
use parquet::{
    basic::ColumnOrder,
    file::{
        metadata::ParquetMetaData,
        reader::{FileReader, SerializedFileReader},
    },
};
use rfd::AsyncFileDialog;
use std::{fs::File, path::Path};

// Trait for popover windows.
pub trait Popover {
    fn show(&mut self, ctx: &Context) -> bool;
}

// Settings popover struct (currently disabled).
pub struct Settings {}

impl Popover for Settings {
    fn show(&mut self, ctx: &Context) -> bool {
        let mut open = true;

        // Create a window named "Settings".
        egui::Window::new("Settings")
            .collapsible(false) // Make the window non-collapsible.
            .open(&mut open) // Control the window's open state.
            .show(ctx, |ui| {
                ctx.style_ui(ui, egui::Theme::Dark); // Apply dark theme.
                ui.disable(); // Disable user interaction.
            });

        open // Return whether the window is open.
    }
}

// Error popover struct.
pub struct Error {
    pub message: String,
}

impl Popover for Error {
    fn show(&mut self, ctx: &Context) -> bool {
        let mut open = true;

        // Create a window named "Error".
        egui::Window::new("Error")
            .collapsible(false) // Make the window non-collapsible.
            .open(&mut open) // Control the window's open state.
            .show(ctx, |ui| {
                ui.label(format!("Error: {}", self.message)); // Display the error message.
                ui.disable(); // Disable user interaction.
            });

        open // Return whether the window is open.
    }
}

// Query pane struct for filtering data.
pub struct QueryPane {
    filename: String,   // Filename of the Parquet file.
    table_name: String, // Table name to query.
    query: String,      // Query string for filtering.
}

impl QueryPane {
    // Creates a new QueryPane instance.
    pub fn new(filename: Option<String>, filters: &DataFilters) -> Self {
        Self {
            filename: filename.unwrap_or_default(), // Use default if no filename provided.
            query: filters.get_query(),             // Initialize query from DataFilters.
            table_name: filters.get_table_name(),   // Initialize table_name from DataFilters.
        }
    }

    // Renders the query pane UI.
    pub fn render(&mut self, ui: &mut Ui) -> Option<(String, DataFilters)> {
        ui.label("Filename:".to_string());
        ui.text_edit_singleline(&mut self.filename); // Text input for filename.

        ui.label("Table Name:".to_string());
        ui.text_edit_singleline(&mut self.table_name); // Text input for table name.

        ui.label("Query:".to_string());
        ui.text_edit_singleline(&mut self.query); // Text input for query.

        // If the button is clicked and the query is not empty:
        if ui.button("Apply").clicked() && !self.query.is_empty() {
            Some((
                self.filename.clone(), // Clone the filename.
                DataFilters {
                    query: Some(self.query.clone()), // Clone the query.
                    table_name: Some(self.table_name.clone()),
                    ..Default::default() // Use default values for other fields.
                },
            ))
        } else {
            None // Return None if the button is not clicked or the query is empty.
        }
    }
}

// Struct to hold Parquet file metadata.
pub struct FileMetadata {
    info: ParquetMetaData, // Parquet metadata.
}

impl FileMetadata {
    // Creates a FileMetadata instance from a filename.
    pub fn from_filename(filename: &str) -> Result<Self, String> {
        let path = Path::new(filename);
        // Open the file.
        match File::open(path) {
            Ok(file) => {
                // Create a SerializedFileReader.
                match SerializedFileReader::new(file) {
                    Ok(reader) => Ok(Self {
                        info: reader.metadata().to_owned(), // Store the metadata.
                    }),
                    Err(error) => {
                        // Propagate errors related to file reading
                        let msg = format!("fn from_filename()\n{}", error);
                        Err(msg)
                    }
                }
            }
            Err(_) => Err("Could not read metadata from file.".to_string()), // Propagate file open errors
        }
    }

    // Renders the file metadata in the UI.
    pub fn render_metadata(&self, ui: &mut Ui) {
        let file_metadata = self.info.file_metadata(); // Get file metadata.

        // Start a ui with vertical layout. Widgets will be left-justified.
        ui.vertical(|ui| {
            let metadata_created_by = file_metadata.created_by().unwrap_or("unknown");
            let version = format!("version: {}", file_metadata.version());
            let created_by = format!("created by: {}", metadata_created_by);
            let row_groups = format!("row groups: {}", self.info.num_row_groups());
            let columns = format!("columns: {}", file_metadata.schema_descr().num_columns());
            let rows = format!("rows: {}", file_metadata.num_rows());

            ui.label(version); // Display version.
            ui.label(created_by);
            ui.label(row_groups);
            ui.label(columns);
            ui.label(rows);

            // Set the minimum width of the ui.
            ui.set_min_width(200.0);
        });
    }

    // Renders the file schema in the UI.
    pub fn render_schema(&self, ui: &mut Ui) {
        let file_metadata = self.info.file_metadata(); // Get file metadata.
        for (idx, field) in file_metadata.schema_descr().columns().iter().enumerate() {
            // Iterate through columns.
            ui.collapsing(field.name(), |ui| {
                // Create a collapsing header for each column.
                let field_type = field.self_type(); // Get field type.
                let field_type = if field_type.is_primitive() {
                    format!("{}", field_type.get_physical_type()) // Format primitive type.
                } else {
                    format!("{}", field.converted_type()) // Format converted type.
                };
                ui.label(format!("type: {}", field_type)); // Display field type.
                ui.label(format!(
                    "sort_order: {}",
                    match file_metadata.column_order(idx) {
                        // Display sort order.
                        ColumnOrder::TYPE_DEFINED_ORDER(sort_order) => format!("{}", sort_order),
                        _ => "undefined".to_string(),
                    }
                ));
            });
        }
    }
}

impl ParquetData {
    // Renders the Parquet data in a table.
    pub fn render_table(&self, ui: &mut Ui) -> Option<DataFilters> {
        let style = ui.style().as_ref();

        // Helper function to check if a column is sorted.
        fn is_sorted_column(sorted_col: &Option<SortState>, col: &str) -> bool {
            match sorted_col {
                Some(sort) => match sort {
                    SortState::Ascending(sorted_col) => *sorted_col == col,
                    SortState::Descending(sorted_col) => *sorted_col == col,
                    _ => false,
                },
                None => false,
            }
        }

        let mut filters: Option<DataFilters> = None; // Filters to be returned, if any.
        let mut sorted_column = self.filters.sort.clone(); // Current sort state.

        let text_height = TextStyle::Body.resolve(style).size; // Height of a text line.

        let initial_col_width = (ui.available_width() - style.spacing.scroll.bar_width)
            / (self.data.num_columns() + 1) as f32; // Initial column width.

        // Stop columns from resizing to smaller than the window--remainder stops the last column
        // growing, which we explicitly want to allow for the case of large datatypes.
        let min_col_width = if style.spacing.interact_size.x > initial_col_width {
            style.spacing.interact_size.x
        } else {
            initial_col_width / 4.0
        };

        let header_height = style.spacing.interact_size.y + 2.0f32 * style.spacing.item_spacing.y; // Header height.

        // https://github.com/emilk/egui/issues/3680
        let column = Column::initial(initial_col_width)
            .at_least(min_col_width)
            .resizable(true)
            .clip(true); // Column properties.

        // Closure to analyze and render the table header.
        let analyze_header = |mut table_row: TableRow<'_, '_>| {
            for field in self.data.schema().fields() {
                // Iterate through the columns/fields.
                table_row.col(|ui| {
                    // Render in a column.
                    let column_label = if is_sorted_column(&sorted_column, field.name()) {
                        sorted_column.clone().unwrap() // Display sort state if sorted.
                    } else {
                        SortState::NotSorted(field.name().to_string()) // Default to not sorted.
                    };
                    //ui.centered_and_justified(|ui| {
                    ui.horizontal_centered(|ui| {
                        // Center content horizontally.
                        let response = ui.sort_button(&mut sorted_column, column_label.clone()); // Create a sort button.
                        if response.clicked() {
                            // If the sort button is clicked.
                            filters = Some(DataFilters {
                                sort: sorted_column.clone(), // Update the filters with the sort state.
                                ..self.filters.clone()
                            });
                        }
                    });
                });
            }
        };

        // Closure to analyze and render the table rows.
        let analyze_rows = |mut table_row: TableRow<'_, '_>| {
            let row_index = table_row.index(); // Get the row index.
            let schema = self.data.schema(); // Get the schema.

            // Iterate through columns with their schema fields.
            for (data_col_index, data_col) in self.data.columns().iter().enumerate() {
                let mut value: String =
                    array_value_to_string(data_col, row_index).unwrap_or_default(); // Get the cell value.

                // Get the field for the current column index.
                let field = schema.field(data_col_index);

                let layout = if data_col.data_type().is_floating() {
                    // Check if the column name contains "Alíquota"
                    let col_aliquota = field.name().contains("Alíquota");

                    // Convert string to floating point number
                    value = match value.trim().parse::<f64>() {
                        Ok(float) => {
                            // If column is Alíquota format to 4, else to 2 decimals
                            if col_aliquota {
                                format!("{float:0.4}") // Format to 4 decimal places if it is.
                            } else {
                                format!("{float:0.2}") // Otherwise, format to 2 decimal places.
                            }
                        }
                        Err(_) => value,
                    };
                    // If column is Alíquota align center, else align right
                    if col_aliquota {
                        Layout::centered_and_justified(egui::Direction::LeftToRight)
                    } else {
                        Layout::right_to_left(egui::Align::Center)
                    }
                } else if data_col.data_type().is_integer() {
                    Layout::centered_and_justified(egui::Direction::LeftToRight)
                } else {
                    Layout::left_to_right(egui::Align::Center)
                };

                table_row.col(|ui| {
                    // While not efficient (as noted in docs) we need to display
                    // at most a few dozen records at a time (barring pathological
                    // tables with absurd numbers of columns) and should still
                    // have conversion times on the order of ns.
                    ui.with_layout(layout.with_main_wrap(false), |ui| {
                        ui.label(value); // Display the value.
                    });
                });
            }
        };

        // Build the table.
        TableBuilder::new(ui)
            .striped(false) // false: takes all available height
            .columns(column, self.data.num_columns()) // Setup columns
            .column(Column::remainder())
            .auto_shrink([false, false])
            .min_scrolled_height(1000.0)
            .header(header_height, analyze_header) // Render header.
            .body(|body| {
                let num_rows = self.data.num_rows();
                body.rows(text_height, num_rows, analyze_rows); // Render rows.
            });

        filters // Return the filters.
    }
}

// Trait for selection depth, used for sort state.
pub trait SelectionDepth<Icon> {
    fn inc(&self) -> Self; // Increment the selection depth/state.

    fn reset(&self) -> Self; // Reset the selection depth/state.

    fn format(&self) -> Icon
    where
        Icon: Into<WidgetText>; // Format the selection depth/state.
}

// Trait implementation to increment the sort state.
impl SelectionDepth<String> for SortState {
    fn inc(&self) -> Self {
        match self {
            SortState::NotSorted(col) => SortState::Descending(col.to_owned()), // Not Sorted -> Descending.
            SortState::Ascending(col) => SortState::Descending(col.to_owned()), // Ascending -> Descending.
            SortState::Descending(col) => SortState::Ascending(col.to_owned()), // Descending -> Ascending.
        }
    }

    fn reset(&self) -> Self {
        // one day, I'll be proficient enough with macros that they'll be worth the time...
        match self {
            SortState::NotSorted(col) => SortState::NotSorted(col.to_owned()), // Reset to Not Sorted.
            SortState::Ascending(col) => SortState::NotSorted(col.to_owned()), // Reset to Not Sorted.
            SortState::Descending(col) => SortState::NotSorted(col.to_owned()), // Reset to Not Sorted.
        }
    }

    fn format(&self) -> String {
        match self {
            SortState::Descending(col) => format!("\u{23f7} {}", col), // Format for Descending.
            SortState::Ascending(col) => format!("\u{23f6} {}", col),  // Format for Ascending.
            SortState::NotSorted(col) => format!("\u{2195} {}", col),  // Format for Not Sorted.
        }
    }
}

// Trait for extra UI interactions.
pub trait ExtraInteractions {
    // Creates a sort button.
    fn sort_button<Value: PartialEq + SelectionDepth<Icon>, Icon: Into<WidgetText>>(
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
    ) -> Response;
}

// Implementation of ExtraInteractions for Ui.
impl ExtraInteractions for Ui {
    // Implementation of the sort button.
    fn sort_button<Value: PartialEq + SelectionDepth<Icon>, Icon: Into<WidgetText>>(
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
    ) -> Response {
        let selected = match current_value {
            Some(value) => *value == selected_value, // Check if the value is selected.
            None => false,
        };
        let mut response = self.selectable_label(selected, selected_value.format()); // Create a selectable label as a button.
        if response.clicked() {
            // If the button is clicked.
            if selected {
                *current_value = Some(selected_value.inc()); // Increment the value.
            } else {
                if let Some(value) = current_value {
                    value.reset(); // Reset the value.
                }
                *current_value = Some(selected_value.inc()); // Increment the value.
            };
            response.mark_changed(); // Mark the response as changed.
        }
        response // Return the response.
    }
}

// Asynchronously opens a file dialog.
pub async fn file_dialog() -> Result<String, String> {
    let opt_file_handle = AsyncFileDialog::new().pick_file().await; // Open the file dialog.

    match opt_file_handle {
        Some(file_handle) => Ok(file_handle.file_name()), // Return the filename if a file is selected.
        None => Err("No file loaded.".to_string()),       // Return an error if no file is selected.
    }
}
