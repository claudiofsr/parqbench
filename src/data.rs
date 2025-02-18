use datafusion::{
    arrow::compute::concat_batches,
    arrow::{error::ArrowError, record_batch::RecordBatch},
    common::DFSchema,
    dataframe::DataFrame,
    logical_expr::col,
    prelude::{ParquetReadOptions, SessionContext},
};

use crate::Arguments;
use std::{
    ffi::{IntoStringError, OsStr},
    fmt::{Display, Formatter},
    future::Future,
    path::Path,
    str::FromStr,
    sync::Arc,
};

pub type DataResult = Result<ParquetData, String>;
pub type DataFuture = Box<dyn Future<Output = DataResult> + Unpin + Send + 'static>;

/// Determines the Parquet read options based on the file extension.
///
/// This function extracts the file extension from the given filename and returns
/// a `ParquetReadOptions` struct if a valid extension is found.  Currently,
/// the only relevant option set is the `file_extension`.
///
/// # Arguments
///
/// * `filename` - The name of the file to be read.
///
/// # Returns
///
/// An `Option` containing the `ParquetReadOptions` if a valid extension is found,
/// or `None` otherwise.
fn get_read_options(filename: &str) -> Option<ParquetReadOptions<'_>> {
    Path::new(filename)
        .extension()
        .and_then(OsStr::to_str)
        .map(|s| ParquetReadOptions {
            file_extension: s,
            ..Default::default()
        })
}

/// Represents a table name, used primarily for registering tables in DataFusion.
#[derive(Debug, Clone)]
pub struct TableName {
    pub name: String,
}

impl Default for TableName {
    fn default() -> Self {
        Self {
            name: "main".to_string(),
        }
    }
}

impl FromStr for TableName {
    type Err = IntoStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            name: s.to_string(),
        })
    }
}

impl Display for TableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Represents the sorting state for a column.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SortState {
    /// The column is not sorted.
    NotSorted(String),
    /// The column is sorted in ascending order.
    Ascending(String),
    /// The column is sorted in descending order.
    Descending(String),
}

/// Holds filters to be applied to the data.
#[derive(Clone, Debug, Default)]
pub struct DataFilters {
    /// Optional sorting state.
    pub sort: Option<SortState>,
    /// Optional table name for DataFusion registration.
    pub table_name: Option<TableName>,
    /// Optional SQL query to apply.
    pub query: Option<String>,
}

impl DataFilters {
    /// Prints the debug information about the `DataFilters` based on the provided `Arguments`.
    ///
    /// This function clones the provided `Arguments`, creates a `DataFilters` instance with the
    /// `query` and `table_name` fields from the arguments, and then prints the debug representation
    /// of the created `DataFilters` instance using `dbg!`.
    ///
    /// # Arguments
    ///
    /// * `args` - A reference to the `Arguments` struct containing the query and table name.
    pub fn debug(args: &Arguments) {
        let args = args.clone();

        let data_filters = DataFilters {
            query: args.query,
            table_name: args.table_name,
            ..Default::default()
        };

        dbg!(data_filters);
    }

    /// Retrieves the table name from the filters.
    ///
    /// If a table name is present in the filters, it returns the name.
    /// Otherwise, it returns the default table name.
    pub fn get_table_name(&self) -> String {
        match self.table_name.as_ref() {
            Some(tb) => tb.name.clone(),
            None => TableName::default().name,
        }
    }

    /// Retrieves the query from the filters.
    ///
    /// If a query is present in the filters, it returns the query.
    /// Otherwise, it returns an empty string.
    pub fn get_query(&self) -> String {
        self.query.clone().unwrap_or_default()
    }
}

/// Contains the Parquet data, filename, filters, and a DataFusion DataFrame.
#[derive(Clone)]
pub struct ParquetData {
    /// The filename of the Parquet file.
    pub filename: String,
    /// The data as a RecordBatch.
    pub data: RecordBatch,
    /// The filters applied to the data.
    pub filters: DataFilters,
    /// The DataFusion DataFrame.
    dataframe: Arc<DataFrame>,
}

/// Concatenates an array of RecordBatch into one batch
///
/// <https://docs.rs/datafusion/latest/datafusion/common/arrow/compute/kernels/concat/fn.concat_batches.html>
///
/// <https://docs.rs/datafusion/latest/datafusion/physical_plan/coalesce_batches/fn.concat_batches.html>
fn concat_record_batches(batches: &[RecordBatch]) -> Result<RecordBatch, ArrowError> {
    concat_batches(&batches[0].schema(), batches)
}

impl ParquetData {
    /// Loads Parquet data from a file.
    pub async fn load(filename: String) -> Result<Self, String> {
        let filename = shellexpand::full(&filename)
            .map_err(|err| err.to_string())?
            .to_string();

        dbg!(&filename);

        let ctx = SessionContext::new();
        match ctx
            .read_parquet(
                &filename,
                get_read_options(&filename).ok_or(
                    "Could not set read options. Does this file have a valid extension?"
                        .to_string(),
                )?,
            )
            .await
        {
            Ok(df) => match df.clone().collect().await {
                Ok(vec_record_batch) => {
                    let record_batch =
                        concat_record_batches(&vec_record_batch).map_err(|err| err.to_string())?;
                    let parquet_data = ParquetData {
                        filename,
                        data: record_batch,
                        dataframe: df.into(),
                        filters: DataFilters::default(),
                    };
                    Ok(parquet_data)
                }
                Err(msg) => Err(msg.to_string()),
            },
            Err(msg) => Err(format!("{}", msg)),
        }
    }

    /// Loads Parquet data from a file and applies a query.
    pub async fn load_with_query(filename: String, filters: DataFilters) -> Result<Self, String> {
        let filename = shellexpand::full(&filename)
            .map_err(|err| err.to_string())?
            .to_string();

        dbg!(&filename);

        let ctx = SessionContext::new();
        ctx.register_parquet(
            filters.get_table_name(),
            &filename,
            get_read_options(&filename).ok_or(
                "Could not set read options. Does this file have a valid extension?".to_string(),
            )?,
        )
        .await
        .ok();

        match &filters.query {
            Some(query) => match ctx.sql(query.as_str()).await {
                Ok(df) => match df.clone().collect().await {
                    Ok(vec_record_batch) => {
                        let record_batch = concat_record_batches(&vec_record_batch)
                            .map_err(|err| err.to_string())?;
                        let parquet_data = ParquetData {
                            filename: filename.to_owned(),
                            data: record_batch,
                            dataframe: df.into(),
                            filters,
                        };
                        parquet_data.sort(None).await
                    }
                    Err(msg) => Err(msg.to_string()),
                },
                Err(msg) => Err(msg.to_string()), // two classes of error, sql and file
            },
            None => Err("No query provided".to_string()),
        }
    }

    /// Sorts the data based on the provided filters.
    pub async fn sort(self, opt_filters: Option<DataFilters>) -> Result<Self, String> {
        match opt_filters {
            Some(filters) => match filters.sort.as_ref() {
                Some(sort) => {
                    let (col_name, ascending) = match sort {
                        SortState::Ascending(col_name) => (col_name, true),
                        SortState::Descending(col_name) => (col_name, false),
                        SortState::NotSorted(_col_name) => return Ok(self),
                    };

                    dbg!(sort);
                    dbg!(col_name);
                    dbg!(ascending);

                    let df: DataFrame = self.dataframe.as_ref().clone();
                    let exp = col(col_name).sort(ascending, false);
                    let sorted = df.sort(vec![exp]);

                    match sorted {
                        Ok(df) => match df.clone().collect().await {
                            Ok(vec_record_batch) => {
                                let record_batch = concat_record_batches(&vec_record_batch)
                                    .map_err(|err| err.to_string())?;
                                let parquet_data = ParquetData {
                                    filename: self.filename,
                                    data: record_batch,
                                    dataframe: df.into(),
                                    filters,
                                };
                                Ok(parquet_data)
                            }
                            Err(msg) => {
                                let error_msg = format!("Error sorting data: {msg}!");
                                Err(error_msg)
                            }
                        },
                        Err(msg) => {
                            let error_msg1 = format!("Unable to sort column '{col_name}'\n");
                            let error_msg2 = format!("Selected filter: {filters:?}\n");
                            let error_msg3 = format!("Error message: {msg}!");
                            let error_msg = [error_msg1, error_msg2, error_msg3].concat();
                            Err(error_msg)
                        }
                    }
                }
                None => Ok(self),
            },
            None => Ok(self),
        }
    }

    /// Returns the metadata of the DataFrame.
    pub fn metadata(&self) -> &DFSchema {
        self.dataframe.schema()
    }
}
