use crate::Arguments;

use datafusion::{
    arrow::{compute::concat_batches, error::ArrowError, record_batch::RecordBatch},
    dataframe::DataFrame,
    logical_expr::col,
    prelude::{ParquetReadOptions, SessionContext},
};
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
    pub fn debug(args: &Arguments) {
        let data_filters = DataFilters {
            query: args.query.clone(),           //Avoid clone()
            table_name: args.table_name.clone(), //Avoid clone()
            ..Default::default()
        };

        dbg!(data_filters);
    }

    /// Retrieves the table name from the filters.
    pub fn get_table_name(&self) -> String {
        self.table_name
            .as_ref()
            .map(|tb| tb.name.clone())
            .unwrap_or_else(|| TableName::default().name)
    }

    /// Retrieves the query from the filters.
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
    pub data: Arc<RecordBatch>,
    /// The filters applied to the data.
    pub filters: DataFilters,
    /// The DataFusion DataFrame.
    dataframe: Arc<DataFrame>,
}

/// Concatenates an array of RecordBatch into one batch.
///
/// It reuses the schema of the first batch.
///
/// <https://docs.rs/datafusion/latest/datafusion/common/arrow/compute/kernels/concat/fn.concat_batches.html>
///
/// <https://docs.rs/datafusion/latest/datafusion/physical_plan/coalesce_batches/fn.concat_batches.html>
fn concat_record_batches(batches: &[RecordBatch]) -> Result<RecordBatch, ArrowError> {
    if batches.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "No batches to concatenate".to_string(),
        )); // Handle empty case
    }
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
        let read_options = get_read_options(&filename)
            .ok_or("Could not set read options. Does this file have a valid extension?")?;

        let df = ctx
            .read_parquet(&filename, read_options)
            .await
            .map_err(|e| format!("{}", e))?;

        let vec_record_batch = df.clone().collect().await.map_err(|e| e.to_string())?;
        let record_batch = concat_record_batches(&vec_record_batch).map_err(|e| e.to_string())?;

        Ok(ParquetData {
            filename,
            data: record_batch.into(),
            dataframe: df.into(),
            filters: DataFilters::default(),
        })
    }

    /// Loads Parquet data from a file and applies a query.
    pub async fn load_with_query(filename: String, filters: DataFilters) -> Result<Self, String> {
        let filename = shellexpand::full(&filename)
            .map_err(|err| err.to_string())?
            .to_string();

        dbg!(&filename);

        let ctx = SessionContext::new();
        let table_name = filters.get_table_name();
        let read_options = get_read_options(&filename)
            .ok_or("Could not set read options. Does this file have a valid extension?")?;

        // Use register_parquet directly, handle potential error
        ctx.register_parquet(&table_name, &filename, read_options)
            .await
            .map_err(|e| format!("Failed to register parquet table: {}", e))?;

        let query = filters.get_query();
        if query.is_empty() {
            return Err("No query provided".to_string());
        }

        let df = ctx.sql(&query).await.map_err(|e| e.to_string())?;
        let vec_record_batch = df.clone().collect().await.map_err(|e| e.to_string())?;
        let record_batch = concat_record_batches(&vec_record_batch).map_err(|e| e.to_string())?;

        let parquet_data = ParquetData {
            filename,
            data: record_batch.into(),
            dataframe: df.into(),
            filters,
        };

        parquet_data.sort(None).await
    }

    /// Sorts the data based on the provided filters.
    pub async fn sort(mut self, opt_filters: Option<DataFilters>) -> Result<Self, String> {
        let Some(filters) = opt_filters else {
            return Ok(self);
        };

        let Some(sort) = &filters.sort else {
            return Ok(self);
        };

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
        let df_sorted = df
            .sort(vec![exp])
            .map_err(|e| format!("Unable to sort column '{col_name}': {}", e))?;

        let vec_record_batch = df_sorted
            .clone()
            .collect()
            .await
            .map_err(|e| format!("Error collecting sorted data: {}", e))?;

        self.data = concat_record_batches(&vec_record_batch)
            .map_err(|e| format!("Error concatenating sorted batches: {}", e))?
            .into();
        self.dataframe = df_sorted.into(); //Update dataframe
        self.filters = filters; //Update filters

        Ok(self)
    }
}
