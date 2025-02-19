use crate::TableName;
use clap::Parser;

// Function to define styles for clap's help output.
fn get_styles() -> clap::builder::Styles {
    let cyan = anstyle::Color::Ansi(anstyle::AnsiColor::Cyan);
    let green = anstyle::Color::Ansi(anstyle::AnsiColor::Green);
    let yellow = anstyle::Color::Ansi(anstyle::AnsiColor::Yellow);

    clap::builder::Styles::styled()
        .placeholder(anstyle::Style::new().fg_color(Some(yellow)))
        .usage(anstyle::Style::new().fg_color(Some(cyan)).bold())
        .header(
            anstyle::Style::new()
                .fg_color(Some(cyan))
                .bold()
                .underline(),
        )
        .literal(anstyle::Style::new().fg_color(Some(green)))
}

// Custom template for clap's help output.
const APPLET_TEMPLATE: &str = "\
{before-help}
{about-with-newline}
{usage-heading} {usage}

{all-args}
{after-help}";

// Structure to define command-line arguments.
#[derive(Parser, Debug, Clone)]
#[command(
    // Read metadata from `Cargo.toml`
    author,
    version,
    about,
    long_about = None,
    next_line_help = true,
    help_template = APPLET_TEMPLATE,
    styles = get_styles(),
)]
pub struct Arguments {
    /// Sets the Parquet filename.
    pub filename: Option<String>,

    /// Sets the query.
    #[arg(short('q'), long("query"), required = false, requires = "filename")]
    pub query: Option<String>,

    /// Sets the TableName.
    #[arg(short('t'), long("table_name"), required = false, requires = "query")]
    pub table_name: Option<TableName>,
}

impl Arguments {
    /// Builds and parses command-line arguments.
    pub fn build() -> Self {
        Arguments::parse()
    }
}
