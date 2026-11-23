
use anyhow::{anyhow, bail, Context, Result};
use crossbeam_channel::{bounded, Sender};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Instant;
use tempfile::{Builder, NamedTempFile, TempDir};
use walkdir::WalkDir;

// --------------------------------------------------
// CONFIGURATION
// --------------------------------------------------

const BATCH_SIZE: usize = 4096;
const CHANNEL_CAPACITY: usize = 32;

struct Config {
    archive: PathBuf,
    output: PathBuf,
    workers: usize,
    seven_zip: String,
}

enum Message {
    Batch(Vec<Vec<String>>),
    Error(String),
}

fn usage() {
    println!(
        "CSVMerger.exe <archive.7z> <output.csv> [workers] [7z.exe]\n\
         \n\
         Example:\n\
         CSVMerger.exe data.7z combined.csv 6\n\
         CSVMerger.exe data.7z combined.csv 8 C:\\Program Files\\7-Zip\\7z.exe"
    );
}

fn parse_args() -> Result<Config> {
    let args: Vec<String> = env::args().skip(1).collect();

    if args.is_empty() || args.iter().any(|a| a == "--help" || a == "-h") {
        usage();
        std::process::exit(0);
    }

    if args.len() < 2 || args.len() > 4 {
        usage();
        bail!("Invalid arguments");
    }

    let archive = fs::canonicalize(&args[0])
        .with_context(|| format!("Cannot find archive: {}", args[0]))?;

    let workers = if args.len() >= 3 {
        args[2].parse::<usize>()?
    } else {
        thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .clamp(1, 8)
    };

    if workers == 0 || workers > 64 {
        bail!("Worker count must be between 1 and 64");
    }

    Ok(Config {
        archive,
        output: PathBuf::from(&args[1]),
        workers,
        seven_zip: args
            .get(3)
            .cloned()
            .unwrap_or_else(|| "7z".to_string()),
    })
}

// --------------------------------------------------
// 7-ZIP EXTRACTION
// --------------------------------------------------

fn run_7zip(
    executable: &str,
    arguments: &[&std::ffi::OsStr],
) -> Result<()> {
    let status = Command::new(executable)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| {
            format!(
                "Cannot execute {}. Install 7-Zip or specify its full path.",
                executable
            )
        })?;

    if !status.success() {
        bail!("7-Zip failed with exit status: {}", status);
    }

    Ok(())
}

fn extract_archive(
    config: &Config,
    temp: &TempDir,
) -> Result<()> {
    println!("\n[1/5] Checking archive integrity...");

    run_7zip(
        &config.seven_zip,
        &[
            std::ffi::OsStr::new("t"),
            config.archive.as_os_str(),
        ],
    )?;

    println!("\n[2/5] Extracting archive...");

    let destination = format!("-o{}", temp.path().display());

    run_7zip(
        &config.seven_zip,
        &[
            std::ffi::OsStr::new("x"),
            config.archive.as_os_str(),
            std::ffi::OsStr::new(&destination),
            std::ffi::OsStr::new("-y"),
            std::ffi::OsStr::new("-bd"),
        ],
    )?;

    Ok(())
}

// --------------------------------------------------
// CSV DISCOVERY
// --------------------------------------------------

fn discover_csv(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry?;

        if entry.file_type().is_file() {
            let path = entry.path();

            if path.extension()
                .and_then(|x| x.to_str())
                .map(|x| x.eq_ignore_ascii_case("csv"))
                .unwrap_or(false)
            {
                files.push(path.to_path_buf());
            }
        }
    }

    // Stable schema discovery order.
    files.sort();

    if files.is_empty() {
        bail!("No CSV files found in archive");
    }

    Ok(files)
}

// --------------------------------------------------
// HEADER NORMALIZATION
// --------------------------------------------------

fn read_header(path: &Path) -> Result<Vec<String>> {
    let mut reader = ReaderBuilder::new()
        .delimiter(b',')
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("Cannot open {}", path.display()))?;

    let headers = reader.headers()?.clone();

    let mut result = Vec::new();
    let mut seen = HashMap::new();

    for (i, field) in headers.iter().enumerate() {
        let name = if i == 0 {
            field.trim_start_matches('\u{feff}')
        } else {
            field
        };

        if name.is_empty() {
            bail!(
                "Empty column name in {}",
                path.display()
            );
        }

        if seen.insert(name.to_string(), i).is_some() {
            bail!(
                "Duplicate column '{}' in {}",
                name,
                path.display()
            );
        }

        result.push(name.to_string());
    }

    if result.is_empty() {
        bail!("Empty CSV header: {}", path.display());
    }

    Ok(result)
}

fn build_schema(files: &[PathBuf]) -> Result<Vec<String>> {
    println!("\n[3/5] Discovering CSV columns...");

    let mut schema = Vec::new();
    let mut lookup = HashMap::<String, usize>::new();

    for (i, path) in files.iter().enumerate() {
        let headers = read_header(path)?;

        for name in headers {
            if !lookup.contains_key(&name) {
                let index = schema.len();
                lookup.insert(name.clone(), index);
                schema.push(name);
            }
        }

        if (i + 1) % 500 == 0 {
            println!(
                "  Scanned headers: {}/{}",
                i + 1,
                files.len()
            );
        }
    }

    println!("  CSV files: {}", files.len());
    println!("  Unified columns: {}", schema.len());

    Ok(schema)
}

// --------------------------------------------------
// CSV WORKERS
// --------------------------------------------------

fn process_file(
    path: &Path,
    schema_lookup: &HashMap<String, usize>,
    column_count: usize,
    sender: &Sender<Message>,
) -> Result<()> {
    let mut reader = ReaderBuilder::new()
        .delimiter(b',')
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("Cannot open {}", path.display()))?;

    let headers = reader.headers()?.clone();

    let mut mapping = Vec::with_capacity(headers.len());
    let mut seen = HashMap::new();

    for (i, field) in headers.iter().enumerate() {
        let name = if i == 0 {
            field.trim_start_matches('\u{feff}')
        } else {
            field
        };

        if !seen.insert(name, ()).is_none() {
            bail!("Duplicate header in {}", path.display());
        }

        let index = schema_lookup
            .get(name)
            .ok_or_else(|| anyhow!("Unknown column: {}", name))?;

        mapping.push(*index);
    }

    let mut batch: Vec<Vec<String>> =
        Vec::with_capacity(BATCH_SIZE);

    for record in reader.records() {
        let record = record.with_context(|| {
            format!("CSV parsing error in {}", path.display())
        })?;

        let mut row = vec![String::new(); column_count];

        for (source_index, value) in record.iter().enumerate() {
            if let Some(&destination_index) = mapping.get(source_index) {
                row[destination_index] = value.to_string();
            }
        }

        batch.push(row);

        if batch.len() >= BATCH_SIZE {
            sender
                .send(Message::Batch(std::mem::take(&mut batch)))
                .map_err(|_| anyhow!("Output writer stopped"))?;

            batch = Vec::with_capacity(BATCH_SIZE);
        }
    }

    if !batch.is_empty() {
        sender
            .send(Message::Batch(batch))
            .map_err(|_| anyhow!("Output writer stopped"))?;
    }

    Ok(())
}

fn worker(
    files: Arc<Vec<PathBuf>>,
    schema_lookup: Arc<HashMap<String, usize>>,
    column_count: usize,
    next_file: Arc<AtomicUsize>,
    sender: Sender<Message>,
) {
    loop {
        let index = next_file.fetch_add(1, Ordering::Relaxed);

        if index >= files.len() {
            break;
        }

        let path = &files[index];

        if let Err(error) = process_file(
            path,
            &schema_lookup,
            column_count,
            &sender,
        ) {
            let _ = sender.send(Message::Error(format!(
                "{}: {:#}",
                path.display(),
                error
            )));
        }
    }
}

// --------------------------------------------------
// OUTPUT
// --------------------------------------------------

fn prepare_output(path: &Path) -> Result<NamedTempFile> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));

    fs::create_dir_all(parent)?;

    let temp = Builder::new()
        .prefix("csvmerge_")
        .suffix(".tmp")
        .tempfile_in(parent)?;

    Ok(temp)
}

fn write_output(
    temp_file: File,
    schema: &[String],
    receiver: crossbeam_channel::Receiver<Message>,
    total_files: usize,
) -> Result<(File, u64)> {
    let mut writer = WriterBuilder::new()
        .delimiter(b',')
        .has_headers(false)
        .from_writer(temp_file);

    writer.write_record(schema)?;

    let mut total_rows: u64 = 0;
    let mut errors = Vec::new();
    let mut write_error: Option<String> = None;
    let start = Instant::now();

    while let Ok(message) = receiver.recv() {
        match message {
            Message::Batch(batch) => {
                if write_error.is_none() {
                    for row in batch {
                        if let Err(e) = writer.write_record(&row) {
                            write_error = Some(e.to_string());
                            break;
                        }

                        total_rows += 1;
                    }
                }
            }

            Message::Error(error) => {
                errors.push(error);
            }
        }

        if total_rows > 0 && total_rows % 1_000_000 < BATCH_SIZE as u64 {
            let elapsed = start.elapsed().as_secs_f64();

            println!(
                "\r  Rows written: {:>15} | {:.0} rows/sec",
                total_rows,
                total_rows as f64 / elapsed.max(0.001)
            );
        }
    }

    if let Some(error) = write_error {
        bail!("Output write error: {}", error);
    }

    if !errors.is_empty() {
        eprintln!("\nCSV processing errors: {}", errors.len());

        for error in errors.iter().take(100) {
            eprintln!("  ERROR: {}", error);
        }

        if errors.len() > 100 {
            eprintln!("  ... additional errors omitted");
        }

        bail!(
            "Processing failed. Output will not be published."
        );
    }

    writer.flush()?;

    let file = writer.into_inner()
        .map_err(|e| anyhow!("Cannot finalize output: {}", e))?;

    Ok((file, total_rows))
}

// --------------------------------------------------
// MAIN
// --------------------------------------------------

fn main() -> Result<()> {
    let config = parse_args()?;

    let start = Instant::now();

    println!("======================================");
    println!("       NATIVE CSV MERGER 1.0");
    println!("======================================");

    println!("Archive : {}", config.archive.display());
    println!("Output  : {}", config.output.display());
    println!("Workers : {}", config.workers);

    if config.archive == config.output {
        bail!("Archive and output paths cannot be identical");
    }

    let temp_dir = TempDir::new()
        .context("Cannot create temporary directory")?;

    extract_archive(&config, &temp_dir)?;

    let files = discover_csv(temp_dir.path())?;

    println!("  Found {} CSV files", files.len());

    let schema = build_schema(&files)?;

    let schema_lookup: HashMap<String, usize> = schema
        .iter()
        .enumerate()
        .map(|(i, name)| (name.clone(), i))
        .collect();

    let output_temp = prepare_output(&config.output)?;
    let temp_path = output_temp.path().to_path_buf();

    let (sender, receiver) =
        bounded::<Message>(CHANNEL_CAPACITY);

    let files = Arc::new(files);
    let lookup = Arc::new(schema_lookup);
    let next_file = Arc::new(AtomicUsize::new(0));

    println!("\n[4/5] Processing CSV files...");
    println!("  Starting {} workers", config.workers);

    let mut handles = Vec::new();

    for _ in 0..config.workers {
        let files = Arc::clone(&files);
        let lookup = Arc::clone(&lookup);
        let next_file = Arc::clone(&next_file);
        let sender = sender.clone();

        let column_count = schema.len();

        let handle = thread::spawn(move || {
            worker(
                files,
                lookup,
                column_count,
                next_file,
                sender,
            );
        });

        handles.push(handle);
    }

    // Drop original sender so receiver closes
    // after all workers finish.
    drop(sender);

    let temp_file = output_temp
        .reopen()
        .context("Cannot open temporary output")?;

    let (output_file, total_rows) = write_output(
        temp_file,
        &schema,
        receiver,
        files.len(),
    )?;

    for handle in handles {
        handle.join()
            .map_err(|_| anyhow!("Worker thread panicked"))?;
    }

    // Ensure data is flushed to disk.
    output_file.sync_all()?;
    drop(output_file);

    println!("\n[5/5] Publishing final output...");

    // Windows cannot replace an existing file with
    // persist() in every situation. Remove old output
    // only after the new temporary file is complete.
    if config.output.exists() {
        fs::remove_file(&config.output)
            .context("Cannot replace existing output")?;
    }

    output_temp.persist(&config.output)
        .map_err(|e| anyhow!("Cannot publish output: {}", e))?;

    let elapsed = start.elapsed();

    println!("\n======================================");
    println!("             COMPLETED");
    println!("======================================");
    println!("Files       : {}", files.len());
    println!("Columns     : {}", schema.len());
    println!("Rows        : {}", total_rows);
    println!("Output      : {}", config.output.display());
    println!("Time        : {:.2} seconds", elapsed.as_secs_f64());

    if total_rows > 0 {
        println!(
            "Average     : {:.0} rows/sec",
            total_rows as f64 / elapsed.as_secs_f64().max(0.001)
        );
    }

    // Keep this variable referenced so the
    // compiler does not consider it accidental.
    let _ = temp_path;

    println!("======================================");

    Ok(())
}
