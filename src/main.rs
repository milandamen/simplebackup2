use anyhow::{anyhow, bail, Context, Result};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufRead, Read, Write};
use std::path::{Path, MAIN_SEPARATOR};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::SystemTime;
use std::{cmp, fs, io};

const SNAPSHOT_DIR: &str = "snapshots";
const SNAPSHOT_EXTENSION: &str = ".snapshot";
const DIFF_EXTENSION: &str = ".diff";

fn main() -> ExitCode {
    println!("SimpleBackup 2");

    loop {
        println!("Available options:");
        println!("1: Generate new snapshot and generate difference between previous snapshot");
        println!("3: Use last diff file to copy files to backup");
        println!("q: Exit SimpleBackup");

        let response = &mut String::new();
        if let Err(err) = io::stdin().read_line(response) {
            eprintln!("cannot read line from input: {}", err);
            return ExitCode::FAILURE;
        }

        match response.trim() {
            "1" => {
                match create_snapshot_and_diff() {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("cannot create snapshot and diff: {:#}", err);
                        return ExitCode::FAILURE;
                    }
                }
                break;
            }
            "3" => {
                match load_diff_and_do_backup() {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("cannot load diff and do backup: {:#}", err);
                        return ExitCode::FAILURE;
                    }
                }
                break;
            }
            "q" => return ExitCode::SUCCESS,
            _ => {
                eprintln!("unknown command: {}", response.trim());
            }
        }
    }

    ExitCode::SUCCESS
}

fn create_snapshot_and_diff() -> Result<()> {
    ensure_snapshot_dir()?;
    let root_path = ask_dir_path("Enter the path from which you want to snapshot:")?;

    println!("Collecting list of files...");
    let now = SystemTime::now();
    let mut file_list: Vec<FileEntry> = vec![];
    visit_collect(&root_path, &mut file_list)
        .with_context(|| format!("cannot create list for path '{}'", root_path.display()))?;

    println!("Created list of files: {}", file_list.len());
    println!("Sorting it...");
    file_list.sort_unstable_by(|a, b| a.path.cmp(&b.path));
    let file_list = Arc::new(file_list);

    let previous_snapshot =
        load_previous_snapshot(&root_path).context("cannot load previous snapshot")?;

    println!("Generating diff list...");
    let diff_list =
        diff_snapshots(previous_snapshot, &file_list).context("cannot diff snapshots")?;

    println!("Number of files changed: {}", diff_list.len());

    save_snapshot(now, &root_path, &file_list).context("cannot save snapshot")?;
    save_diff(&root_path, &diff_list).context("cannot save diff file")?;

    Ok(())
}

struct FileEntry {
    path: String,
    modified: u64,
}

fn visit_collect(path: &Path, file_list: &mut Vec<FileEntry>) -> Result<()> {
    // symlink_metadata also causes is_dir and is_file to return false for symlinks, skipping them.
    let md = path
        .symlink_metadata()
        .with_context(|| format!("cannot get metadata for path '{}'", path.display()))?;

    if md.is_dir() {
        let entries: Vec<String> = fs::read_dir(path)
            .with_context(|| format!("cannot read directory '{}'", path.display()))?
            .map(|res| {
                res.map(|e| {
                    let p = e.path();
                    p.to_str().map(|s| s.to_string()).with_context(|| {
                        format!("path '{}' contains invalid UTF-8 data", p.display())
                    })
                })?
            })
            .collect::<Result<_>>()?;

        for p in entries.iter() {
            visit_collect(Path::new(p), file_list)?;
        }
    } else if md.is_file() {
        let modified = md
            .modified()
            .with_context(|| format!("cannot get modified time for file '{}'", path.display()))?
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();

        // We know s is valid Unicode because it was checked when the parent directory was visited,
        // so that's why we can unwrap here.
        let s = path.to_str().unwrap();
        file_list.push(FileEntry {
            path: s.to_string(),
            modified,
        })
    }

    Ok(())
}

fn load_previous_snapshot(root_path: &Path) -> Result<Option<Vec<FileEntry>>> {
    let (f, path) = match find_latest_file(root_path, SNAPSHOT_EXTENSION)? {
        None => return Ok(None),
        Some(tup) => tup,
    };

    println!("Loading previous snapshot file '{}'", path);
    let mut snapshot: Vec<FileEntry> = vec![];
    let mut reader = io::BufReader::new(f);
    let mut line_idx: u64 = 2;
    loop {
        let entry = read_snapshot_line(&mut reader)
            .with_context(|| format!("error on line {}", line_idx))?;

        match entry {
            ReadLine::Value(e) => {
                snapshot.push(e);
                line_idx += 1;
            }
            ReadLine::Empty => {}
            ReadLine::Eof => {
                break;
            }
        }
    }

    Ok(Some(snapshot))
}

enum ReadLine<T> {
    Value(T),
    Empty,
    Eof,
}

fn read_snapshot_line(reader: &mut io::BufReader<File>) -> Result<ReadLine<FileEntry>> {
    let mut line = String::new();
    reader.read_line(&mut line).map(|n| {
        if n == 0 {
            // EOF
            return Ok(ReadLine::Eof);
        }

        let line = line.trim();
        if line.is_empty() {
            return Ok(ReadLine::Empty);
        }

        let mut split = line.split('\t');
        let modified_str = split.next().context("line contained no data")?;
        let modified: u64 = modified_str
            .parse()
            .with_context(|| format!("cannot parse modified time '{}' as integer", modified_str))?;

        let file_path = split.next().context("line contained no file path")?;

        if split.next().is_some() {
            bail!("line contained extra data");
        }

        Ok(ReadLine::Value(FileEntry {
            path: file_path.to_string(),
            modified,
        }))
    })?
}

fn diff_snapshots(
    previous_snapshot: Option<Vec<FileEntry>>,
    snapshot: &Arc<Vec<FileEntry>>,
) -> Result<Vec<String>> {
    match previous_snapshot {
        None => {
            println!("No previous snapshot found, so diff file will be the whole list of files.");
            Ok(snapshot
                .iter()
                .map(|entry| entry.path.to_string())
                .collect::<Vec<_>>())
        }
        Some(previous_snapshot) => {
            let n = std::thread::available_parallelism()
                .context("cannot get available number of CPUs")?
                .get();

            // If there are fewer files than threads, have each thread handle 1 file.
            let n = cmp::min(n, snapshot.len());

            let previous_snapshot = Arc::new(previous_snapshot);

            let mut threads: Vec<std::thread::JoinHandle<_>> = Vec::with_capacity(n);
            let chunk_size = snapshot.len() / n;
            let mut chunk_offset = 0;
            for i in 0..n {
                let until = if i == n - 1 {
                    snapshot.len()
                } else {
                    chunk_offset + chunk_size
                };
                let snapshot = Arc::clone(snapshot);
                let previous_snapshot = Arc::clone(&previous_snapshot);
                threads.push(std::thread::spawn(move || -> Vec<String> {
                    let chunk = &snapshot[chunk_offset..until];
                    diff_chunk(previous_snapshot, chunk)
                }));

                chunk_offset += chunk_size;
            }

            let mut result = Vec::with_capacity(chunk_size);
            for thread in threads.into_iter() {
                let chunk_diff = thread.join().unwrap();
                result.extend(chunk_diff.into_iter());
            }

            Ok(result)
        }
    }
}

fn diff_chunk(previous_snapshot: Arc<Vec<FileEntry>>, chunk: &[FileEntry]) -> Vec<String> {
    let mut result = vec![];
    for e in chunk {
        let idx = binary_search(e.path.as_str(), &previous_snapshot);
        match idx {
            Some(idx) => {
                let prev_entry = &previous_snapshot[idx];
                if prev_entry.modified != e.modified {
                    result.push(e.path.clone());
                }
            }
            None => result.push(e.path.clone()),
        }
    }
    result
}

fn binary_search(file_path: &str, previous_snapshot: &[FileEntry]) -> Option<usize> {
    let mut min = 0;
    let mut max = previous_snapshot.len() - 1;
    while min <= max {
        let middle = min + (max - min) / 2;
        let entry = &previous_snapshot[middle];
        let o = file_path.cmp(entry.path.as_str());
        match o {
            Ordering::Less => {
                if middle == 0 {
                    return None;
                }
                max = middle - 1
            }
            Ordering::Equal => return Some(middle),
            Ordering::Greater => min = middle + 1,
        }
    }

    None
}

fn save_snapshot(time: SystemTime, root_path: &Path, file_list: &[FileEntry]) -> Result<()> {
    let snapshot_path = Path::new(SNAPSHOT_DIR).join(
        time.duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs()
            .to_string()
            + SNAPSHOT_EXTENSION,
    );
    let mut file = File::create_new(&snapshot_path)
        .with_context(|| format!("cannot create snapshot file '{}'", snapshot_path.display()))?;

    let mut write = |data: &str| -> Result<()> {
        file.write_all(data.as_bytes())
            .context("cannot write to snapshot file")
    };

    // We know root_path is valid Unicode because it was checked before, so that's why we can unwrap here.
    write(root_path.to_str().unwrap())?;
    write("\n")?;

    for entry in file_list.iter() {
        write(entry.modified.to_string().as_str())?;
        write("\t")?;
        write(entry.path.as_str())?;
        write("\n")?;
    }

    Ok(())
}

fn save_diff(root_path: &Path, diff_list: &[String]) -> Result<()> {
    let diff_path = Path::new(SNAPSHOT_DIR).join(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs()
            .to_string()
            + DIFF_EXTENSION,
    );
    let mut file = File::create_new(&diff_path)
        .with_context(|| format!("cannot create diff file '{}'", diff_path.display()))?;

    let mut write = |data: &str| -> Result<()> {
        file.write_all(data.as_bytes())
            .context("cannot write to diff file")
    };

    // We know root_path is valid Unicode because it was checked before, so that's why we can unwrap here.
    write(root_path.to_str().unwrap())?;
    write("\n")?;

    for file_path in diff_list.iter() {
        write(file_path.as_str())?;
        write("\n")?;
    }

    file.sync_all()
        .context("cannot complete writing diff file")?;

    Ok(())
}

fn load_diff_and_do_backup() -> Result<()> {
    ensure_snapshot_dir()?;
    let root_path = ask_dir_path("Enter the source path you want to backup:")?;
    let diff_list = load_previous_diff(&root_path)?;
    let dest_path = ask_dir_path("Enter the destination path you want to backup to:")?;
    backup(&diff_list, &root_path, &dest_path).context("cannot perform backup")?;

    Ok(())
}

fn load_previous_diff(root_path: &Path) -> Result<Vec<String>> {
    let (f, path) = find_latest_file(root_path, DIFF_EXTENSION)?
        .context("no latest diff file found for the given path")?;
    println!("Loading previous diff file '{}'", path);
    let mut diff_list: Vec<String> = vec![];
    let mut reader = io::BufReader::new(f);
    let mut line_idx: u64 = 1;
    let mut line = String::new();

    // We know root_path_str is valid Unicode because it was checked when the parent directory was visited,
    // so that's why we can unwrap here.
    let mut root_path_str = String::from(root_path.to_str().unwrap());
    root_path_str.push(MAIN_SEPARATOR);
    loop {
        line_idx += 1;
        line.clear();
        let n = reader
            .read_line(&mut line)
            .with_context(|| format!("error on line {}", line_idx))?;
        if n == 0 {
            // EOF.
            break;
        }
        let line_trimmed = line.trim();
        if line_trimmed.is_empty() {
            // empty line
            continue;
        }

        if line_trimmed.starts_with(&root_path_str) && line_trimmed.len() > (root_path_str.len()) {
            let next_char = line_trimmed.as_bytes()[root_path_str.len()];
            if next_char == MAIN_SEPARATOR.try_into()? {
                bail!(
                    "line {} contains an invalid character '{}' after the root path: {}",
                    line_idx,
                    MAIN_SEPARATOR,
                    line_trimmed
                );
            }

            diff_list.push(line_trimmed.to_string());
        } else {
            bail!(
                "line {} must start with root path, but was: {}",
                line_idx,
                line_trimmed
            );
        }
    }

    Ok(diff_list)
}

fn backup(diff_list: &[String], root_path: &Path, dest_path: &Path) -> Result<()> {
    // We know root_path_str is valid Unicode because it was checked when the parent directory was visited,
    // so that's why we can unwrap here.
    let mut root_path_str = String::from(root_path.to_str().unwrap());
    root_path_str.push(MAIN_SEPARATOR);
    for source_path in diff_list {
        let source_path_rel = &source_path[root_path_str.len()..];
        println!("source_path_rel: {}", source_path_rel); // TODO remove
        let s = format!(
            "{}{}{}",
            dest_path.display(),
            MAIN_SEPARATOR,
            source_path_rel
        );
        let dest_path_abs = Path::new(s.as_str());
        println!("dest_path_abs: {}", dest_path_abs.display()); // TODO remove

        let source_path_p = Path::new(source_path);
        if let Err(err) = source_path_p.metadata() {
            println!(
                "cannot get info for path '{}': {}",
                source_path_p.display(),
                err
            );
            continue;
        }

        // We know source_path_p has a parent, so we can unwrap here.
        let source_parent = source_path_p.parent().unwrap();

        // We know dest_path_abs has a parent, so we can unwrap here.
        let dest_parent = dest_path_abs.parent().unwrap();
        ensure_backup_dir(source_parent, dest_parent)?;

        let options = &mut uu_cp::Options::default();
        options.attributes = uu_cp::Attributes::ALL;
        options.recursive = true;
        options.dereference = false;
        options.copy_mode = uu_cp::CopyMode::Update;
        options.update = uu_cp::UpdateMode::IfOlder;

        // TODO find out why a file is copied even if the dest contains a newer file.
        uu_cp::copy(&[source_path_p.to_path_buf()], dest_path_abs, options).with_context(|| {
            format!(
                "cannot copy file from path '{}' to path '{}'",
                source_path,
                dest_path_abs.display()
            )
        })?;
        break;
    }
    Ok(())
}

fn ensure_backup_dir(source_dir_path: &Path, dest_dir_path: &Path) -> Result<()> {
    match dest_dir_path.metadata() {
        Ok(_) => Ok(()),
        Err(err) => {
            if err.kind() != io::ErrorKind::NotFound {
                bail!(
                    "cannot get info for directory '{}': {}",
                    dest_dir_path.display(),
                    err
                );
            }

            // We can unwrap here because source path is always a subdirectory of root_path,
            // which has already been verified to exist.
            let source_parent = source_dir_path.parent().unwrap();
            // We can unwrap here because the root dest_path was already checked to exist,
            // so there will always be at least that one as a parent.
            let dest_parent = dest_dir_path.parent().unwrap();
            ensure_backup_dir(source_parent, dest_parent)?;

            let source_meta = source_dir_path.metadata().with_context(|| {
                format!(
                    "cannot get metadata for path '{}'",
                    source_dir_path.display()
                )
            })?;
            let perms = source_meta.permissions();

            fs::create_dir(dest_dir_path).with_context(|| {
                format!(
                    "cannot create directory '{}': {}",
                    dest_dir_path.display(),
                    err
                )
            })?;
            fs::set_permissions(dest_dir_path, perms).with_context(|| {
                format!(
                    "cannot set permissions for new directory '{}'",
                    dest_dir_path.display()
                )
            })
        }
    }
}

fn ensure_snapshot_dir() -> Result<()> {
    match fs::metadata(SNAPSHOT_DIR) {
        Ok(_) => Ok(()),
        Err(err) => {
            if err.kind() != io::ErrorKind::NotFound {
                bail!(
                    "cannot get info for snapshot directory at '{}': {}",
                    SNAPSHOT_DIR,
                    err
                );
            }

            fs::create_dir(SNAPSHOT_DIR).with_context(|| {
                format!(
                    "cannot create snapshot directory at '{}': {}",
                    SNAPSHOT_DIR, err
                )
            })
        }
    }
}

fn find_latest_file(root_path: &Path, ext: &str) -> Result<Option<(File, String)>> {
    let entries = read_snapshot_dir_entries(ext)?;
    for p in entries.iter().rev() {
        let expected = root_path.to_str().unwrap().to_string() + "\n";
        let mut buffer = expected.as_bytes().to_vec();

        let path = format!("{}{}{}", SNAPSHOT_DIR, MAIN_SEPARATOR, p);
        let mut f = File::open(&path).with_context(|| format!("cannot open file '{}'", path))?;

        if let Err(err) = f.read_exact(&mut buffer) {
            eprintln!("cannot read file '{}': {}", path, err);
            continue;
        }

        if buffer == expected.as_bytes() {
            return Ok(Some((f, path)));
        }
    }

    Ok(None)
}

fn read_snapshot_dir_entries(ext: &str) -> Result<Vec<String>> {
    let mut entries: Vec<String> = fs::read_dir(SNAPSHOT_DIR)
        .with_context(|| format!("cannot read snapshot directory '{}'", SNAPSHOT_DIR))?
        .map(|res| {
            res.map(|e| {
                let p = e.path();
                let os_file_name = e.file_name();
                os_file_name
                    .into_string()
                    .map_err(|_| anyhow!("path '{}' contains invalid UTF-8 data", p.display()))
            })?
        })
        .filter(|res| res.is_ok() && res.as_ref().unwrap().ends_with(ext))
        .collect::<Result<_>>()?;
    entries.sort_unstable();
    Ok(entries)
}

fn ask_dir_path(question: &str) -> Result<Box<Path>> {
    let mut root_path: Box<Path>;
    let mut response = String::new();
    loop {
        response.clear();

        println!("{}", question);
        io::stdin()
            .read_line(&mut response)
            .context("cannot read line from input")?;

        root_path = Path::new(response.trim().trim_end_matches(MAIN_SEPARATOR)).into();
        if root_path.to_str().is_none() {
            eprintln!("path '{}' is not valid UTF-8", root_path.display());
            continue;
        }

        if !root_path.is_absolute() {
            eprintln!("path '{}' is not absolute", root_path.display());
            continue;
        }

        let is_dir = match root_path.metadata().map(|md| md.is_dir()) {
            Ok(is_dir) => is_dir,
            Err(err) => {
                eprintln!(
                    "cannot check if path '{}' is a directory: {}",
                    root_path.display(),
                    err
                );
                continue;
            }
        };

        if !is_dir {
            eprintln!("path '{}' is not a directory", root_path.display());
            continue;
        }

        break;
    }

    Ok(root_path)
}
