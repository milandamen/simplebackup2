use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::ExitCode;
use std::time::SystemTime;
use std::{fs, io};

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
                        eprintln!("cannot create snapshot and diff: {}", err);
                        return ExitCode::FAILURE;
                    }
                }
                break;
            }
            "3" => {
                println!("3qwe");
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

fn create_snapshot_and_diff() -> io::Result<()> {
    ensure_snapshot_dir()?;

    let mut root_path: &Path;
    let mut response = String::new();
    loop {
        response.clear();

        println!("Enter the path from which you want to snapshot:");
        wrap_err(io::stdin().read_line(&mut response), || {
            String::from("cannot read line from input")
        })?;

        root_path = Path::new(response.trim());
        if root_path.to_str().is_none() {
            eprintln!("path '{}' is not valid UTF-8", root_path.display());
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

    println!("Collecting list of files...");
    let now = SystemTime::now();
    let mut file_list: Vec<FileEntry> = vec![];
    wrap_err(visit(root_path, &mut file_list), || {
        format!("cannot create list for path '{}'", root_path.display())
    })?;

    println!("Created list of files: {}", file_list.len());

    // TODO move saving of snapshot till after the diff has been done, so that an error when diffing does not cause a next execution to generate a different diff list based on the now-saved snapshot.
    wrap_err(save_snapshot(now, root_path, &file_list), || {
        String::from("cannot save snapshot")
    })?;

    wrap_err(
        save_diff(
            root_path,
            &file_list
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
        ),
        || String::from("cannot save diff file"),
    )?;

    Ok(())
}

struct FileEntry {
    path: String,
    modified: SystemTime,
}

fn visit(path: &Path, file_list: &mut Vec<FileEntry>) -> io::Result<()> {
    // symlink_metadata also causes is_dir and is_file to return false for symlinks, skipping them.
    let md = wrap_err(path.symlink_metadata(), || {
        format!("cannot get metadata for path '{}'", path.display())
    })?;

    if md.is_dir() {
        let iter = wrap_err(fs::read_dir(path), || {
            format!("cannot read directory '{}'", path.display())
        })?;

        let res = iter
            .map(|res| match res {
                Ok(e) => {
                    let p = e.path();
                    match p.to_str() {
                        Some(s) => Ok(s.to_string()),
                        None => Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("path '{}' contains invalid UTF-8 data", p.display()),
                        )),
                    }
                }
                Err(err) => Err(err),
            })
            .collect::<Result<_, io::Error>>();

        let mut entries: Vec<String> = wrap_err(res, || {
            format!("cannot collect contents of directory '{}'", path.display())
        })?;

        entries.sort_unstable();
        for p in entries.iter() {
            visit(Path::new(p), file_list)?;
        }
    } else if md.is_file() {
        let modified = wrap_err(md.modified(), || {
            format!("cannot get modified time for file '{}'", path.display())
        })?;

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

fn load_previous_snapshot(root_path: &Path) -> io::Result<Vec<FileEntry>> {
    let iter = wrap_err(fs::read_dir(SNAPSHOT_DIR), || {
        format!("cannot read snapshot directory '{}'", SNAPSHOT_DIR)
    })?;

    let res = iter
        .map(|res| match res {
            Ok(e) => {
                let p = e.path();
                let os_file_name = e.file_name();
                match os_file_name.into_string() {
                    Ok(file_name) => Ok(file_name),
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("path '{}' contains invalid UTF-8 data", p.display()),
                    )),
                }
            }
            Err(err) => Err(err),
        })
        .filter(|res| res.is_ok() && res.as_ref().unwrap().ends_with(SNAPSHOT_EXTENSION))
        .collect::<Result<_, io::Error>>();

    let mut entries: Vec<String> = wrap_err(res, || {
        format!("cannot collect contents of directory '{}'", SNAPSHOT_DIR)
    })?;

    entries.sort_unstable();
    for p in entries.iter() {
        println!("snapshot file: {}", p) // TODO find latest relevant snapshot
    }

    Ok(vec![]) // TODO fix return value
}

fn save_snapshot(time: SystemTime, root_path: &Path, file_list: &[FileEntry]) -> io::Result<()> {
    let snapshot_path = Path::new(SNAPSHOT_DIR).join(
        time.duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string()
            + SNAPSHOT_EXTENSION,
    );
    let mut file = wrap_err(File::create_new(&snapshot_path), || {
        format!("cannot create snapshot file '{}'", snapshot_path.display())
    })?;

    let mut write = |data: &str| -> io::Result<()> {
        wrap_err(file.write_all(data.as_bytes()), || {
            String::from("cannot write to snapshot file")
        })
    };

    // We know root_path is valid Unicode because it was checked before, so that's why we can unwrap here.
    write(root_path.to_str().unwrap())?;
    write("\n")?;

    for entry in file_list.iter() {
        write(
            entry
                .modified
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .to_string()
                .as_str(),
        )?;
        write("\t")?;
        write(entry.path.as_str())?;
        write("\n")?;
    }

    Ok(())
}

fn save_diff(root_path: &Path, diff_list: &Vec<&str>) -> io::Result<()> {
    let diff_path = Path::new(SNAPSHOT_DIR).join(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string()
            + DIFF_EXTENSION,
    );
    let mut file = wrap_err(File::create_new(&diff_path), || {
        format!("cannot create diff file '{}'", diff_path.display())
    })?;

    let mut write = |data: &str| -> io::Result<()> {
        wrap_err(file.write_all(data.as_bytes()), || {
            String::from("cannot write to diff file")
        })
    };

    // We know root_path is valid Unicode because it was checked before, so that's why we can unwrap here.
    write(root_path.to_str().unwrap())?;
    write("\n")?;

    for file_path in diff_list.iter() {
        write(file_path)?;
        write("\n")?;
    }

    wrap_err(file.sync_all(), || {
        String::from("cannot complete writing diff file")
    })?;

    Ok(())
}

fn ensure_snapshot_dir() -> io::Result<()> {
    match fs::metadata(SNAPSHOT_DIR) {
        Ok(_) => Ok(()),
        Err(err) => {
            if err.kind() != io::ErrorKind::NotFound {
                return wrap_err2(
                    err,
                    format!(
                        "cannot get info for snapshot directory at '{}'",
                        SNAPSHOT_DIR
                    ),
                );
            }

            wrap_err(fs::create_dir(SNAPSHOT_DIR), || {
                format!("cannot create snapshot directory at '{}'", SNAPSHOT_DIR)
            })
        }
    }
}

fn wrap_err<T, F>(res: io::Result<T>, msg_func: F) -> io::Result<T>
where
    F: FnOnce() -> String,
{
    match res {
        Ok(v) => Ok(v),
        Err(err) => Err(io::Error::new(
            err.kind(),
            format!("{}: {}", msg_func(), err),
        )),
    }
}

fn wrap_err2(err: io::Error, msg: String) -> io::Result<()> {
    Err(io::Error::new(err.kind(), format!("{}: {}", msg, err)))
}
