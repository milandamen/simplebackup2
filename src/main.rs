use std::io;
use std::path::Path;
use std::process::ExitCode;

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
                match create_snapshot() {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("cannot create snapshot: {}", err);
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

struct FileEntry {
    path: String,
    modified: u64,
}

fn create_snapshot() -> io::Result<()> {
    let mut root_path: &Path;
    loop {
        let mut response = &mut String::new();
        print!("Enter the path from which you want to snapshot: ");
        if let Err(err) = io::stdin().read_line(response) {
            return Err(io::Error::new(
                err.kind(),
                format!("cannot read line from input: {}", err),
            ));
        }

        root_path = Path::new(response.trim().clone());
        let is_dir = match root_path.metadata().and_then(|md| Ok(md.is_dir())) {
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

    _ = root_path;

    Ok(())
}
