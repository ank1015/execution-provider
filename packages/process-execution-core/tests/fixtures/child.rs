use std::{io::{self, BufRead, Read, Write}, time::Duration};

fn main() {
    let args: Vec<_> = std::env::args().collect();
    match args[1].as_str() {
        "echo" => { println!("{}", args[2]); eprintln!("diagnostic"); }
        "exit" => std::process::exit(args[2].parse().unwrap()),
        "bytes" => { io::stdout().write_all(&(0..args[2].parse().unwrap()).map(|i| (i % 256) as u8).collect::<Vec<_>>()).unwrap(); }
        "sleep" => { println!("ready"); io::stdout().flush().unwrap(); std::thread::sleep(Duration::from_secs(60)); }
        "copy" => { let mut data = Vec::new(); io::stdin().read_to_end(&mut data).unwrap(); io::stdout().write_all(&data).unwrap(); }
        "interactive" => {
            println!("ready"); io::stdout().flush().unwrap();
            for line in io::stdin().lock().lines() {
                let line = line.unwrap();
                println!("received:{line}"); io::stdout().flush().unwrap();
                if line == "quit" { break; }
            }
        }
        "delayed" => {
            println!("first"); io::stdout().flush().unwrap();
            std::thread::sleep(Duration::from_millis(180));
            println!("last");
        }
        "context" => {
            println!("cwd={}", std::env::current_dir().unwrap().display());
            println!("env={}", std::env::var("PROCESS_CORE_TEST").unwrap());
            for arg in &args[2..] { println!("arg={arg}"); }
        }
        "descendant" | "orphan" | "escaped" => {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command.arg("sleep");
            #[cfg(unix)]
            if args[1] == "escaped" {
                use std::os::unix::process::CommandExt;
                command.process_group(0);
            }
            let mut child = command.spawn().unwrap();
            println!("child={}", child.id()); io::stdout().flush().unwrap();
            if args[1] == "descendant" { let _ = child.wait(); }
        }
        _ => panic!("unknown fixture mode"),
    }
}
