use std::env;
use std::path::PathBuf;

pub fn get_data_dir() -> PathBuf {
    let mut path = PathBuf::new();

    #[cfg(target_os = "windows")]
    {
        // On Windows, get the Roaming AppData directory
        if let Ok(appdata) = env::var("APPDATA") {
            path.push(appdata);
            path.push("reth");
        } else {
            panic!("Could not find the APPDATA environment variable");
        }
    }

    #[cfg(target_os = "linux")]
    {
        // On Linux, use XDG_DATA_HOME or default to ~/.local/share/reth
        if let Ok(xdg_data_home) = env::var("XDG_DATA_HOME") {
            path.push(xdg_data_home);
        } else if let Ok(home) = env::var("HOME") {
            path.push(home);
            path.push(".local/share/reth");
        } else {
            panic!("Could not find HOME environment variable");
        }
    }

    #[cfg(target_os = "macos")]
    {
        // On macOS, use ~/Library/Application Support/reth
        if let Ok(home) = env::var("HOME") {
            path.push(home);
            path.push("Library/Application Support/reth");
        } else {
            panic!("Could not find HOME environment variable");
        }
    }

    path
}
