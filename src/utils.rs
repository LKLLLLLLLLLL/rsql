use std::path::PathBuf;
use std::sync::OnceLock;

static TEST_RUN_ID: OnceLock<u64> = OnceLock::new();

pub(crate) fn test_dir(annot: String) -> PathBuf {
    let run_id = TEST_RUN_ID.get_or_init(|| {
        rand::random::<u64>()
    });

    let thread = std::thread::current();
    let thread_name = thread.name().unwrap_or("unknown");
    let safe_thread_name = thread_name.replace("::", "_");

    // temp/rsql_test/run_{id}/{test_function}/{annot}
    let path = std::env::temp_dir()
        .join("rsql_test")
        .join(format!("run_{}", run_id))
        .join(safe_thread_name)
        .join(annot);
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            eprintln!("Failed to create test directory {:?}: {}", parent, e);
        }
    }
    path
}
