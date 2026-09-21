use super::*;

#[test]
fn planned_writes_recheck_existing_and_missing_targets() {
    let directory = tempfile::tempdir().unwrap();
    let existing = directory.path().join("existing");
    fs::write(&existing, b"planned before").unwrap();
    let planned_before = fs::read(&existing).unwrap();
    fs::write(&existing, b"intervening change").unwrap();

    let failure = commit_planned_write(
        &existing,
        b"patch output",
        Some(&planned_before),
        false,
        MAX_FILE_BYTES,
    )
    .unwrap_err();
    assert_eq!(failure.code, ErrorCode::PreconditionFailed);
    assert_eq!(fs::read(&existing).unwrap(), b"intervening change");

    let initially_missing = directory.path().join("initially-missing");
    fs::write(&initially_missing, b"intervening creation").unwrap();
    let failure = commit_planned_write(
        &initially_missing,
        b"patch output",
        None,
        true,
        MAX_FILE_BYTES,
    )
    .unwrap_err();
    assert_eq!(failure.code, ErrorCode::PreconditionFailed);
    assert_eq!(
        fs::read(&initially_missing).unwrap(),
        b"intervening creation"
    );
}
