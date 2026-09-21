use process_execution_core::{
    Config, ErrorCode, FilePathRequest, FilePrecondition, MutationDisposition,
    ProcessExecutionCore, ReadFileRequest, RemoveFileRequest, WriteFileMode, WriteFileRequest,
};

fn core(directory: &tempfile::TempDir) -> ProcessExecutionCore {
    ProcessExecutionCore::new(Config::new(directory.path())).unwrap()
}

fn path(path: &str) -> FilePathRequest {
    FilePathRequest {
        cwd: None,
        path: path.into(),
    }
}

#[tokio::test]
async fn metadata_and_binary_reads_are_bounded_and_content_addressed() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("image.bin"), [0, 1, 255, 2]).unwrap();
    let core = core(&directory);

    let metadata = core.get_file_metadata(path("image.bin")).await.unwrap();
    assert!(metadata.is_file);
    assert!(!metadata.is_directory);
    assert_eq!(metadata.size, 4);

    let read = core
        .read_file(ReadFileRequest {
            cwd: None,
            path: "image.bin".into(),
            max_bytes: Some(4),
        })
        .await
        .unwrap();
    assert_eq!(read.data, [0, 1, 255, 2]);
    assert_eq!(
        read.sha256,
        "02800ebf0a2229473506252ebffaea5f602e450772e007f29ad29fdb02d5d7e2"
    );
    assert_eq!(
        core.read_file(ReadFileRequest {
            cwd: None,
            path: "image.bin".into(),
            max_bytes: Some(3),
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::ResourceLimit
    );
    assert_eq!(
        core.get_file_metadata(path("missing"))
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
}

#[tokio::test]
async fn conditional_writes_are_atomic_replayable_and_conflict_aware() {
    let directory = tempfile::tempdir().unwrap();
    let core = core(&directory);
    let create = WriteFileRequest {
        mutation_id: "create".into(),
        cwd: None,
        path: "nested/file.txt".into(),
        data: b"first".to_vec(),
        create_parent_directories: true,
        mode: WriteFileMode::Conditional(FilePrecondition::Missing),
    };

    let first = core.write_file(create.clone()).await.unwrap();
    assert_eq!(first.disposition, MutationDisposition::Applied);
    assert_eq!(core.write_file(create.clone()).await.unwrap(), first);
    assert_eq!(
        core.write_file(WriteFileRequest {
            data: b"different".to_vec(),
            ..create.clone()
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::IdempotencyConflict
    );

    let observed = core
        .read_file(ReadFileRequest {
            cwd: None,
            path: "nested/file.txt".into(),
            max_bytes: None,
        })
        .await
        .unwrap();
    let replaced = core
        .write_file(WriteFileRequest {
            mutation_id: "replace".into(),
            cwd: None,
            path: "nested/file.txt".into(),
            data: b"second".to_vec(),
            create_parent_directories: false,
            mode: WriteFileMode::Conditional(FilePrecondition::Sha256 {
                sha256: observed.sha256.clone(),
            }),
        })
        .await
        .unwrap();
    assert_eq!(replaced.disposition, MutationDisposition::Applied);
    assert_eq!(
        std::fs::read(directory.path().join("nested/file.txt")).unwrap(),
        b"second"
    );
    assert_eq!(
        core.write_file(WriteFileRequest {
            mutation_id: "stale".into(),
            cwd: None,
            path: "nested/file.txt".into(),
            data: b"third".to_vec(),
            create_parent_directories: false,
            mode: WriteFileMode::Conditional(FilePrecondition::Sha256 {
                sha256: observed.sha256,
            }),
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::PreconditionFailed
    );

    let converged = core
        .write_file(WriteFileRequest {
            mutation_id: "recovered-after-unknown".into(),
            cwd: None,
            path: "nested/file.txt".into(),
            data: b"second".to_vec(),
            create_parent_directories: false,
            mode: WriteFileMode::Conditional(FilePrecondition::Missing),
        })
        .await
        .unwrap();
    assert_eq!(converged.disposition, MutationDisposition::AlreadyApplied);
}

#[tokio::test]
async fn overwrite_replaces_large_files_and_replays_by_mutation_id() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("large.bin");
    std::fs::write(&file, vec![1; process_execution_core::MAX_FILE_BYTES + 1]).unwrap();
    let core = core(&directory);
    let request = WriteFileRequest {
        mutation_id: "overwrite-large".into(),
        cwd: None,
        path: "large.bin".into(),
        data: b"small".to_vec(),
        create_parent_directories: false,
        mode: WriteFileMode::Overwrite,
    };
    assert_eq!(
        core.write_file(WriteFileRequest {
            mutation_id: "conditional-large".into(),
            mode: WriteFileMode::Conditional(FilePrecondition::Missing),
            ..request.clone()
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::ResourceLimit
    );
    let receipt = core.write_file(request.clone()).await.unwrap();
    assert_eq!(receipt.disposition, MutationDisposition::Applied);
    assert_eq!(std::fs::read(&file).unwrap(), b"small");
    assert_eq!(core.write_file(request.clone()).await.unwrap(), receipt);
    assert_eq!(
        core.write_file(WriteFileRequest {
            mode: WriteFileMode::Conditional(FilePrecondition::Missing),
            ..request.clone()
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::IdempotencyConflict
    );
    assert_eq!(
        core.write_file(WriteFileRequest {
            mutation_id: "same-content".into(),
            ..request
        })
        .await
        .unwrap()
        .disposition,
        MutationDisposition::AlreadyApplied
    );
    assert_eq!(
        core.write_file(WriteFileRequest {
            mutation_id: "too-large-new-content".into(),
            cwd: None,
            path: "large.bin".into(),
            data: vec![0; process_execution_core::MAX_FILE_BYTES + 1],
            create_parent_directories: false,
            mode: WriteFileMode::Overwrite,
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::ResourceLimit
    );
}

#[cfg(unix)]
#[tokio::test]
async fn overwrite_follows_symlinks_and_detaches_only_the_replaced_hard_link() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("target.txt");
    let other_link = directory.path().join("other-link.txt");
    let symbolic = directory.path().join("symbolic.txt");
    std::fs::write(&target, b"old").unwrap();
    std::fs::hard_link(&target, &other_link).unwrap();
    symlink("target.txt", &symbolic).unwrap();
    let core = core(&directory);
    let request = WriteFileRequest {
        mutation_id: "through-symlink".into(),
        cwd: None,
        path: "symbolic.txt".into(),
        data: b"new".to_vec(),
        create_parent_directories: false,
        mode: WriteFileMode::Overwrite,
    };
    assert_eq!(
        core.write_file(request).await.unwrap().disposition,
        MutationDisposition::Applied
    );
    assert!(
        std::fs::symlink_metadata(&symbolic)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"new");
    assert_eq!(std::fs::read(&other_link).unwrap(), b"old");

    symlink("missing/target.txt", directory.path().join("dangling.txt")).unwrap();
    core.write_file(WriteFileRequest {
        mutation_id: "dangling".into(),
        cwd: None,
        path: "dangling.txt".into(),
        data: b"created".to_vec(),
        create_parent_directories: true,
        mode: WriteFileMode::Overwrite,
    })
    .await
    .unwrap();
    assert_eq!(
        std::fs::read(directory.path().join("missing/target.txt")).unwrap(),
        b"created"
    );

    symlink("target.txt", directory.path().join("conditional-link.txt")).unwrap();
    let observed = core
        .read_file(ReadFileRequest {
            cwd: None,
            path: "conditional-link.txt".into(),
            max_bytes: None,
        })
        .await
        .unwrap();
    core.write_file(WriteFileRequest {
        mutation_id: "conditional-symlink".into(),
        cwd: None,
        path: "conditional-link.txt".into(),
        data: b"conditional".to_vec(),
        create_parent_directories: false,
        mode: WriteFileMode::Conditional(FilePrecondition::Sha256 {
            sha256: observed.sha256,
        }),
    })
    .await
    .unwrap();
    assert!(
        !std::fs::symlink_metadata(directory.path().join("conditional-link.txt"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"new");
}

#[tokio::test]
async fn removal_is_conditional_file_only_and_converges_after_retries() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("delete.txt"), b"remove me").unwrap();
    let core = core(&directory);
    let observed = core
        .read_file(ReadFileRequest {
            cwd: None,
            path: "delete.txt".into(),
            max_bytes: None,
        })
        .await
        .unwrap();
    let request = RemoveFileRequest {
        mutation_id: "remove".into(),
        cwd: None,
        path: "delete.txt".into(),
        precondition: FilePrecondition::Sha256 {
            sha256: observed.sha256,
        },
    };
    let removed = core.remove_file(request.clone()).await.unwrap();
    assert_eq!(removed.disposition, MutationDisposition::Applied);
    assert_eq!(core.remove_file(request).await.unwrap(), removed);
    assert_eq!(
        core.remove_file(RemoveFileRequest {
            mutation_id: "remove-after-unknown".into(),
            cwd: None,
            path: "delete.txt".into(),
            precondition: FilePrecondition::Missing,
        })
        .await
        .unwrap()
        .disposition,
        MutationDisposition::AlreadyApplied
    );

    std::fs::create_dir(directory.path().join("directory")).unwrap();
    assert_eq!(
        core.remove_file(RemoveFileRequest {
            mutation_id: "no-directory-removal".into(),
            cwd: None,
            path: "directory".into(),
            precondition: FilePrecondition::Missing,
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::InvalidArgument
    );
}
