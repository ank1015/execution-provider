use process_execution_core::{
    ApplyPatchRequest, Config, ErrorCode, MAX_FILE_BYTES, PatchInput, PatchStatus,
    ProcessExecutionCore, ReplacementFile, TextEdit, WriteFileMode, WriteFileRequest,
};

fn runtime(dir: &tempfile::TempDir) -> ProcessExecutionCore {
    ProcessExecutionCore::new(Config::new(dir.path())).unwrap()
}
fn codex(id: &str, text: &str) -> ApplyPatchRequest {
    ApplyPatchRequest {
        mutation_id: id.into(),
        cwd: None,
        patch: PatchInput::Codex { text: text.into() },
    }
}
fn replace(id: &str, path: &str, edits: &[(&str, &str)]) -> ApplyPatchRequest {
    ApplyPatchRequest {
        mutation_id: id.into(),
        cwd: None,
        patch: PatchInput::TextReplacements {
            files: vec![ReplacementFile {
                path: path.into(),
                edits: edits
                    .iter()
                    .map(|(old, new)| TextEdit {
                        old_text: (*old).into(),
                        new_text: (*new).into(),
                    })
                    .collect(),
            }],
        },
    }
}

#[tokio::test]
async fn codex_multi_file_sections_repeated_paths_moves_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let run = runtime(&dir);
    std::fs::write(dir.path().join("existing"), "old\n").unwrap();
    std::fs::write(dir.path().join("destination"), "former\n").unwrap();
    let request = codex(
        "multi",
        "*** Begin Patch\n*** Add File: nested/a\n+hello\n*** Update File: nested/a\n@@\n-hello\n+HELLO\n*** Add File: existing\n+new\n*** Update File: existing\n*** Move to: destination\n@@\n-new\n+done\n*** Delete File: nested/a\n*** End Patch",
    );
    let first = run.apply_patch(request.clone()).await.unwrap();
    assert_eq!(first.status, PatchStatus::Applied);
    assert_eq!(first.changes.len(), 5);
    assert_eq!(
        std::fs::read(dir.path().join("destination")).unwrap(),
        b"done\n"
    );
    assert!(!dir.path().join("existing").exists());
    assert!(!dir.path().join("nested/a").exists());
    run.write_file(WriteFileRequest {
        mutation_id: "later".into(),
        cwd: None,
        path: "destination".into(),
        data: b"later".to_vec(),
        create_parent_directories: false,
        mode: WriteFileMode::Overwrite,
    })
    .await
    .unwrap();
    assert_eq!(run.apply_patch(request).await.unwrap(), first);
    assert_eq!(
        std::fs::read(dir.path().join("destination")).unwrap(),
        b"later"
    );
}

#[tokio::test]
async fn codex_hunks_anchors_eof_insertions_and_preflight() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("file"), "intro\nanchor\nvalue  \nend\n").unwrap();
    let run = runtime(&dir);
    let patch = codex(
        "hunks",
        "*** Begin Patch\n*** Update File: file\n@@ anchor\n-value\n+new\n@@\n-end\n+END\n*** End of File\n*** End Patch",
    );
    let applied = run.apply_patch(patch).await.unwrap();
    assert_eq!(applied.status, PatchStatus::Applied);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("file")).unwrap(),
        "intro\nanchor\nnew\nEND\n"
    );
    let bad = codex(
        "bad",
        "*** Begin Patch\n*** Add File: other\n+x\n*** Update File: file\n@@ missing\n-a\n+b\n*** End Patch",
    );
    let rejected = run.apply_patch(bad.clone()).await.unwrap();
    assert_eq!(rejected.status, PatchStatus::Rejected);
    assert!(!dir.path().join("other").exists());
    assert_eq!(run.apply_patch(bad).await.unwrap(), rejected);
    let insert = codex(
        "insert",
        "*** Begin Patch\n*** Update File: file\n@@ anchor\n+inserted\n*** End Patch",
    );
    assert_eq!(
        run.apply_patch(insert).await.unwrap().status,
        PatchStatus::Applied
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("file")).unwrap(),
        "intro\nanchor\nnew\nEND\ninserted\n"
    );
}

#[tokio::test]
async fn codex_eof_overlap_is_a_retained_rejection() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file");
    std::fs::write(&file, "first\nsecond\n").unwrap();
    let run = runtime(&dir);
    let request = codex(
        "overlapping-eof",
        "*** Begin Patch\n*** Update File: file\n@@\n-second\n@@\n-second\n*** End of File\n*** End Patch",
    );
    let first = run.apply_patch(request.clone()).await.unwrap();
    assert_eq!(first.status, PatchStatus::Rejected);
    assert_eq!(
        first.error.as_ref().unwrap().code,
        ErrorCode::InvalidArgument
    );
    assert_eq!(first.error.as_ref().unwrap().section, Some(0));
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "first\nsecond\n");
    std::fs::write(&file, "changed after rejection\n").unwrap();
    assert_eq!(run.apply_patch(request).await.unwrap(), first);
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "changed after rejection\n"
    );
}

#[tokio::test]
async fn codex_retries_a_trailing_empty_context_line() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("file"), "last\n").unwrap();
    let run = runtime(&dir);
    let patch = codex(
        "trailing-empty-context",
        "*** Begin Patch\n*** Update File: file\n@@\n-last\n+updated\n \n*** End Patch",
    );
    let result = run.apply_patch(patch).await.unwrap();
    assert_eq!(result.status, PatchStatus::Applied);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("file")).unwrap(),
        "updated\n"
    );
}

#[tokio::test]
async fn codex_pure_insertion_and_later_hunk_keep_original_offsets() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("file"), "one\ntwo\nthree\n").unwrap();
    let run = runtime(&dir);
    let patch = codex(
        "insertion-order",
        "*** Begin Patch\n*** Update File: file\n@@\n+tail\n@@\n-two\n+TWO\n+extra\n*** End Patch",
    );
    assert_eq!(
        run.apply_patch(patch).await.unwrap().status,
        PatchStatus::Applied
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("file")).unwrap(),
        "one\nTWO\nextra\nthree\ntail\n"
    );
}

#[tokio::test]
async fn replacements_are_original_content_unique_nonoverlapping_and_bom_aware() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("file"),
        "\u{feff}first=old\r\nsecond=old\r\n",
    )
    .unwrap();
    let run = runtime(&dir);
    let request = replace(
        "replace",
        "file",
        &[("second=old", "second=new"), ("first=old", "first=")],
    );
    let applied = run.apply_patch(request).await.unwrap();
    assert_eq!(applied.status, PatchStatus::Applied);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("file")).unwrap(),
        "\u{feff}first=\r\nsecond=new\r\n"
    );
    for (id, edits) in [
        ("missing", vec![("absent", "x")]),
        ("empty", vec![("", "x")]),
        ("overlap", vec![("first=", "x"), ("first", "y")]),
        ("nochange", vec![("second=new", "second=new")]),
    ] {
        assert_eq!(
            run.apply_patch(replace(id, "file", &edits))
                .await
                .unwrap()
                .status,
            PatchStatus::Rejected
        );
    }
    std::fs::write(dir.path().join("repeated"), "foo foo").unwrap();
    assert_eq!(
        run.apply_patch(replace("ambiguous", "repeated", &[("foo", "bar")]))
            .await
            .unwrap()
            .status,
        PatchStatus::Rejected
    );
    assert_eq!(
        run.apply_patch(replace(
            "original",
            "repeated",
            &[("foo foo", "bar"), ("bar", "baz")]
        ))
        .await
        .unwrap()
        .status,
        PatchStatus::Rejected
    );
}

#[tokio::test]
async fn limits_utf8_and_conflicting_mutation_ids() {
    let dir = tempfile::tempdir().unwrap();
    let run = runtime(&dir);
    let mut exact = vec![b'a'; MAX_FILE_BYTES];
    exact[..6].copy_from_slice(b"unique");
    std::fs::write(dir.path().join("boundary"), exact).unwrap();
    assert_eq!(
        run.apply_patch(replace("boundary", "boundary", &[("unique", "UNIQUE")]))
            .await
            .unwrap()
            .status,
        PatchStatus::Applied
    );
    assert_eq!(
        std::fs::metadata(dir.path().join("boundary"))
            .unwrap()
            .len(),
        MAX_FILE_BYTES as u64
    );
    std::fs::write(dir.path().join("large"), vec![b'a'; MAX_FILE_BYTES + 1]).unwrap();
    let large = run
        .apply_patch(replace("oversize", "large", &[("a", "b")]))
        .await
        .unwrap();
    assert_eq!(large.status, PatchStatus::Rejected);
    assert_eq!(large.error.unwrap().code, ErrorCode::ResourceLimit);
    std::fs::write(dir.path().join("binary"), [255]).unwrap();
    assert_eq!(
        run.apply_patch(replace("binary", "binary", &[("x", "y")]))
            .await
            .unwrap()
            .status,
        PatchStatus::Rejected
    );
    let first = replace("same", "binary", &[("x", "y")]);
    run.apply_patch(first).await.unwrap();
    assert_eq!(
        run.apply_patch(replace("same", "binary", &[("z", "y")]))
            .await
            .unwrap_err()
            .code,
        ErrorCode::IdempotencyConflict
    );
}

#[tokio::test]
async fn multiline_empty_replacement_and_capacity_reservation() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("file"), "left\nmiddle\nright").unwrap();
    let mut config = Config::new(dir.path());
    config.limits.max_file_mutation_receipts = 1;
    let run = ProcessExecutionCore::new(config).unwrap();
    let request = replace("one", "file", &[("middle\n", "")]);
    let applied = run.apply_patch(request.clone()).await.unwrap();
    assert_eq!(applied.status, PatchStatus::Applied);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("file")).unwrap(),
        "left\nright"
    );
    assert_eq!(run.apply_patch(request).await.unwrap(), applied);
    assert_eq!(
        run.apply_patch(replace("two", "file", &[("left", "LEFT")]))
            .await
            .unwrap_err()
            .code,
        ErrorCode::ResourceLimit
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("file")).unwrap(),
        "left\nright"
    );
}

#[tokio::test]
async fn codex_punctuation_matching_and_replacement_duplicate_paths() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("file"), "a\u{2014}b\n").unwrap();
    let run = runtime(&dir);
    let receipt = run
        .apply_patch(codex(
            "punct",
            "*** Begin Patch\n*** Update File: file\n@@\n-a-b\n+changed\n*** End Patch",
        ))
        .await
        .unwrap();
    assert_eq!(receipt.status, PatchStatus::Applied);
    let duplicate = ApplyPatchRequest {
        mutation_id: "duplicate".into(),
        cwd: None,
        patch: PatchInput::TextReplacements {
            files: vec![
                ReplacementFile {
                    path: "file".into(),
                    edits: vec![TextEdit {
                        old_text: "changed".into(),
                        new_text: "first".into(),
                    }],
                },
                ReplacementFile {
                    path: "file".into(),
                    edits: vec![TextEdit {
                        old_text: "first".into(),
                        new_text: "second".into(),
                    }],
                },
            ],
        },
    };
    assert_eq!(
        run.apply_patch(duplicate).await.unwrap().status,
        PatchStatus::Rejected
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("file")).unwrap(),
        "changed\n"
    );
}

#[tokio::test]
async fn codex_reference_scenarios_multiple_chunks_pure_addition_and_eof() {
    // From codex-rs/apply-patch/tests/fixtures/scenarios 003, 016, and 022
    // (Apache-2.0, Copyright 2025 OpenAI).
    let dir = tempfile::tempdir().unwrap();
    let run = runtime(&dir);
    for (id, path, input, patch, expected) in [
        (
            "003",
            "multi.txt",
            "line1\nline2\nline3\nline4\n",
            "*** Begin Patch\n*** Update File: multi.txt\n@@\n-line2\n+changed2\n@@\n-line4\n+changed4\n*** End Patch",
            "line1\nchanged2\nline3\nchanged4\n",
        ),
        (
            "016",
            "input.txt",
            "line 1\nline 2\n",
            "*** Begin Patch\n*** Update File: input.txt\n@@\n+added line 1\n+added line 2\n*** End Patch",
            "line 1\nline 2\nadded line 1\nadded line 2\n",
        ),
        (
            "022",
            "tail.txt",
            "first\nsecond\n",
            "*** Begin Patch\n*** Update File: tail.txt\n@@\n first\n-second\n+second updated\n*** End of File\n*** End Patch",
            "first\nsecond updated\n",
        ),
    ] {
        std::fs::write(dir.path().join(path), input).unwrap();
        assert_eq!(
            run.apply_patch(codex(id, patch)).await.unwrap().status,
            PatchStatus::Applied
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join(path)).unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn codex_mixed_endings_missing_final_newline_and_routing_rejection() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("mixed"), b"a\r\nb\nlast").unwrap();
    let run = runtime(&dir);
    let result = run
        .apply_patch(codex(
            "mixed",
            "*** Begin Patch\n*** Update File: mixed\n@@\n-b\n+B\n*** End Patch",
        ))
        .await
        .unwrap();
    assert_eq!(result.status, PatchStatus::Applied);
    assert_eq!(
        std::fs::read(dir.path().join("mixed")).unwrap(),
        b"a\r\nB\nlast\n"
    );
    let result = run.apply_patch(codex("routing", "*** Begin Patch\n*** Environment ID: another-machine\n*** Add File: bad\n+x\n*** End Patch")).await.unwrap();
    assert_eq!(result.status, PatchStatus::Rejected);
    assert_eq!(result.error.unwrap().code, ErrorCode::UnsupportedOperation);
    assert!(!dir.path().join("bad").exists());
}

#[tokio::test]
async fn concurrent_duplicates_and_bounded_diff() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("file"), "old\n").unwrap();
    let run = runtime(&dir);
    let request = replace("concurrent", "file", &[("old", "new")]);
    let (a, b) = tokio::join!(run.apply_patch(request.clone()), run.apply_patch(request));
    assert_eq!(a.unwrap(), b.unwrap());
    std::fs::write(dir.path().join("diff"), "x").unwrap();
    let long = "z".repeat(100_000);
    let receipt = run
        .apply_patch(replace("diff", "diff", &[("x", &long)]))
        .await
        .unwrap();
    assert_eq!(receipt.status, PatchStatus::Applied);
    assert!(receipt.diff_truncated);
    assert!(receipt.diff.len() <= 64 * 1024);
    assert!(
        serde_json::to_vec(&receipt).unwrap().len()
            <= process_execution_core::MAX_PATCH_RESULT_BYTES
    );
}

#[cfg(unix)]
#[tokio::test]
async fn replacement_follows_symlink_and_rejects_aliases() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("real"), "one").unwrap();
    symlink("real", dir.path().join("alias")).unwrap();
    let run = runtime(&dir);
    assert_eq!(
        run.apply_patch(replace("symlink", "alias", &[("one", "two")]))
            .await
            .unwrap()
            .status,
        PatchStatus::Applied
    );
    assert!(
        std::fs::symlink_metadata(dir.path().join("alias"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("real")).unwrap(),
        "two"
    );
    assert_eq!(
        run.apply_patch(codex(
            "delete-link",
            "*** Begin Patch\n*** Delete File: alias\n*** End Patch"
        ))
        .await
        .unwrap()
        .status,
        PatchStatus::Rejected
    );
    let conflict = ApplyPatchRequest {
        mutation_id: "alias-conflict".into(),
        cwd: None,
        patch: PatchInput::TextReplacements {
            files: vec![
                ReplacementFile {
                    path: "real".into(),
                    edits: vec![TextEdit {
                        old_text: "two".into(),
                        new_text: "three".into(),
                    }],
                },
                ReplacementFile {
                    path: "alias".into(),
                    edits: vec![TextEdit {
                        old_text: "three".into(),
                        new_text: "four".into(),
                    }],
                },
            ],
        },
    };
    assert_eq!(
        run.apply_patch(conflict).await.unwrap().status,
        PatchStatus::Rejected
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("real")).unwrap(),
        "two"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn move_destination_write_then_source_delete_failure_is_partial() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("locked")).unwrap();
    std::fs::write(dir.path().join("locked/source"), "before\n").unwrap();
    std::fs::write(dir.path().join("destination"), "old destination\n").unwrap();
    let run = runtime(&dir);
    let request = codex(
        "partial",
        "*** Begin Patch\n*** Update File: locked/source\n*** Move to: destination\n@@\n-before\n+after\n*** End Patch",
    );
    std::fs::set_permissions(
        dir.path().join("locked"),
        std::fs::Permissions::from_mode(0o555),
    )
    .unwrap();
    let outcome = run.apply_patch(request.clone()).await.unwrap();
    std::fs::set_permissions(
        dir.path().join("locked"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    // Privileged test runners can bypass directory write permissions.
    if outcome.status == PatchStatus::Applied {
        return;
    }
    assert_eq!(outcome.status, PatchStatus::Partial);
    assert!(!outcome.changes_exact);
    assert_eq!(outcome.changes.len(), 1);
    assert_eq!(
        outcome.changes[0].kind,
        process_execution_core::PatchChangeKind::Update
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("destination")).unwrap(),
        "after\n"
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("locked/source")).unwrap(),
        "before\n"
    );
    assert_eq!(run.apply_patch(request).await.unwrap(), outcome);
}

#[cfg(unix)]
#[tokio::test]
async fn content_replacement_preserves_file_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mode");
    std::fs::write(&path, "old").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    let run = runtime(&dir);
    assert_eq!(
        run.apply_patch(replace("mode", "mode", &[("old", "new")]))
            .await
            .unwrap()
            .status,
        PatchStatus::Applied
    );
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o640
    );
}
