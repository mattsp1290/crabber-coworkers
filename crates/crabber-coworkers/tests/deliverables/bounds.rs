use super::*;

#[tokio::test]
async fn operation_bounds_and_invalid_anchors_preserve_existing_bytes() {
    let disk = tempfile::tempdir().unwrap();
    let def = definition(WorkspaceAccess::ReadWrite);
    turn(
        &def,
        disk.path(),
        tool_provider(
            "file_write",
            &json!({"path":"draft.txt","content":"same same"}),
        ),
        None,
    )
    .await
    .unwrap();
    for (name, args) in [
        (
            "file_write",
            json!({"path":"draft.txt","content":"x".repeat(262_145)}),
        ),
        (
            "file_edit",
            json!({"path":"draft.txt","anchor":"same","replacement":"changed"}),
        ),
        (
            "file_edit",
            json!({"path":"draft.txt","anchor":"absent","replacement":"changed"}),
        ),
    ] {
        let provider = tool_provider(name, &args);
        turn(&def, disk.path(), provider.clone(), None)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(disk.path().join("workspaces/scout/draft.txt")).unwrap(),
            "same same"
        );
        let requests = provider.requests();
        assert_eq!(
            common::tool_results(requests.last().unwrap())[0]["outcome"],
            "failed"
        );
    }
    let nested = disk.path().join("workspaces/scout/nested/draft.txt");
    let provider = tool_provider(
        "file_write",
        &json!({"path":"nested/draft.txt","content":"nested"}),
    );
    turn(&def, disk.path(), provider.clone(), None)
        .await
        .unwrap();
    assert!(!nested.exists());
    assert_eq!(
        common::tool_results(provider.requests().last().unwrap())[0]["outcome"],
        "failed"
    );
    turn(
        &def,
        disk.path(),
        tool_provider(
            "file_write",
            &json!({"path":"nested/draft.txt","content":"nested","create_dirs":true}),
        ),
        None,
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read_to_string(nested).unwrap(), "nested");
}

#[tokio::test]
async fn large_reads_and_listings_report_truncation() {
    let disk = tempfile::tempdir().unwrap();
    let def = definition(WorkspaceAccess::ReadWrite);
    turn(
        &def,
        disk.path(),
        tool_provider("file_write", &json!({"path":"large.txt","content":"small"})),
        None,
    )
    .await
    .unwrap();
    let root = disk.path().join("workspaces/scout");
    std::fs::write(root.join("large.txt"), "x".repeat(262_145)).unwrap();
    std::fs::create_dir(root.join("many")).unwrap();
    for n in 0..5001 {
        std::fs::write(root.join(format!("many/{n:05}")), "").unwrap();
    }
    for (name, arguments) in [
        ("file_read", json!({"path":"large.txt"})),
        ("file_list", json!({"path":"many"})),
    ] {
        let provider = tool_provider(name, &arguments);
        turn(&def, disk.path(), provider.clone(), None)
            .await
            .unwrap();
        let requests = provider.requests();
        let results = common::tool_results(requests.last().unwrap());
        assert_eq!(results[0]["truncated"], true);
    }
}

#[tokio::test]
async fn concurrent_replacements_are_whole_versions() {
    let disk = tempfile::tempdir().unwrap();
    let def = definition(WorkspaceAccess::ReadWrite);
    let versions: Vec<_> = (b'a'..=b'h')
        .map(|byte| char::from(byte).to_string().repeat(32_768))
        .collect();
    turn(
        &def,
        disk.path(),
        tool_provider(
            "file_write",
            &json!({"path":"shared.txt","content":versions[0]}),
        ),
        None,
    )
    .await
    .unwrap();
    let file = disk.path().join("workspaces/scout/shared.txt");
    let mut tasks = tokio::task::JoinSet::new();
    for content in &versions {
        let def = def.clone();
        let state = disk.path().to_owned();
        let provider = tool_provider(
            "file_write",
            &json!({"path":"shared.txt","content":content}),
        );
        tasks.spawn(async move { turn(&def, &state, provider, None).await.unwrap() });
    }
    while !tasks.is_empty() {
        assert!(versions.contains(&std::fs::read_to_string(&file).unwrap()));
        tokio::select! {
            result = tasks.join_next() => { result.unwrap().unwrap(); },
            () = tokio::task::yield_now() => {},
        }
    }
    assert!(versions.contains(&std::fs::read_to_string(file).unwrap()));
}
