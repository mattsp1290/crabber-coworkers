use super::*;

#[tokio::test]
async fn coworker_and_state_files_cannot_be_reached_by_escape_or_symlink() {
    let disk = tempfile::tempdir().unwrap();
    let a = definition(WorkspaceAccess::ReadWrite);
    let mut b = a.clone();
    b.id = "quill".into();
    turn(
        &a,
        disk.path(),
        tool_provider(
            "file_write",
            &json!({"path":"draft.txt","content":"outside-secret-marker"}),
        ),
        None,
    )
    .await
    .unwrap();
    turn(
        &b,
        disk.path(),
        tool_provider("file_write", &json!({"path":"own.txt","content":"own"})),
        None,
    )
    .await
    .unwrap();
    let a_file = disk.path().join("workspaces/scout/draft.txt");
    let b_root = disk.path().join("workspaces/quill");
    let state = disk.path().join("scout.sqlite");
    std::fs::write(&state, "state-secret-marker").unwrap();
    std::os::unix::fs::symlink(a_file.parent().unwrap(), b_root.join("other")).unwrap();
    std::os::unix::fs::symlink(&state, b_root.join("state-link")).unwrap();
    let paths = [
        a_file.to_str().unwrap(),
        "../scout/draft.txt",
        "../scout.sqlite",
        "../../scout.sqlite",
        "other/draft.txt",
        "state-link",
    ];
    for path in paths {
        for (name, arguments) in [
            ("file_read", json!({"path":path})),
            ("file_write", json!({"path":path,"content":"changed"})),
            (
                "file_edit",
                json!({"path":path,"anchor":"secret","replacement":"changed"}),
            ),
        ] {
            let provider = tool_provider(name, &arguments);
            turn(&b, disk.path(), provider.clone(), None).await.unwrap();
            let requests = provider.requests();
            let results = common::tool_results(requests.last().unwrap());
            assert_eq!(results.len(), 1);
            assert_eq!(results[0]["outcome"], "failed");
            assert!(!results[0].to_string().contains("outside-secret-marker"));
            assert!(!results[0].to_string().contains("state-secret-marker"));
            assert_eq!(
                std::fs::read_to_string(&a_file).unwrap(),
                "outside-secret-marker"
            );
            assert_eq!(
                std::fs::read_to_string(&state).unwrap(),
                "state-secret-marker"
            );
        }
    }
    for path in [
        "../scout",
        "other",
        a_file.parent().unwrap().to_str().unwrap(),
    ] {
        let provider = tool_provider("file_list", &json!({"path":path}));
        turn(&b, disk.path(), provider.clone(), None).await.unwrap();
        let requests = provider.requests();
        let results = common::tool_results(requests.last().unwrap());
        assert_eq!(results[0]["outcome"], "failed");
        assert!(!results[0].to_string().contains("draft.txt"));
    }
}

#[tokio::test]
async fn readonly_can_retrieve_existing_files_without_mutation() {
    let disk = tempfile::tempdir().unwrap();
    let mut def = definition(WorkspaceAccess::ReadWrite);
    turn(
        &def,
        disk.path(),
        tool_provider(
            "file_write",
            &json!({"path":"draft.txt","content":"retained"}),
        ),
        Some("grant"),
    )
    .await
    .unwrap();
    def.workspace = WorkspaceAccess::ReadOnly;
    let provider = tool_provider("file_read", &json!({"path":"draft.txt"}));
    turn(&def, disk.path(), provider.clone(), Some("grant"))
        .await
        .unwrap();
    let requests = provider.requests();
    let results = common::tool_results(requests.last().unwrap());
    assert_eq!(results[0]["outcome"], "succeeded");
    assert!(results.last().unwrap().to_string().contains("retained"));
    assert_eq!(
        std::fs::read_to_string(disk.path().join("workspaces/scout/draft.txt")).unwrap(),
        "retained"
    );
}
