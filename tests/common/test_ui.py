from ftx_paper.ui.app import create_ui_app


def test_orderflow_replay_page_has_structured_report_mount() -> None:
    client = create_ui_app().test_client()

    response = client.get("/replay?strategy_id=ftx-paper-orderflow")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Location attribution" in body
    assert "Entries filled" in body
    assert "View report" in body
    assert "Remove" in body
