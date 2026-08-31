import main


def test_client_bearer_auth_is_forwarded_unchanged(monkeypatch):
    monkeypatch.setattr(main, "API_KEY", "server-key")
    assert main._get_upstream_auth_header({"Authorization": "Bearer client-key"}) == "Bearer client-key"


def test_client_bare_auth_gets_bearer_prefix(monkeypatch):
    monkeypatch.setattr(main, "API_KEY", "server-key")
    assert main._get_upstream_auth_header({"Authorization": "client-key"}) == "Bearer client-key"


def test_client_auth_takes_precedence_over_configured_key(monkeypatch):
    monkeypatch.setattr(main, "API_KEY", "server-key")
    assert main._get_upstream_auth_header({"authorization": "Bearer client-key"}) == "Bearer client-key"


def test_configured_key_is_used_when_client_auth_is_missing(monkeypatch):
    monkeypatch.setattr(main, "API_KEY", "server-key")
    assert main._get_upstream_auth_header({}) == "Bearer server-key"


def test_missing_client_and_configured_auth_stays_missing(monkeypatch):
    monkeypatch.setattr(main, "API_KEY", "")
    assert main._get_upstream_auth_header({}) is None
