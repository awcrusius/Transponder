import pytest

from transponder.config import ConfigError, ResolvedAuth, load_config

GOOD = """
feeds:
  - feed_id: alpha
    agency: Alpha Transit
    static_url: https://alpha.example/gtfs.zip
    realtime:
      vehicle_positions: https://alpha.example/rt/vp.pb
      service_alerts:
        url: https://alpha.example/rt/alerts.pb
        poll_interval_seconds: 120
    auth:
      type: query_param
      key: apikey
      env: ALPHA_KEY
    rt_poll_interval_seconds: 20
    static_check_interval_hours: 6
  - feed_id: beta
    agency: Beta Transit
    static_url: https://beta.example/gtfs.zip
"""


def write(tmp_path, text):
    p = tmp_path / "feeds.yaml"
    p.write_text(text)
    return p


def test_load_good_config(tmp_path, monkeypatch):
    cfg = load_config(write(tmp_path, GOOD))
    assert [f.feed_id for f in cfg.feeds] == ["alpha", "beta"]
    alpha, beta = cfg.feeds

    endpoints = dict(alpha.realtime.endpoints())
    assert set(endpoints) == {"vehicle_positions", "service_alerts"}
    assert alpha.rt_interval(endpoints["vehicle_positions"]) == 20
    assert alpha.rt_interval(endpoints["service_alerts"]) == 120

    # Defaults for beta.
    assert beta.realtime.endpoints() == []
    assert beta.auth.type == "none"
    assert beta.rt_poll_interval_seconds == 30
    assert beta.static_check_interval_hours == 24
    assert beta.auth.load_keys() == [("anonymous", ResolvedAuth())]

    monkeypatch.setenv("ALPHA_KEY", "s3cret")
    [(label, auth)] = alpha.auth.load_keys()
    assert label == "ALPHA_KEY"
    assert auth.params == {"apikey": "s3cret"}
    assert auth.headers == {}


def test_missing_secret_fails(tmp_path, monkeypatch):
    cfg = load_config(write(tmp_path, GOOD))
    monkeypatch.delenv("ALPHA_KEY", raising=False)
    with pytest.raises(ConfigError, match="ALPHA_KEY"):
        cfg.feeds[0].auth.load_keys()


def test_header_auth(tmp_path, monkeypatch):
    text = GOOD.replace("type: query_param", "type: header").replace("key: apikey", "key: X-API-Key")
    cfg = load_config(write(tmp_path, text))
    monkeypatch.setenv("ALPHA_KEY", "tok")
    assert cfg.feeds[0].auth.load_keys()[0][1].headers == {"X-API-Key": "tok"}


def test_multiple_keys_from_list_and_commas(tmp_path, monkeypatch):
    text = GOOD.replace("      env: ALPHA_KEY\n", "      env: [ALPHA_KEYS, ALPHA_SPARE]\n      daily_request_limit: 1000\n")
    cfg = load_config(write(tmp_path, text))
    monkeypatch.setenv("ALPHA_KEYS", "k1, k2,,k3")
    monkeypatch.setenv("ALPHA_SPARE", "k4")
    keys = cfg.feeds[0].auth.load_keys()
    assert [label for label, _ in keys] == ["ALPHA_KEYS[1]", "ALPHA_KEYS[2]", "ALPHA_KEYS[3]", "ALPHA_SPARE"]
    assert [a.params["apikey"] for _, a in keys] == ["k1", "k2", "k3", "k4"]
    assert cfg.feeds[0].auth.daily_request_limit == 1000

    monkeypatch.delenv("ALPHA_SPARE")
    with pytest.raises(ConfigError, match="ALPHA_SPARE"):
        cfg.feeds[0].auth.load_keys()


def test_alerting_defaults_and_overrides(tmp_path):
    cfg = load_config(write(tmp_path, GOOD))
    assert cfg.alerting.pushover is None
    assert cfg.stale_after(cfg.feeds[0]) == 600

    text = GOOD.replace("    agency: Beta Transit\n", "    agency: Beta Transit\n    stale_after_seconds: 900\n") + """
alerting:
  pushover: {}
  webhook:
    url_env: HOOK
  stale_after_seconds: 120
  min_consecutive_failures: 1
"""
    cfg = load_config(write(tmp_path, text))
    assert cfg.alerting.pushover.token_env == "PUSHOVER_TOKEN"
    assert cfg.alerting.webhook.url_env == "HOOK"
    assert cfg.alerting.min_consecutive_failures == 1
    assert cfg.stale_after(cfg.feeds[0]) == 120
    assert cfg.stale_after(cfg.feeds[1]) == 900


def test_duplicate_feed_id_rejected(tmp_path):
    with pytest.raises(ConfigError, match="duplicate feed_id"):
        load_config(write(tmp_path, GOOD.replace("feed_id: beta", "feed_id: alpha")))


def test_auth_requires_key_and_env(tmp_path):
    text = GOOD.replace("      env: ALPHA_KEY\n", "")
    with pytest.raises(ConfigError, match="auth.key and auth.env"):
        load_config(write(tmp_path, text))


def test_bad_feed_id_rejected(tmp_path):
    with pytest.raises(ConfigError):
        load_config(write(tmp_path, GOOD.replace("feed_id: beta", "feed_id: Beta Transit")))


def test_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yaml")
