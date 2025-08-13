import logging
import json
import pytest
import httpx
import news_pipeline as np

class DummyResp:
    def __init__(self, json_data, status=200):
        self._json = json_data
        self.status_code = status
        self.text = json.dumps(json_data)

    def json(self):
        return self._json

class DummyClient:
    def __init__(self, responses):
        self._responses = iter(responses)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def get(self, url, params=None):
        return next(self._responses)


@pytest.mark.asyncio
async def test_fetch_juhe_caijing_continues_on_error(monkeypatch, caplog):
    np.JUHE_KEY = "x"
    monkeypatch.setattr(np, "API_BATCH_KW", 1)
    good = {
        "error_code": 0,
        "result": {"newslist": [{"ctime": "2024-01-01T00:00:00Z", "title": "t", "description": "d", "url": "u"}]},
    }
    responses = [
        DummyResp({"error_code": 123, "reason": "bad"}),
        DummyResp(good),
    ]
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: DummyClient(responses))
    caplog.set_level(logging.WARNING, logger=np.logger.name)
    src, items, err = await np.fetch_juhe_caijing(["k1", "k2"])
    assert src == "juhe_caijing"
    assert len(items) == 1
    assert err is None
    assert "juhe error response" in caplog.text


@pytest.mark.asyncio
async def test_fetch_juhe_caijing_logs_non_dict(monkeypatch, caplog):
    np.JUHE_KEY = "x"
    responses = [DummyResp(["oops"])]
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: DummyClient(responses))
    caplog.set_level(logging.WARNING, logger=np.logger.name)
    src, items, err = await np.fetch_juhe_caijing(["k1"])
    assert src == "juhe_caijing"
    assert items == []
    assert err == "0 items"
    assert "juhe non-dict response" in caplog.text
