import unittest


from helpers.lyst.http_client import AsyncLystHttpClient


class _FakeResponse:
    def __init__(self, *, status=200, text="", url="https://www.lyst.com/test") -> None:
        self.status = status
        self._text = text
        self.url = url

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self._text


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.requests = []
        self.closed = False

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        if not self._responses:
            raise AssertionError("No fake responses left")
        return self._responses.pop(0)

    async def close(self):
        self.closed = True


class AsyncLystHttpClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_text_sets_country_cookie_and_returns_body(self) -> None:
        session = _FakeSession([_FakeResponse(text="<html>ok</html>")])
        client = AsyncLystHttpClient(
            timeout_sec=5,
            user_agent="ua",
            default_headers={"Accept-Language": "en-US"},
            session_factory=lambda **_kwargs: session,
        )

        result = await client.fetch_text("https://www.lyst.com/shop", "PL")

        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.text, "<html>ok</html>")
        self.assertEqual(session.requests[0][1]["cookies"]["country"], "PL")
        await client.close()
        self.assertTrue(session.closed)

    async def test_warm_home_fetches_homepage_before_target(self) -> None:
        session = _FakeSession(
            [
                _FakeResponse(text="<html>home</html>", url="https://www.lyst.com/"),
                _FakeResponse(text="<html>target</html>", url="https://www.lyst.com/shop"),
            ]
        )
        client = AsyncLystHttpClient(
            timeout_sec=5,
            user_agent="ua",
            default_headers={"Accept-Language": "en-US"},
            session_factory=lambda **_kwargs: session,
        )

        result = await client.fetch_text("https://www.lyst.com/shop?x=1", "US", warm_home=True)

        self.assertEqual(result.text, "<html>target</html>")
        self.assertEqual(session.requests[0][0], "https://www.lyst.com/")
        self.assertEqual(session.requests[1][0], "https://www.lyst.com/shop?x=1")
        self.assertEqual(
            session.requests[1][1]["headers"]["Referer"],
            "https://www.lyst.com/shop?x=1",
        )


if __name__ == "__main__":
    unittest.main()
