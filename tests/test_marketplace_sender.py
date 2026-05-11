import unittest

from helpers.marketplace_core import DeliveryResult
from helpers.marketplace_sender import RetryableHttpStatus
from helpers.image_pipeline import send_remote_photo_with_fallback


class MarketplaceSenderTests(unittest.IsolatedAsyncioTestCase):
    async def test_image_404_returns_retryable_failure_without_text_fallback(self) -> None:
        calls = {"message": 0, "photo": 0}

        async def _download(_url):
            raise RetryableHttpStatus(404, context="image download")

        async def _send_message(*_args):
            calls["message"] += 1
            return DeliveryResult(delivered=True, telegram_message_id=10, channel="text")

        async def _send_photo(*_args):
            calls["photo"] += 1
            return DeliveryResult(delivered=True, telegram_message_id=11, channel="photo")

        async def _run_cpu_bound(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        result = await send_remote_photo_with_fallback(
            bot=object(),
            chat_id="1",
            caption="caption",
            image_url="https://image-thumbs.shafastatic.net/missing",
            is_valid_image_url=lambda url: True,
            download_bytes=_download,
            send_message=_send_message,
            send_photo_by_bytes=_send_photo,
            run_cpu_bound_fn=_run_cpu_bound,
            logger=None,
        )

        self.assertFalse(result.delivered)
        self.assertEqual(result.failure_reason, "image_download_404")
        self.assertTrue(result.retry_later)
        self.assertEqual(calls, {"message": 0, "photo": 0})
