import unittest

from second_brain_bot.youtube import clean_transcript_text, extract_youtube_video_id, transcript_segments_to_text


class SecondBrainYouTubeTests(unittest.TestCase):
    def test_extract_youtube_video_id_from_common_urls(self) -> None:
        self.assertEqual(extract_youtube_video_id("https://youtu.be/dQw4w9WgXcQ"), "dQw4w9WgXcQ")
        self.assertEqual(
            extract_youtube_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=30s"),
            "dQw4w9WgXcQ",
        )
        self.assertEqual(extract_youtube_video_id("https://www.youtube.com/shorts/dQw4w9WgXcQ"), "dQw4w9WgXcQ")
        self.assertEqual(extract_youtube_video_id("https://example.com/watch?v=dQw4w9WgXcQ"), "")

    def test_clean_transcript_text_removes_timecodes_and_normalizes_space(self) -> None:
        raw = """
        00:00:01 Intro
        [00:00:04] This is the first idea.
        00:01:05.120 --> 00:01:07.000
        Second idea continues
        """

        cleaned = clean_transcript_text(raw)

        self.assertEqual(cleaned, "Intro This is the first idea.\n\nSecond idea continues")
        self.assertNotIn("00:00", cleaned)
        self.assertNotIn("-->", cleaned)

    def test_transcript_segments_to_text_accepts_dicts_and_objects(self) -> None:
        item = type("Snippet", (), {"text": "Second line"})()

        text = transcript_segments_to_text([{"text": "First line", "start": 0.0}, item])

        self.assertEqual(text, "First line\nSecond line")


if __name__ == "__main__":
    unittest.main()
