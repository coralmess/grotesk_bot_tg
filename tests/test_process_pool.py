import asyncio
import unittest

from helpers.process_pool import recommended_process_workers, run_cpu_bound


def _multiply(a: int, b: int) -> int:
    return a * b


class ProcessPoolTests(unittest.TestCase):
    def test_recommended_workers_is_positive(self) -> None:
        self.assertGreaterEqual(recommended_process_workers(), 1)

    def test_run_cpu_bound_executes_function(self) -> None:
        result = asyncio.run(run_cpu_bound(_multiply, 6, 7))
        self.assertEqual(result, 42)


if __name__ == "__main__":
    unittest.main()
