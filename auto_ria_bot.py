import asyncio
import logging

from dotenv import load_dotenv

from helpers.auto_ria.runtime import build_auto_ria_runtime
from helpers.logging_utils import configure_third_party_loggers, install_secret_redaction


async def _main() -> None:
    runtime = build_auto_ria_runtime(logger=logging.getLogger("auto_ria_bot"))
    await runtime.start()
    try:
        await runtime.run_forever()
    finally:
        await runtime.shutdown()


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    configure_third_party_loggers()
    install_secret_redaction(logging.getLogger())
    asyncio.run(_main())


if __name__ == "__main__":
    main()
