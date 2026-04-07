import asyncio

from GroteskBotTg import IS_RUNNING_LYST, create_tables, main


if __name__ == "__main__":
    if IS_RUNNING_LYST:
        create_tables()
    asyncio.run(main())
