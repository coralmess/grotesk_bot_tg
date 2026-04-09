import asyncio

from GroteskBotTg import IS_RUNNING_LYST, create_tables, main
from helpers.service_health import build_service_health


SERVICE_HEALTH = build_service_health("grotesk-lyst")


if __name__ == "__main__":
    if IS_RUNNING_LYST:
        create_tables()
    asyncio.run(main(service_health=SERVICE_HEALTH))
