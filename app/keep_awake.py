import asyncio
import logging

import httpx

from app.config import settings

log = logging.getLogger("keep_awake")


class KeepAwake:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if not (settings.keep_awake and settings.render_external_url):
            return
        self._task = asyncio.create_task(self._run(), name="keep-awake")
        log.info(
            "pinging %s every %.0fs to prevent idle spin-down",
            settings.render_external_url,
            settings.keep_awake_interval_seconds,
        )

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run(self) -> None:
        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                await asyncio.sleep(settings.keep_awake_interval_seconds)
                try:
                    await client.head(settings.render_external_url)
                except httpx.HTTPError as exc:
                    log.warning("keep-awake ping failed: %s", exc)


keep_awake = KeepAwake()
