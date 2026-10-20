import asyncio
import sys
import time
import uuid

import httpx

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000").rstrip("/")


def body(txn_id: str) -> dict:
    return {
        "transaction_id": txn_id,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }


async def main() -> None:
    async with httpx.AsyncClient(base_url=BASE, timeout=30) as c:
        r = await c.get("/")
        print("health:", r.status_code, r.json())

        txn = f"txn_{uuid.uuid4().hex[:12]}"
        results = await asyncio.gather(*[c.post("/v1/webhooks/transactions", json=body(txn)) for _ in range(5)])
        print("duplicate sends:", [(r.status_code, r.json()["duplicate"]) for r in results])
        assert all(r.status_code == 202 for r in results)
        assert sum(not r.json()["duplicate"] for r in results) == 1, "exactly one should be new"

        async def timed(i: int) -> float:
            t0 = time.perf_counter()
            r = await c.post("/v1/webhooks/transactions", json=body(f"load_{uuid.uuid4().hex[:10]}_{i}"))
            assert r.status_code == 202
            return (time.perf_counter() - t0) * 1000

        lat = sorted(await asyncio.gather(*[timed(i) for i in range(50)]))
        print(f"load: 50 webhooks  p50={lat[25]:.0f}ms  p95={lat[47]:.0f}ms  max={lat[-1]:.0f}ms")

        print(f"waiting for {txn} to be processed...")
        t0 = time.perf_counter()
        while True:
            r = await c.get(f"/v1/transactions/{txn}")
            data = r.json()[0]
            if data["status"] == "PROCESSED":
                break
            await asyncio.sleep(2)
        print(f"processed after ~{time.perf_counter() - t0:.0f}s:", data)


asyncio.run(main())
