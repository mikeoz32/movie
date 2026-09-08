import time

from fastapi.testclient import TestClient

from examples.durable_orders import app

CREATE_KEY = "00000000-0000-0000-0000-000000000001"
PAYMENT_KEY = "00000000-0000-0000-0000-000000000002"
SHIPMENT_KEY = "00000000-0000-0000-0000-000000000003"
ARCHIVE_KEY = "00000000-0000-0000-0000-000000000004"


def wait_for(client: TestClient, path: str, predicate, timeout: float = 3.0):
    deadline = time.monotonic() + timeout
    while True:
        response = client.get(path)
        if predicate(response):
            return response
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{path} did not reach the expected state")
        time.sleep(0.02)


def test_fastapi_order_service_survives_restart(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MOVIE_ORDER_DATA", str(tmp_path))
    order = {
        "order_id": "order-1001",
        "customer_id": "customer-42",
        "lines": [
            {"sku": "mechanical-keyboard", "quantity": 1, "unit_price_cents": 12_500},
            {"sku": "usb-c-cable", "quantity": 2, "unit_price_cents": 1_800},
        ],
    }

    with TestClient(app) as client:
        created = client.post(
            "/orders",
            json=order,
            headers={"Idempotency-Key": CREATE_KEY},
        )
        assert created.status_code == 202
        assert created.json()["revision"] == 1

        duplicate = client.post(
            "/orders",
            json=order,
            headers={"Idempotency-Key": CREATE_KEY},
        )
        assert duplicate.status_code == 202
        assert duplicate.json() == created.json()

        view = wait_for(
            client,
            "/orders/order-1001",
            lambda response: response.status_code == 200,
        ).json()
        assert view["status"] == "awaiting-payment"
        assert view["total_cents"] == 16_100

        payment = client.post(
            "/orders/order-1001/payments",
            json={"payment_id": "payment-9001"},
            headers={"Idempotency-Key": PAYMENT_KEY},
        )
        assert payment.status_code == 202
        assert payment.json()["revision"] == 2

    with TestClient(app) as client:
        payment_retry = client.post(
            "/orders/order-1001/payments",
            json={"payment_id": "payment-9001"},
            headers={"Idempotency-Key": PAYMENT_KEY},
        )
        assert payment_retry.status_code == 202
        assert payment_retry.json()["revision"] == 2

        shipment = client.post(
            "/orders/order-1001/shipments",
            json={"tracking_number": "TRACK-123"},
            headers={"Idempotency-Key": SHIPMENT_KEY},
        )
        assert shipment.status_code == 202
        assert shipment.json()["revision"] == 3

        shipped = wait_for(
            client,
            "/orders/order-1001",
            lambda response: response.status_code == 200
            and response.json()["status"] == "shipped",
        ).json()
        assert shipped["tracking_number"] == "TRACK-123"

        audit = wait_for(
            client,
            "/orders/order-1001/audit",
            lambda response: len(response.json()["changes"]) == 3,
        ).json()
        assert [change["revision"] for change in audit["changes"]] == [1, 2, 3]

        archived = client.delete(
            "/orders/order-1001",
            headers={"Idempotency-Key": ARCHIVE_KEY},
        )
        assert archived.status_code == 202
        assert archived.json()["revision"] == 4

        wait_for(
            client,
            "/orders/order-1001/audit",
            lambda response: len(response.json()["changes"]) == 4,
        )
        wait_for(
            client,
            "/orders/order-1001",
            lambda response: response.status_code == 404,
        )

        compacted = client.post("/admin/compact").json()["deleted_changes"]
        assert compacted == 4

        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ready"
