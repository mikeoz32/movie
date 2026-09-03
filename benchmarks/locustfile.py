from locust import FastHttpUser, task

_EXPECTED_BODY = b"Hello, World!"
_EXPECTED_CONTENT_TYPE = "text/plain"


class PlaintextUser(FastHttpUser):
    connection_timeout = 10.0
    network_timeout = 10.0

    @task
    def plaintext(self) -> None:
        with self.client.get(
            "/plaintext",
            name="/plaintext",
            catch_response=True,
            allow_redirects=False,
        ) as response:
            if response.status_code == 0:
                # Preserve Locust's underlying transport exception.
                return
            elif response.status_code != 200:
                response.failure(f"unexpected HTTP status {response.status_code}")
            elif response.headers.get("Content-Type") != _EXPECTED_CONTENT_TYPE:
                response.failure("unexpected Content-Type")
            elif response.content != _EXPECTED_BODY:
                response.failure("unexpected response body")
