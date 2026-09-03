from __future__ import annotations

from movie.http.model import HttpRequest, HttpResponse

_TOKEN_BYTES = frozenset(
    b"!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)
_REASONS = {
    100: "Continue",
    200: "OK",
    201: "Created",
    202: "Accepted",
    204: "No Content",
    301: "Moved Permanently",
    302: "Found",
    304: "Not Modified",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    408: "Request Timeout",
    413: "Content Too Large",
    414: "URI Too Long",
    431: "Request Header Fields Too Large",
    500: "Internal Server Error",
    501: "Not Implemented",
    503: "Service Unavailable",
}


class HttpProtocolError(ValueError):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail
        self.parsed_requests: tuple[HttpRequest, ...] = ()


class HttpRequestParser:
    def __init__(
        self,
        *,
        max_request_line_bytes: int = 8 * 1024,
        max_header_bytes: int = 32 * 1024,
        max_header_count: int = 100,
        max_body_bytes: int = 1024 * 1024,
        max_buffer_bytes: int = 2 * 1024 * 1024,
    ) -> None:
        limits = (
            max_request_line_bytes,
            max_header_bytes,
            max_header_count,
            max_body_bytes,
            max_buffer_bytes,
        )
        if any(
            not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0 for limit in limits
        ):
            raise ValueError("HTTP parser limits must be positive integers")
        if max_header_bytes < max_request_line_bytes:
            raise ValueError("HTTP header limit must include the request-line limit")
        if max_buffer_bytes < max_header_bytes + max_body_bytes:
            raise ValueError("HTTP buffer limit must cover one maximum-size request")
        self._max_request_line_bytes = max_request_line_bytes
        self._max_header_bytes = max_header_bytes
        self._max_header_count = max_header_count
        self._max_body_bytes = max_body_bytes
        self._max_buffer_bytes = max_buffer_bytes
        self._buffer = bytearray()

    @property
    def buffered_bytes(self) -> int:
        return len(self._buffer)

    def feed(self, data: bytes, *, max_requests: int | None = None) -> list[HttpRequest]:
        if not isinstance(data, bytes):
            raise TypeError("HTTP parser input must be bytes")
        if max_requests is not None and (
            not isinstance(max_requests, int) or isinstance(max_requests, bool) or max_requests < 0
        ):
            raise ValueError("HTTP parser max_requests must be a nonnegative integer or None")
        if len(self._buffer) + len(data) > self._max_buffer_bytes:
            raise HttpProtocolError(413, "HTTP request buffer capacity exceeded")
        self._buffer.extend(data)
        requests = []
        while max_requests is None or len(requests) < max_requests:
            try:
                parsed = self._parse_one()
            except HttpProtocolError as error:
                error.parsed_requests = tuple(requests)
                raise
            if parsed is None:
                break
            request, consumed = parsed
            del self._buffer[:consumed]
            requests.append(request)
        return requests

    def finish(self, *, max_requests: int | None = None) -> list[HttpRequest]:
        requests = self.feed(b"", max_requests=max_requests)
        if max_requests == 0 or (max_requests is not None and len(requests) == max_requests):
            return requests
        if self._buffer:
            error = HttpProtocolError(400, "HTTP request ended before its framing was complete")
            error.parsed_requests = tuple(requests)
            raise error
        return requests

    def _parse_one(self) -> tuple[HttpRequest, int] | None:
        first_line_end = self._buffer.find(b"\r\n")
        if first_line_end < 0:
            if len(self._buffer) > self._max_request_line_bytes:
                raise HttpProtocolError(414, "HTTP request line is too long")
            return None
        if first_line_end > self._max_request_line_bytes:
            raise HttpProtocolError(414, "HTTP request line is too long")

        header_end = self._buffer.find(b"\r\n\r\n", first_line_end)
        if header_end < 0:
            if len(self._buffer) > self._max_header_bytes:
                raise HttpProtocolError(431, "HTTP request headers are too large")
            return None
        header_bytes = header_end + 4
        if header_bytes > self._max_header_bytes:
            raise HttpProtocolError(431, "HTTP request headers are too large")

        lines = bytes(self._buffer[:header_end]).split(b"\r\n")
        method, target, version = self._parse_request_line(lines[0])
        headers = self._parse_headers(lines[1:])
        body_bytes = self._content_length(headers)
        consumed = header_bytes + body_bytes
        if len(self._buffer) < consumed:
            return None
        body = bytes(self._buffer[header_bytes:consumed])
        return HttpRequest(method, target, headers, body, version), consumed

    @staticmethod
    def _parse_request_line(line: bytes) -> tuple[str, str, str]:
        parts = line.split(b" ")
        if len(parts) != 3 or not all(parts):
            raise HttpProtocolError(400, "Malformed HTTP request line")
        raw_method, raw_target, raw_version = parts
        if not raw_method or any(byte not in _TOKEN_BYTES for byte in raw_method):
            raise HttpProtocolError(400, "Malformed HTTP request method")
        try:
            method = raw_method.decode("ascii")
            target = raw_target.decode("ascii")
            version = raw_version.decode("ascii")
        except UnicodeDecodeError as error:
            raise HttpProtocolError(400, "HTTP request line must be ASCII") from error
        if any(ord(character) < 33 or ord(character) == 127 for character in target):
            raise HttpProtocolError(400, "Malformed HTTP request target")
        if version != "HTTP/1.1":
            raise HttpProtocolError(400, "Only HTTP/1.1 requests are supported")
        return method, target, version

    def _parse_headers(self, lines: list[bytes]) -> tuple[tuple[str, str], ...]:
        if len(lines) > self._max_header_count:
            raise HttpProtocolError(431, "HTTP request has too many headers")
        headers = []
        for line in lines:
            if not line or line[:1] in (b" ", b"\t"):
                raise HttpProtocolError(400, "Malformed HTTP header line")
            name, separator, raw_value = line.partition(b":")
            if not separator or not name or any(byte not in _TOKEN_BYTES for byte in name):
                raise HttpProtocolError(400, "Malformed HTTP header name")
            value = raw_value.strip(b" \t")
            if any((byte < 32 and byte != 9) or byte == 127 for byte in value):
                raise HttpProtocolError(400, "Malformed HTTP header value")
            headers.append((name.decode("ascii").lower(), value.decode("latin-1")))
        hosts = [value for name, value in headers if name == "host"]
        if len(hosts) != 1 or not hosts[0]:
            raise HttpProtocolError(400, "HTTP/1.1 requires exactly one Host header")
        return tuple(headers)

    def _content_length(self, headers: tuple[tuple[str, str], ...]) -> int:
        lengths = [value for name, value in headers if name == "content-length"]
        transfer_encodings = [value for name, value in headers if name == "transfer-encoding"]
        if transfer_encodings and lengths:
            raise HttpProtocolError(400, "Content-Length and Transfer-Encoding cannot be combined")
        if transfer_encodings:
            raise HttpProtocolError(501, "Transfer-Encoding request bodies are not supported")
        if len(lengths) > 1:
            raise HttpProtocolError(400, "Multiple Content-Length headers are not supported")
        if not lengths:
            return 0
        value = lengths[0]
        if not value or not value.isascii() or not value.isdecimal():
            raise HttpProtocolError(400, "Malformed Content-Length header")
        normalized = value.lstrip("0") or "0"
        if len(normalized) > len(str(self._max_body_bytes)):
            raise HttpProtocolError(413, "HTTP request body is too large")
        length = int(normalized)
        if length > self._max_body_bytes:
            raise HttpProtocolError(413, "HTTP request body is too large")
        return length


def render_response(
    response: HttpResponse,
    *,
    close: bool = False,
    head_request: bool = False,
) -> bytes:
    if 100 <= response.status < 200:
        raise ValueError("Informational HTTP responses are not supported")
    close = close or response.close_requested
    reason = response.reason if response.reason is not None else _REASONS.get(response.status, "")
    if "\r" in reason or "\n" in reason:
        raise ValueError("HTTP response reason must not contain line breaks")
    try:
        status_line = f"HTTP/1.1 {response.status} {reason}\r\n".encode("latin-1")
    except UnicodeEncodeError as error:
        raise ValueError("HTTP response reason must be Latin-1") from error
    if any((byte < 32 and byte != 9) or byte == 127 for byte in status_line[:-2]):
        raise ValueError("HTTP response reason contains invalid control bytes")

    body_allowed = response.status not in (204, 205, 304)
    if not body_allowed and response.body:
        raise ValueError(f"HTTP {response.status} responses must not contain a body")
    content_length = len(response.body) if body_allowed else 0
    payload = b"" if head_request or not body_allowed else response.body
    rendered_headers = []
    for name, value in response.headers:
        lower_name = name.lower()
        if lower_name in ("content-length", "connection"):
            continue
        if lower_name in ("transfer-encoding", "trailer"):
            raise ValueError(f"HTTP response header {name!r} conflicts with fixed-length framing")
        try:
            encoded_name = name.encode("ascii")
            encoded_value = value.encode("latin-1")
        except UnicodeEncodeError as error:
            raise ValueError(
                "HTTP response headers must use ASCII names and Latin-1 values"
            ) from error
        if not encoded_name or any(byte not in _TOKEN_BYTES for byte in encoded_name):
            raise ValueError("Malformed HTTP response header name")
        if any((byte < 32 and byte != 9) or byte == 127 for byte in encoded_value):
            raise ValueError("HTTP response header values contain invalid control bytes")
        rendered_headers.append(encoded_name + b": " + encoded_value + b"\r\n")
    if response.status not in (204, 304):
        rendered_headers.append(f"Content-Length: {content_length}\r\n".encode("ascii"))
    if close:
        rendered_headers.append(b"Connection: close\r\n")
    return status_line + b"".join(rendered_headers) + b"\r\n" + payload


__all__ = ["HttpProtocolError", "HttpRequestParser", "render_response"]
