from __future__ import annotations

from dataclasses import dataclass

HttpHeaders = tuple[tuple[str, str], ...]


def _normalize_headers(headers) -> HttpHeaders:
    normalized = tuple(headers)
    for header in normalized:
        if not isinstance(header, tuple) or len(header) != 2:
            raise TypeError("HTTP headers must contain (name, value) tuples")
        name, value = header
        if not isinstance(name, str) or not isinstance(value, str):
            raise TypeError("HTTP header names and values must be strings")
        if not name:
            raise ValueError("HTTP header names must not be empty")
    return normalized


def _header_values(headers: HttpHeaders, name: str) -> tuple[str, ...]:
    expected = name.lower()
    return tuple(value for header, value in headers if header.lower() == expected)


def _connection_close(headers: HttpHeaders) -> bool:
    return any(
        token.strip().lower() == "close"
        for value in _header_values(headers, "connection")
        for token in value.split(",")
    )


@dataclass(frozen=True, slots=True)
class HttpRequest:
    method: str
    target: str
    headers: HttpHeaders = ()
    body: bytes = b""
    version: str = "HTTP/1.1"

    def __post_init__(self) -> None:
        if not isinstance(self.method, str) or not self.method:
            raise ValueError("HTTP request method must be a nonempty string")
        if not isinstance(self.target, str) or not self.target:
            raise ValueError("HTTP request target must be a nonempty string")
        if self.version != "HTTP/1.1":
            raise ValueError("Only HTTP/1.1 requests are supported")
        if not isinstance(self.body, bytes):
            raise TypeError("HTTP request body must be bytes")
        object.__setattr__(self, "headers", _normalize_headers(self.headers))

    def header_values(self, name: str) -> tuple[str, ...]:
        return _header_values(self.headers, name)

    def header(self, name: str) -> str | None:
        values = self.header_values(name)
        return values[0] if values else None

    @property
    def close_requested(self) -> bool:
        return _connection_close(self.headers)


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int = 200
    headers: HttpHeaders = ()
    body: bytes = b""
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, int) or isinstance(self.status, bool):
            raise TypeError("HTTP response status must be an integer")
        if not 100 <= self.status <= 599:
            raise ValueError("HTTP response status must be between 100 and 599")
        if not isinstance(self.body, bytes):
            raise TypeError("HTTP response body must be bytes")
        if self.reason is not None and not isinstance(self.reason, str):
            raise TypeError("HTTP response reason must be a string or None")
        object.__setattr__(self, "headers", _normalize_headers(self.headers))

    def header_values(self, name: str) -> tuple[str, ...]:
        return _header_values(self.headers, name)

    def header(self, name: str) -> str | None:
        values = self.header_values(name)
        return values[0] if values else None

    @property
    def close_requested(self) -> bool:
        return _connection_close(self.headers)


__all__ = ["HttpHeaders", "HttpRequest", "HttpResponse"]
