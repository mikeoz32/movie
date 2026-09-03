import pytest

from movie.http import (
    HttpProtocolError,
    HttpRequestParser,
    HttpResponse,
    render_response,
)


def test_http_parser_handles_fragmented_request_body() -> None:
    parser = HttpRequestParser()

    assert parser.feed(b"POST /echo HTTP/1.1\r\nHost: example") == []
    assert parser.feed(b"\r\nContent-Length: 5\r\n\r\nhe") == []
    requests = parser.feed(b"llo")

    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert requests[0].target == "/echo"
    assert requests[0].header("host") == "example"
    assert requests[0].body == b"hello"
    assert parser.buffered_bytes == 0


def test_http_parser_handles_pipelined_requests_with_a_bound() -> None:
    parser = HttpRequestParser()
    wire = (
        b"GET /one HTTP/1.1\r\nHost: localhost\r\n\r\n"
        b"GET /two HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )

    first = parser.feed(wire, max_requests=1)
    second = parser.feed(b"", max_requests=1)

    assert [request.target for request in first] == ["/one"]
    assert [request.target for request in second] == ["/two"]
    assert second[0].close_requested
    assert parser.buffered_bytes == 0


def test_http_parser_preserves_valid_prefix_before_protocol_error() -> None:
    parser = HttpRequestParser()

    with pytest.raises(HttpProtocolError) as raised:
        parser.feed(b"GET /valid HTTP/1.1\r\nHost: localhost\r\n\r\nGET /invalid HTTP/1.1\r\n\r\n")

    assert [request.target for request in raised.value.parsed_requests] == ["/valid"]
    assert raised.value.status == 400


@pytest.mark.parametrize(
    ("wire", "status"),
    [
        (b"GET / HTTP/1.1\r\n\r\n", 400),
        (
            b"POST / HTTP/1.1\r\nHost: localhost\r\n"
            b"Content-Length: 1\r\nContent-Length: 1\r\n\r\nx",
            400,
        ),
        (
            b"POST / HTTP/1.1\r\nHost: localhost\r\n"
            b"Content-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\nx",
            400,
        ),
        (
            b"POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
            501,
        ),
    ],
)
def test_http_parser_rejects_ambiguous_or_unsupported_framing(
    wire: bytes,
    status: int,
) -> None:
    parser = HttpRequestParser()

    with pytest.raises(HttpProtocolError) as raised:
        parser.feed(wire)

    assert raised.value.status == status


def test_http_parser_enforces_request_limits() -> None:
    parser = HttpRequestParser(
        max_request_line_bytes=16,
        max_header_bytes=32,
        max_header_count=1,
        max_body_bytes=4,
        max_buffer_bytes=64,
    )

    with pytest.raises(HttpProtocolError) as raised:
        parser.feed(b"GET /target-too-long HTTP/1.1\r\n")
    assert raised.value.status == 414


def test_http_parser_rejects_content_length_above_integer_digit_limit() -> None:
    parser = HttpRequestParser()
    wire = b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: " + b"9" * 5000 + b"\r\n\r\n"

    with pytest.raises(HttpProtocolError) as raised:
        parser.feed(wire)

    assert raised.value.status == 413


def test_http_renderer_sets_framing_and_connection_headers() -> None:
    response = HttpResponse(
        200,
        (("Content-Type", "text/plain"), ("Content-Length", "wrong")),
        b"hello",
    )

    wire = render_response(response, close=True)

    assert wire == (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/plain\r\n"
        b"Content-Length: 5\r\n"
        b"Connection: close\r\n"
        b"\r\nhello"
    )


def test_http_renderer_suppresses_head_response_body() -> None:
    wire = render_response(HttpResponse(body=b"hello"), head_request=True)

    assert wire.endswith(b"Content-Length: 5\r\n\r\n")


def test_http_renderer_rejects_ambiguous_response_framing() -> None:
    with pytest.raises(ValueError, match="fixed-length framing"):
        render_response(HttpResponse(headers=(("Transfer-Encoding", "chunked"),)))


def test_http_renderer_omits_prohibited_content_length() -> None:
    assert render_response(HttpResponse(204)) == b"HTTP/1.1 204 No Content\r\n\r\n"
    with pytest.raises(ValueError, match="must not contain a body"):
        render_response(HttpResponse(205, body=b"invalid"))


def test_http_codec_rejects_invalid_control_bytes() -> None:
    parser = HttpRequestParser()
    with pytest.raises(HttpProtocolError):
        parser.feed(b"GET / HTTP/1.1\r\nHost: localhost\r\n: empty-name\r\n\r\n")
    with pytest.raises(ValueError, match="control bytes"):
        render_response(HttpResponse(headers=(("X-Test", "bad\x00value"),)))
