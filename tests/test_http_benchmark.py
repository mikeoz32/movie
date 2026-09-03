from benchmarks.http import benchmark_config, sampled_sequences


def test_http_benchmark_samples_connections_and_steady_state() -> None:
    connections = 10_000
    sampled = sampled_sequences(1_000_000, connections)

    assert len(sampled) == 1_000
    connection_indexes = {sequence % connections for sequence in sampled}
    rounds = {sequence // connections for sequence in sampled}
    assert len(connection_indexes) == 1_000
    assert max(connection_indexes) > 9_000
    assert max(rounds) > 90


def test_http_benchmark_sizes_limits_from_payload() -> None:
    payload_bytes = 1024 * 1024
    config = benchmark_config(3_200, 8, payload_bytes, 4, 2)

    assert config.get_int("movie.http.server.max-body-bytes") == payload_bytes + 8
    assert config.get_int("movie.io.tcp.backlog") == 3_200
    assert config.get_int("movie.io.tcp.write-byte-limit") > payload_bytes * 8
