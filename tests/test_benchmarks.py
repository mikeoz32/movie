from benchmarks.actor_benchmarks import benchmark_parallelism, benchmark_throughput


def test_throughput_benchmark_smoke():
    result = benchmark_throughput(messages=1_000, timeout_s=5.0)

    assert result.messages == 1_000
    assert result.elapsed_s > 0
    assert result.msg_per_sec > 0


def test_parallelism_benchmark_smoke():
    result = benchmark_parallelism(actors=4, work_iterations=50_000, timeout_s=10.0)

    assert result.actors == 4
    assert result.work_iterations == 50_000
    assert result.sequential_s > 0
    assert result.parallel_s > 0
    assert result.speedup > 0
