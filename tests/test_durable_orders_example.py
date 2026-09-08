from examples.durable_orders import run_demo


def test_durable_orders_example_runs_end_to_end(tmp_path) -> None:
    result = run_demo(
        tmp_path,
        order_id="order-1001",
        reset=True,
        emit=lambda message: None,
    )

    assert result.order_id == "order-1001"
    assert 0 <= result.order_slice < 1024
    assert result.duplicate_payment_revision == 2
    assert result.recovered_revision == 2
    assert result.final_revision == 3
    assert result.final_status == "shipped"
    assert result.item_count == 3
    assert result.total_cents == 16_100
    assert result.audit_statuses == ("awaiting-payment", "paid", "shipped")
    assert result.audit_entry_count == 5
    assert result.outbox_entry_count == 5
    assert result.history_entry_count == 5
    assert result.exactly_once_retry_observed
    assert result.at_least_once_retry_observed
    assert result.compacted_changes == 5
    assert result.compaction_batches == (2, 2, 1)
    assert result.resumed_projection_offset == 4
    assert result.tombstone_revision == 2
    assert result.baseline_rejection_observed
    assert result.baseline_offset == 5
