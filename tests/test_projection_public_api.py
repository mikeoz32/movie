import movie.projection as projection_api


def test_projection_package_exports_only_public_api() -> None:
    assert set(projection_api.__all__) == {
        "PROJECTIONS",
        "AtLeastOnceHandler",
        "ExactlyOnceHandler",
        "ProjectionAlreadyRunningError",
        "ProjectionBaselineError",
        "ProjectionCapacityError",
        "ProjectionError",
        "ProjectionExtension",
        "ProjectionHandle",
        "ProjectionHandlerTimeout",
        "ProjectionId",
        "ProjectionOffsetConflictError",
        "ProjectionSourceConflictError",
        "ProjectionTransaction",
        "ProjectionTransactionError",
    }
