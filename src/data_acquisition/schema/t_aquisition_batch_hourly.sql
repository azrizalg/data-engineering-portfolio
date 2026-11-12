CREATE TABLE t_acquisition_batch_hourly (
    batch_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    awb_date_from BIGINT NOT NULL,
    awb_date_to BIGINT NOT NULL,

    expected_count BIGINT,
    extracted_count BIGINT NOT NULL DEFAULT 0,

    status TEXT NOT NULL DEFAULT 'PENDING',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT uq_acquisition_batch_period
        UNIQUE (awb_date_from, awb_date_to),

    CONSTRAINT chk_acquisition_batch_date_range
        CHECK (awb_date_from < awb_date_to),

    CONSTRAINT chk_acquisition_batch_counts
        CHECK (
            extracted_count >= 0
            AND (expected_count IS NULL OR expected_count >= 0)
        ),

    CONSTRAINT chk_acquisition_batch_status
        CHECK (
            status IN (
                'PENDING',
                'PROCESSING',
                'COMPLETED',
                'FAILED'
            )
        )  
);
CREATE INDEX idx_acquisition_batch_status
ON t_acquisition_batch_hourly (status, awb_date_from);