CREATE TABLE t_batch_run_hourly (
    batch_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    report_param_id BIGINT NOT NULL,

    awb_date_from BIGINT NOT NULL,
    awb_date_to BIGINT NOT NULL,

    expected_count BIGINT,
    extracted_count BIGINT NOT NULL DEFAULT 0,

    status TEXT NOT NULL DEFAULT 'PENDING',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT fk_batch_run_report_param
        FOREIGN KEY (report_param_id)
        REFERENCES t_report_param (report_param_id),

    CONSTRAINT uq_batch_run_report_period
        UNIQUE (report_param_id, awb_date_from, awb_date_to),

    CONSTRAINT chk_batch_run_date_range
        CHECK (awb_date_from < awb_date_to),

    CONSTRAINT chk_batch_run_counts
        CHECK (
            extracted_count >= 0
            AND (expected_count IS NULL OR expected_count >= 0)
        ),

    CONSTRAINT chk_batch_run_status
        CHECK (
            status IN (
                'PENDING',
                'PROCESSING',
                'COMPLETED',
                'FAILED'
            )
        )
);

CREATE INDEX idx_batch_run_report_status
ON t_batch_run_hourly (report_param_id, status);