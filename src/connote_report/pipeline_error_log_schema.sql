CREATE TABLE t_pipeline_error_log (
    error_log_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    report_param_id BIGINT NOT NULL,
    batch_run_id BIGINT,
    api_batch_run_id BIGINT,

    error_stage TEXT NOT NULL,
    error_type TEXT NOT NULL,
    error_message TEXT NOT NULL,
    error_details JSONB,

    attempt_number INTEGER NOT NULL DEFAULT 1,
    is_retryable BOOLEAN NOT NULL DEFAULT FALSE,

    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,

    CONSTRAINT fk_error_log_report
        FOREIGN KEY (report_param_id)
        REFERENCES t_report_param (report_param_id),

    CONSTRAINT fk_error_log_batch
        FOREIGN KEY (batch_run_id)
        REFERENCES t_batch_run_hourly (batch_run_id),

    CONSTRAINT fk_error_log_api_batch
        FOREIGN KEY (api_batch_run_id)
        REFERENCES t_api_batch_run (api_batch_run_id),

    CONSTRAINT chk_error_log_attempt
        CHECK (attempt_number >= 1),

    CONSTRAINT chk_error_log_stage
        CHECK (
            error_stage IN (
                'REPORT_VALIDATION',
                'BATCH_GENERATION',
                'API_COUNT',
                'API_EXTRACTION',
                'DATA_VALIDATION',
                'STAGING_INSERT',
                'CSV_GENERATION'
            )
        )
);

CREATE INDEX idx_error_log_report
ON t_pipeline_error_log (report_param_id, occurred_at DESC);

CREATE INDEX idx_error_log_api_batch
ON t_pipeline_error_log (api_batch_run_id, occurred_at DESC)
WHERE api_batch_run_id IS NOT NULL;