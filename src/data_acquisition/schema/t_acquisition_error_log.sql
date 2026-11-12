CREATE TABLE t_acquisition_error_log (
    error_log_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    
    batch_run_id BIGINT NOT NULL,
    api_batch_run_id BIGINT,

    error_stage TEXT NOT NULL,
    error_type TEXT NOT NULL,
    error_message TEXT NOT NULL,
    error_details JSONB,

    attempt_number INTEGER NOT NULL DEFAULT 1,
    is_retryable BOOLEAN NOT NULL DEFAULT FALSE,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,

    CONSTRAINT fk_acquisition_error_batch
        FOREIGN KEY (batch_run_id)
        REFERENCES t_acquisition_batch_hourly (batch_run_id),

    CONSTRAINT fk_acquisition_error_api_batch
        FOREIGN KEY (api_batch_run_id)
        REFERENCES t_acquisition_api_batch_run (api_batch_run_id),

    CONSTRAINT chk_acquisition_error_stage
        CHECK (
            error_stage IN (
                'API_COUNT',
                'API_EXTRACTION',
                'DATA_VALIDATION',
                'DATA_MASKING',
                'MONGODB_INSERT',
                'BATCH_VALIDATION'
            )
        )
);

CREATE INDEX idx_acquisition_error_batch
ON t_acquisition_error_log (
    batch_run_id,
    occurred_at DESC
);

CREATE INDEX idx_acquisition_error_api_batch
ON t_acquisition_error_log (
    api_batch_run_id,
    occurred_at DESC
)
WHERE api_batch_run_id IS NOT NULL;

COMMENT ON TABLE t_acquisition_error_log IS
'Stores error history for the JNE data acquisition pipeline. Each failure creates a new record; previous errors are retained for auditing.';

COMMENT ON COLUMN t_acquisition_error_log.error_log_id IS
'Unique identifier for each error log entry.';

COMMENT ON COLUMN t_acquisition_error_log.batch_run_id IS
'Foreign key referencing t_acquisition_batch_hourly. Identifies the hourly acquisition batch associated with the error.';

COMMENT ON COLUMN t_acquisition_error_log.api_batch_run_id IS
'Nullable foreign key referencing t_acquisition_api_batch_run. Identifies the specific paginated API request associated with the error. NULL when the error occurs before an API batch record exists, such as during API_COUNT.';

COMMENT ON COLUMN t_acquisition_error_log.error_stage IS
'Pipeline stage where the error occurred, such as API_COUNT, API_EXTRACTION, DATA_VALIDATION, DATA_MASKING, or MONGODB_INSERT.';

COMMENT ON COLUMN t_acquisition_error_log.error_type IS
'Classification of the error, such as TimeoutError, ConnectionError, HTTPError, or DatabaseError.';

COMMENT ON COLUMN t_acquisition_error_log.error_message IS
'Human-readable description of the error.';

COMMENT ON COLUMN t_acquisition_error_log.error_details IS
'Optional JSONB containing additional debugging context, such as API response details, endpoint information, MongoDB error codes, or affected record counts. Must not contain sensitive or unmasked data.';

COMMENT ON COLUMN t_acquisition_error_log.attempt_number IS
'Sequential attempt number associated with the failed operation. Starts at 1.';

COMMENT ON COLUMN t_acquisition_error_log.is_retryable IS
'Indicates whether the failed operation is eligible for automatic retry. TRUE for transient failures; FALSE for permanent failures or errors requiring manual intervention.';

COMMENT ON COLUMN t_acquisition_error_log.occurred_at IS
'Timestamp when the error occurred.';

COMMENT ON COLUMN t_acquisition_error_log.resolved_at IS
'Timestamp when the specific error was successfully addressed, typically after a successful retry. NULL while unresolved.';