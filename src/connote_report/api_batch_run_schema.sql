CREATE TABLE t_api_batch_run (
    api_batch_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    batch_run_id BIGINT NOT NULL,

    page_number INTEGER NOT NULL,
    page_limit INTEGER NOT NULL DEFAULT 50000,

    expected_count INTEGER,
    extracted_count INTEGER NOT NULL DEFAULT 0,

    status TEXT NOT NULL DEFAULT 'PENDING',

    attempt_count INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT fk_api_batch_run_hourly
        FOREIGN KEY (batch_run_id)
        REFERENCES t_batch_run_hourly (batch_run_id),

    CONSTRAINT uq_api_batch_run_page
        UNIQUE (batch_run_id, page_number),

    CONSTRAINT chk_api_batch_run_page
        CHECK (page_number >= 1),

    CONSTRAINT chk_api_batch_run_limit
        CHECK (page_limit BETWEEN 1 AND 50000),

    CONSTRAINT chk_api_batch_run_counts
        CHECK (
            attempt_count >= 0
            AND extracted_count >= 0
            AND (expected_count IS NULL OR expected_count >= 0)
        ),

    CONSTRAINT chk_api_batch_run_status
        CHECK (
            status IN (
                'PENDING',
                'PROCESSING',
                'COMPLETED',
                'FAILED'
            )
        )
);

CREATE INDEX idx_api_batch_run_status
ON t_api_batch_run (batch_run_id, status);