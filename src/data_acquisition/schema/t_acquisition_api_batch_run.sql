CREATE TABLE t_acquisition_api_batch_run (
    api_batch_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    batch_run_id BIGINT NOT NULL,

    page_number INTEGER NOT NULL,
    page_limit INTEGER NOT NULL DEFAULT 50000,

    expected_count INTEGER NOT NULL,
    extracted_count INTEGER NOT NULL DEFAULT 0,

    status TEXT NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT fk_acquisition_api_batch
        FOREIGN KEY (batch_run_id)
        REFERENCES t_acquisition_batch_hourly (batch_run_id),

    CONSTRAINT uq_acquisition_api_batch_page
        UNIQUE (batch_run_id, page_number),

    CONSTRAINT chk_acquisition_api_page
        CHECK (page_number >= 0),

    CONSTRAINT chk_acquisition_api_limit
        CHECK (page_limit BETWEEN 1 AND 50000),

    CONSTRAINT chk_acquisition_api_counts
        CHECK (
            expected_count >= 0
            AND extracted_count >= 0
            AND extracted_count <= expected_count
        ),

    CONSTRAINT chk_acquisition_api_attempt
        CHECK (attempt_count >= 0),

    CONSTRAINT chk_acquisition_api_status
        CHECK (
            status IN (
                'PENDING',
                'PROCESSING',
                'COMPLETED',
                'FAILED'
            )
        )
);