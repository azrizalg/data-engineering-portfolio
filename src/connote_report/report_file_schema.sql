CREATE TABLE t_report_file (
    report_file_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    report_param_id BIGINT NOT NULL,

    file_number INTEGER NOT NULL,

    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,

    row_count BIGINT NOT NULL DEFAULT 0,
    file_size_bytes BIGINT NOT NULL DEFAULT 0,

    status TEXT NOT NULL DEFAULT 'PENDING',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT fk_report_file_param
        FOREIGN KEY (report_param_id)
        REFERENCES t_report_param (report_param_id),

    CONSTRAINT uq_report_file_number
        UNIQUE (report_param_id, file_number),

    CONSTRAINT chk_report_file_number
        CHECK (file_number >= 1),

    CONSTRAINT chk_report_file_row_count
        CHECK (row_count BETWEEN 0 AND 1000000),

    CONSTRAINT chk_report_file_size
        CHECK (file_size_bytes >= 0),

    CONSTRAINT chk_report_file_status
        CHECK (
            status IN (
                'PENDING',
                'PROCESSING',
                'COMPLETED',
                'FAILED'
            )
        )
);

CREATE INDEX idx_report_file_status
ON t_report_file (report_param_id, status);