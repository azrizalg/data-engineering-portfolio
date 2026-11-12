CREATE TABLE t_report_param (
    report_param_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    awb_date_from BIGINT NOT NULL,
    awb_date_to BIGINT NOT NULL,

    status TEXT NOT NULL DEFAULT 'PENDING',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT chk_report_param_date_range
        CHECK (awb_date_from < awb_date_to),
    
    CONSTRAINT chk_report_param_max_duration
    CHECK (
        awb_date_to - awb_date_from
        <= 60 * 24 * 60 * 60 * 1000
    ),

    CONSTRAINT chk_report_param_status
        CHECK (status IN (
            'PENDING',
            'PROCESSING',
            'COMPLETED',
            'FAILED'
        ))
);