CREATE TABLE IF NOT EXISTS raw_bars (
    symbol          TEXT             NOT NULL,
    bar_time        TIMESTAMPTZ      NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          BIGINT           NOT NULL,
    kafka_time      TIMESTAMPTZ      NOT NULL,
    processed_time  TIMESTAMPTZ      NOT NULL
);

CREATE INDEX IF NOT EXISTS raw_bars_symbol_time_idx ON raw_bars (symbol, bar_time);
CREATE INDEX IF NOT EXISTS raw_bars_time_idx ON raw_bars (bar_time);

CREATE TABLE IF NOT EXISTS agg_5m (
    window_start    TIMESTAMPTZ      NOT NULL,
    window_end      TIMESTAMPTZ      NOT NULL,
    symbol          TEXT             NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          BIGINT           NOT NULL,
    vwap            DOUBLE PRECISION,
    pct_change      DOUBLE PRECISION NOT NULL,
    bar_count       BIGINT           NOT NULL
);

CREATE INDEX IF NOT EXISTS agg_5m_symbol_window_idx ON agg_5m (symbol, window_start);
CREATE INDEX IF NOT EXISTS agg_5m_window_idx ON agg_5m (window_start);

CREATE TABLE IF NOT EXISTS volatility (
    window_start    TIMESTAMPTZ      NOT NULL,
    window_end      TIMESTAMPTZ      NOT NULL,
    symbol          TEXT             NOT NULL,
    volatility      DOUBLE PRECISION,
    avg_return      DOUBLE PRECISION NOT NULL,
    volume          BIGINT           NOT NULL,
    bar_count       BIGINT           NOT NULL
);

CREATE INDEX IF NOT EXISTS volatility_symbol_window_idx ON volatility (symbol, window_start);
CREATE INDEX IF NOT EXISTS volatility_window_idx ON volatility (window_start);

CREATE TABLE IF NOT EXISTS anomaly_scores (
    symbol            TEXT             NOT NULL,
    bar_time          TIMESTAMPTZ      NOT NULL,
    close             DOUBLE PRECISION NOT NULL,
    volume            BIGINT           NOT NULL,
    bar_return        DOUBLE PRECISION NOT NULL,
    z_return          DOUBLE PRECISION NOT NULL,
    z_range           DOUBLE PRECISION NOT NULL,
    z_volume          DOUBLE PRECISION NOT NULL,
    cluster           INTEGER          NOT NULL,
    kmeans_score      DOUBLE PRECISION NOT NULL,
    kmeans_anomaly    BOOLEAN          NOT NULL,
    zscore_anomaly    BOOLEAN          NOT NULL
);

CREATE INDEX IF NOT EXISTS anomaly_scores_symbol_time_idx ON anomaly_scores (symbol, bar_time);
CREATE INDEX IF NOT EXISTS anomaly_scores_flagged_idx ON anomaly_scores (bar_time) WHERE kmeans_anomaly;

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    recorded_at             TIMESTAMPTZ      NOT NULL DEFAULT now(),
    query_name              TEXT             NOT NULL,
    batch_id                BIGINT           NOT NULL,
    input_rows              BIGINT           NOT NULL,
    input_rows_per_sec      DOUBLE PRECISION,
    processed_rows_per_sec  DOUBLE PRECISION,
    batch_duration_ms       BIGINT           NOT NULL
);

CREATE INDEX IF NOT EXISTS pipeline_metrics_time_idx ON pipeline_metrics (recorded_at);
