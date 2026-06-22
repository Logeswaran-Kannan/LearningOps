-- =====================================================================================
-- GUIDEWIRE TABLE AUDIT SCRIPT (PostgreSQL / PL-pgSQL) -- SELECT-ONLY VERSION
-- =====================================================================================
-- No CREATE privilege required. Everything reads via SELECT; results are emitted as
-- RAISE NOTICE messages (visible in pgAdmin's "Messages" tab under the Query Tool).
--
-- PART 1: For every table in the schema -
--           - if a `createtime` column exists -> count rows from last N days
--           - if it does NOT exist            -> fast estimated overall row count
-- PART 2: For every table/column - flag columns containing a value larger than
--           the configured size threshold (default 1 MB)
--
-- Output lines are pipe-delimited so you can copy the Messages panel into a text
-- file and paste straight into Excel (Data > Text to Columns > delimiter "|").
-- =====================================================================================


-- =====================================================================================
-- PART 1: ROW COUNT AUDIT (createtime last 10 days OR overall estimate)
-- =====================================================================================
DO $$
DECLARE
    v_schema        text := 'public';     -- schema to audit
    v_ts_column     text := 'createtime'; -- the timestamp/epoch column to look for
    v_days          int  := 10;           -- "last N days" window

    v_rec           record;
    v_col_type      text;
    v_sql           text;
    v_count         bigint;
    v_estimate      bigint;
BEGIN
    RAISE NOTICE 'COUNT_AUDIT|schema|table|has_createtime|createtime_dtype|basis|row_count|error';

    FOR v_rec IN
        SELECT c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = v_schema
          AND c.relkind  = 'r'              -- ordinary tables only (skip views/foreign tables)
        ORDER BY c.relname
    LOOP
        v_col_type := NULL;
        BEGIN
            -- does the createtime column exist on this table?
            SELECT data_type INTO v_col_type
            FROM information_schema.columns
            WHERE table_schema = v_schema
              AND table_name   = v_rec.table_name
              AND lower(column_name) = lower(v_ts_column);

            IF v_col_type IS NOT NULL THEN

                IF v_col_type IN ('timestamp without time zone','timestamp with time zone','date') THEN
                    -- real timestamp column
                    v_sql := format(
                        'SELECT count(*) FROM %I.%I WHERE %I >= now() - interval ''%s days''',
                        v_schema, v_rec.table_name, v_ts_column, v_days
                    );
                ELSE
                    -- assume Guidewire-style epoch-millis bigint/numeric column
                    v_sql := format(
                        'SELECT count(*) FROM %I.%I WHERE %I >= (extract(epoch from now() - interval ''%s days'') * 1000)::bigint',
                        v_schema, v_rec.table_name, v_ts_column, v_days
                    );
                END IF;

                EXECUTE v_sql INTO v_count;

                RAISE NOTICE 'COUNT_AUDIT|%|%|%|%|LAST_N_DAYS|%|',
                    v_schema, v_rec.table_name, true, v_col_type, v_count;

            ELSE
                -- no createtime column -> use planner's row estimate (fast, metadata-only,
                -- no table scan). Swap to an exact COUNT(*) below if you need precision.
                SELECT reltuples::bigint INTO v_estimate
                FROM pg_class
                WHERE oid = (quote_ident(v_schema) || '.' || quote_ident(v_rec.table_name))::regclass;

                RAISE NOTICE 'COUNT_AUDIT|%|%|%|%|OVERALL_ESTIMATE|%|',
                    v_schema, v_rec.table_name, false, NULL, GREATEST(v_estimate, 0);
            END IF;

        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'COUNT_AUDIT|%|%|%|%|ERROR||%',
                v_schema, v_rec.table_name, NULL, v_col_type, SQLERRM;
        END;
    END LOOP;
END $$;


-- =====================================================================================
-- PART 2: OVERSIZED COLUMN VALUE AUDIT (> 1 MB)
-- =====================================================================================
DO $$
DECLARE
    v_schema          text  := 'public';
    v_size_threshold  bigint := 1048576;  -- 1 MB in bytes

    v_tbl             record;
    v_col             record;
    v_sql             text;
    v_found           boolean;
    v_toast_size      bigint;
BEGIN
    RAISE NOTICE 'LARGE_COL_AUDIT|schema|table|column|data_type|flagged_over_1MB|error';

    FOR v_tbl IN
        SELECT c.relname AS table_name, c.reltoastrelid
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = v_schema
          AND c.relkind  = 'r'
        ORDER BY c.relname
    LOOP
        -- Pre-filter (metadata only, no row scan): values over ~2KB get pushed into
        -- the table's TOAST relation. If the TOAST relation is empty, this table
        -- cannot contain any value anywhere near 1MB, so skip it entirely.
        v_toast_size := 0;
        IF v_tbl.reltoastrelid <> 0 THEN
            SELECT pg_total_relation_size(v_tbl.reltoastrelid) INTO v_toast_size;
        END IF;

        IF v_toast_size > 0 THEN
            FOR v_col IN
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = v_schema
                  AND table_name   = v_tbl.table_name
                  AND data_type IN ('text','character varying','character',
                                     'bytea','json','jsonb','xml')
            LOOP
                BEGIN
                    -- pg_column_size = stored (possibly compressed) byte size of the value.
                    -- Cheaper than octet_length() but can under-report for highly
                    -- compressible text/json/xml. Swap to octet_length(%I) for exact
                    -- uncompressed length if accuracy matters more than speed.
                    v_sql := format(
                        'SELECT EXISTS (SELECT 1 FROM %I.%I WHERE pg_column_size(%I) > %s)',
                        v_schema, v_tbl.table_name, v_col.column_name, v_size_threshold
                    );
                    EXECUTE v_sql INTO v_found;

                    IF v_found THEN
                        RAISE NOTICE 'LARGE_COL_AUDIT|%|%|%|%|true|',
                            v_schema, v_tbl.table_name, v_col.column_name, v_col.data_type;
                    END IF;

                EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'LARGE_COL_AUDIT|%|%|%|%||%',
                        v_schema, v_tbl.table_name, v_col.column_name, v_col.data_type, SQLERRM;
                END;
            END LOOP;
        END IF;
    END LOOP;
END $$;
