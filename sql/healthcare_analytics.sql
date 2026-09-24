-- Healthcare Claims Lakehouse
-- Gold-layer analytical queries
--
-- These queries demonstrate downstream analytical consumption
-- of curated Delta tables in Unity Catalog.


-- ============================================================
-- 1. Carrier utilization and payment trends by year
-- ============================================================

SELECT
    service_year,
    service_line_count,
    distinct_claim_count,
    distinct_beneficiary_count,
    distinct_hcpcs_count,
    distinct_provider_count,
    total_line_payment_amount,
    total_allowed_charge_amount,
    avg_line_payment_amount
FROM healthcare_claims.gold.carrier_yearly_summary
ORDER BY service_year;


-- ============================================================
-- 2. Carrier payment-to-allowed-charge ratio
-- ============================================================

SELECT
    service_year,
    total_line_payment_amount,
    total_allowed_charge_amount,
    ROUND(
        total_line_payment_amount
        / NULLIF(total_allowed_charge_amount, 0),
        4
    ) AS payment_to_allowed_ratio
FROM healthcare_claims.gold.carrier_yearly_summary
ORDER BY service_year;


-- ============================================================
-- 3. Prescription drug utilization and cost trends
-- ============================================================

SELECT *
FROM healthcare_claims.gold.prescription_drug_yearly_summary
ORDER BY service_year;


-- ============================================================
-- 4. Carrier data-quality monitoring
-- ============================================================

SELECT
    service_year,
    service_line_count,
    missing_provider_npi_count,
    negative_line_payment_count,
    ROUND(
        100.0 * missing_provider_npi_count
        / NULLIF(service_line_count, 0),
        4
    ) AS missing_provider_npi_pct
FROM healthcare_claims.gold.carrier_yearly_summary
ORDER BY service_year;


-- ============================================================
-- 5. Pipeline execution monitoring
-- ============================================================

SELECT
    pipeline_name,
    source_system,
    layer,
    status,
    rows_processed,
    run_timestamp
FROM healthcare_claims.ops.pipeline_audit
ORDER BY run_timestamp DESC;


-- ============================================================
-- 6. Data-quality monitoring
-- ============================================================

SELECT
    pipeline_name,
    dataset,
    total_rows,
    dq_status,
    run_timestamp
FROM healthcare_claims.ops.data_quality_audit
ORDER BY run_timestamp DESC;