/* Various Examples on possible insights, and things of interest to stakeholder */
-- Service Type by approved amount
with top_services as (
SELECT claims.*, demographics.age,  demographics.gender,  demographics.state, demographics.medicare_plan
FROM claims_clean as claims
left join demographics_clean as demographics 
on claims.patient_id = demographics.patient_id)
select top_services.service_type, sum(top_services.approved_amount) as approved_amount
from top_services
where top_services.claim_status = 'approved' 
group by top_services.service_type
order by approved_amount desc;

-- Claims trend (month over month)
SELECT DATE_FORMAT(service_date, '%Y-%m') AS yr_mo,
       COUNT(*) AS claims
FROM claims_clean c
GROUP BY yr_mo
ORDER BY yr_mo;

-- Service mix (what drives volume)
SELECT service_type, COUNT(*) AS claims
FROM claims_clean
GROUP BY service_type
ORDER BY claims DESC;

-- Top procedures by volume and cost
SELECT c.Procedure_Code,
       COUNT(*) AS claims,
       SUM(c.Billed_Amount) AS billed_total,
       SUM(c.Approved_Amount) AS approved_total
FROM claims_clean c
#WHERE c.claim_status = 'approved'
GROUP BY c.Procedure_Code
ORDER BY approved_total DESC
LIMIT 10;

-- Patient responsibility (what members actually pay)
SELECT SUM(patient_payment) AS patient_pay_total
FROM payments_clean;

-- -------------------------------------------------------------------
-- View created for visualization purposes, with multiple fields and data.
-- Base enrichment: claims + member demographics
CREATE OR REPLACE VIEW vw_claims_demographics AS
SELECT
  c.claim_id,
  c.patient_id,
  c.service_date,
  c.service_type,
  c.procedure_code,
  c.billed_amount,
  c.approved_amount,
  c.claim_status,
  d.age,
  CASE
    WHEN d.age IS NULL THEN 'Unknown'
    WHEN d.age < 18 THEN '00-17'
    WHEN d.age BETWEEN 18 AND 34 THEN '18-34'
    WHEN d.age BETWEEN 35 AND 49 THEN '35-49'
    WHEN d.age BETWEEN 50 AND 64 THEN '50-64'
    ELSE '65+'
  END AS age_band,
  d.gender,
  d.state,
  d.medicare_plan,
  CASE WHEN c.approved_amount > 0 AND UPPER(IFNULL(c.claim_status,'')) <> 'DENIED' THEN 1 ELSE 0 END AS is_approved,
  CASE WHEN c.approved_amount = 0 OR UPPER(IFNULL(c.claim_status,'')) = 'DENIED' THEN 1 ELSE 0 END AS is_denied
FROM claims_clean c
LEFT JOIN demographics_clean d ON d.patient_ID = c.patient_id;
