/* Defining PK and FK */
SELECT * FROM claims_raw
where claim_id like '% %'
or patient_id like '% %';

-- any nulls?
SELECT COUNT(*) AS null_claim_ids 
FROM claims_raw 
WHERE claim_id IS NULL;

-- any duplicates?
SELECT claim_id, COUNT(*) c
FROM claims_raw
GROUP BY claim_id
HAVING c > 1
LIMIT 50;

SELECT * FROM claims_raw
where claim_id like '% %'
or patient_id like '% %';

-- creating the prepared tables 
-- claims_clean
CREATE TABLE claims_clean (
  claim_id         VARCHAR(64)  NOT NULL,
  patient_id       VARCHAR(64)  NOT NULL,
  service_date_ts  DATETIME     NULL,
  service_date     DATE         NULL,
  service_type     VARCHAR(100) NULL,
  procedure_code   VARCHAR(32)  NULL,
  billed_amount    DECIMAL(18,2) NULL,
  claim_status     VARCHAR(50)  NULL,
  approved_amount  DECIMAL(18,2) NULL,
  PRIMARY KEY (claim_id)
  ) ENGINE=InnoDB;

INSERT INTO claims_clean (
  patient_id, claim_id, service_date_ts, service_date, service_type,
  procedure_code, billed_amount, claim_status, approved_amount
)
SELECT
  -- removing leading/trailing spaces for the keys
  NULLIF(TRIM(`Patient_ID`), '') AS patient_id,
  NULLIF(TRIM(`Claim_ID`), '') AS claim_id,
  -- One parse that accepts both with time 
  COALESCE(
    STR_TO_DATE(NULLIF(TRIM(Service_Date),''), '%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(NULLIF(TRIM(Service_Date),''), '%Y-%m-%d'),
    STR_TO_DATE(NULLIF(TRIM(Service_Date),''), '%m/%d/%Y %H:%i:%s'),
    STR_TO_DATE(NULLIF(TRIM(Service_Date),''), '%m/%d/%Y')
  ) AS service_date_ts,
  -- Date-only derived from the parsed datetime 
  DATE(
    COALESCE(
      STR_TO_DATE(NULLIF(TRIM(Service_Date),''), '%Y-%m-%d %H:%i:%s'),
      STR_TO_DATE(NULLIF(TRIM(Service_Date),''), '%Y-%m-%d'),
      STR_TO_DATE(NULLIF(TRIM(Service_Date),''), '%m/%d/%Y %H:%i:%s'),
      STR_TO_DATE(NULLIF(TRIM(Service_Date),''), '%m/%d/%Y')
    )
  ) AS service_date,
  NULLIF(TRIM(`Service_Type`), '') AS service_type,
  -- digits only */
  NULLIF(TRIM(`Procedure_Code`), '') AS procedure_code,
  -- money: allow digits, dot, minus; remove commas/currency/etc. 
  CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Billed_Amount`),   ',', ''), '[^0-9.-]', ''), '') AS DECIMAL(18,2)) AS billed_amount,
  NULLIF(TRIM(`Claim_Status`), '') AS claim_status,
  CAST(NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`Approved_Amount`), ',', ''), '[^0-9.-]', ''), '') AS DECIMAL(18,2)) AS approved_amount
FROM claims_raw;

-- demographcis_clean
CREATE TABLE demographics_clean (
  patient_id VARCHAR(64) NOT NULL,
  age TINYINT UNSIGNED NULL,
  gender VARCHAR(6) NULL,
  state VARCHAR(3) NULL,
  medicare_plan VARCHAR(10)  NULL,
  PRIMARY KEY (patient_id)
  ) ENGINE=InnoDB;
  
INSERT INTO demographics_clean (
  patient_id, age, gender, state, medicare_plan
)
SELECT
  NULLIF(TRIM(`Patient_ID`), '') AS patient_id,
  -- keep only digits, then cast to number
  CAST(
    NULLIF(REGEXP_REPLACE(REPLACE(TRIM(`age`), ',', ''), '[^0-9]', ''), '')
    AS UNSIGNED
  ) AS age,
  NULLIF(TRIM(`gender`), '') AS gender,
  NULLIF(TRIM(`state`), '') AS state,
  NULLIF(TRIM(`medicare_plan`), '') AS medicare_plan
  FROM demographics_raw;
  
  -- diagnoses_clean
CREATE TABLE diagnoses_clean (
  patient_id VARCHAR(64) NOT NULL,
  diagnosis_date_ts DATETIME NULL,
  diagnosis_date DATE NULL,
  diagnosis_code VARCHAR(20) NULL,
  severity VARCHAR(20) NULL
  ) ENGINE=InnoDB;

INSERT INTO diagnoses_clean (
  patient_id, diagnosis_date_ts, diagnosis_date, diagnosis_code, severity
)
SELECT
  NULLIF(TRIM(`Patient_ID`), '') AS patient_id,
  COALESCE(
    STR_TO_DATE(NULLIF(TRIM(Diagnosis_Date),''), '%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(NULLIF(TRIM(Diagnosis_Date),''), '%Y-%m-%d'),
    STR_TO_DATE(NULLIF(TRIM(Diagnosis_Date),''), '%m/%d/%Y %H:%i:%s'),
    STR_TO_DATE(NULLIF(TRIM(Diagnosis_Date),''), '%m/%d/%Y')
  ) AS diagnosise_date_ts,
  DATE(
    COALESCE(
      STR_TO_DATE(NULLIF(TRIM(Diagnosis_Date),''), '%Y-%m-%d %H:%i:%s'),
      STR_TO_DATE(NULLIF(TRIM(Diagnosis_Date),''), '%Y-%m-%d'),
      STR_TO_DATE(NULLIF(TRIM(Diagnosis_Date),''), '%m/%d/%Y %H:%i:%s'),
      STR_TO_DATE(NULLIF(TRIM(Diagnosis_Date),''), '%m/%d/%Y')
    )
  ) AS diagnosise_date,
  NULLIF(TRIM(`diagnosis_code`), '') AS diagnosis_code,
  NULLIF(TRIM(`severity`), '') AS severity
  FROM diagnoses_raw;
  
  -- payments_clean --
CREATE TABLE payments_clean (
  claim_id VARCHAR(64) NOT NULL,
  payment_date_ts DATETIME NULL,
  payment_date DATE NULL,
  payer VARCHAR(25) NULL
  ) ENGINE=InnoDB;
  
INSERT INTO payments_clean (
  claim_id, payment_date_ts, payment_date, payer
)
SELECT
  NULLIF(TRIM(`claim_id`), '') AS claim_id,
  COALESCE(
    STR_TO_DATE(NULLIF(TRIM(payment_date),''), '%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(NULLIF(TRIM(payment_date),''), '%Y-%m-%d'),
    STR_TO_DATE(NULLIF(TRIM(payment_date),''), '%m/%d/%Y %H:%i:%s'),
    STR_TO_DATE(NULLIF(TRIM(payment_date),''), '%m/%d/%Y')
  ) AS payment_date_ts,
  DATE(
    COALESCE(
      STR_TO_DATE(NULLIF(TRIM(payment_date),''), '%Y-%m-%d %H:%i:%s'),
      STR_TO_DATE(NULLIF(TRIM(payment_date),''), '%Y-%m-%d'),
      STR_TO_DATE(NULLIF(TRIM(payment_date),''), '%m/%d/%Y %H:%i:%s'),
      STR_TO_DATE(NULLIF(TRIM(payment_date),''), '%m/%d/%Y')
    )
  ) AS payment_date,
  NULLIF(TRIM(`payer`), '') AS payer
  FROM payments_raw;