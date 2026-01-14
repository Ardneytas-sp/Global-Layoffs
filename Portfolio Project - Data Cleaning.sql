/*
SQL Project: Data Cleaning
Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022
*/

-- ---------------------------------------------------------
-- 1. DATA STAGING
-- ---------------------------------------------------------

-- Create a staging table to preserve raw data
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT INTO world_layoffs.layoffs_staging 
SELECT * FROM world_layoffs.layoffs;


-- ---------------------------------------------------------
-- 2. DUPLICATE HANDLING
-- ---------------------------------------------------------

/* CTE DEMONSTRATION: 
Using a Common Table Expression to identify duplicates before deletion.
This allows for a safe check of the data logic.
*/

WITH Duplicate_CTE AS (
    SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
    FROM world_layoffs.layoffs_staging
)
SELECT * FROM Duplicate_CTE 
WHERE row_num > 1;


-- ACTUAL DELETION (MySQL Method):
-- Since MySQL does not allow direct deletes from CTEs with updates to the target table easily,
-- we create a staging table with the row_num column to filter deletes.

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `world_layoffs`.`layoffs_staging2`
SELECT *,
ROW_NUMBER() OVER (
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM world_layoffs.layoffs_staging;

DELETE 
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;


-- ---------------------------------------------------------
-- 3. STANDARDIZE DATA
-- ---------------------------------------------------------

-- A. Industry Cleanup
-- -------------------

-- Trim whitespace
UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);

-- Standardize 'Crypto' variations
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Convert empty strings to NULL
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Populate NULL industry values using data from the same company
UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


-- B. Location/Country Cleanup
-- ---------------------------

-- Fix trailing periods (e.g., "United States." -> "United States")
UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- C. Date Formatting
-- ------------------

-- Convert `date` text column to DATE format
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Modify column data type to DATE
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;


-- ---------------------------------------------------------
-- 4. NULL VALUES & GARBAGE DATA REMOVAL
-- ---------------------------------------------------------

-- Remove rows where both layoff metrics are NULL (useless data)
DELETE 
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Drop the helper column
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

-- Final Verification
SELECT * FROM world_layoffs.layoffs_staging2;