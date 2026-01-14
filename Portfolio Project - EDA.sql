/*
SQL Project: Exploratory Data Analysis (EDA)
Dataset: Layoffs Data (2020-2023)
Goal: Explore trends, outliers, and patterns in layoff data.
*/

-- ---------------------------------------------------------
-- 1. GENERAL STATISTICS & OUTLIERS
-- ---------------------------------------------------------

-- View the full dataset
SELECT * FROM world_layoffs.layoffs_staging2;

-- Check maximum number of layoffs in a single day and max percentage
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM world_layoffs.layoffs_staging2;

-- Identify companies with 100% layoffs (went under)
-- Ordered by funds_raised_millions to see the largest company failures (e.g., Quibi, BritishVolt)
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


-- ---------------------------------------------------------
-- 2. BREAKDOWN BY KEY FACTORS
-- ---------------------------------------------------------

-- A. Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- B. Layoffs by Industry
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- C. Layoffs by Country
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- D. Layoffs by Year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- E. Layoffs by Company Stage (Startup vs. Established)
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;


-- ---------------------------------------------------------
-- 3. TEMPORAL ANALYSIS (TIME SERIES)
-- ---------------------------------------------------------

-- Rolling Total of Layoffs Per Month
-- This shows the momentum of layoffs growing over time
WITH Rolling_Total AS 
(
    SELECT SUBSTRING(`date`,1,7) AS `month`, SUM(total_laid_off) AS total_off
    FROM world_layoffs.layoffs_staging2
    WHERE SUBSTRING(`date`,1,7) IS NOT NULL
    GROUP BY `month`
    ORDER BY 1 ASC
)
SELECT `month`, total_off,
SUM(total_off) OVER(ORDER BY `month`) AS rolling_total
FROM Rolling_Total;


-- ---------------------------------------------------------
-- 4. COMPLEX ANALYSIS
-- ---------------------------------------------------------

-- Top 5 Companies with the most layoffs per year
-- Uses DENSE_RANK to rank companies within each year based on total layoffs
WITH Company_Year (company, years, total_laid_off) AS 
(
    SELECT company, YEAR(`date`), SUM(total_laid_off)
    FROM world_layoffs.layoffs_staging2
    GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS 
(
    SELECT *, 
    DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
    FROM Company_Year
    WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE ranking <= 5;