-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- ***Data Cleaning*** 
-- 1. check and remove duplicates
-- 2. standardize data and fix errors
-- 3. Look at null values and remove any if needed
-- 4. remove any columns and rows that are not necessary

select * 
from layoffs;

-- creating a staging table to not misplace data
CREATE TABLE layoffs_staging 
LIKE layoffs;

select * 
FROM layoffs_staging;

-- copy data to new table
INSERT layoffs_staging 
SELECT * FROM layoffs;

-- Remove Duplicates
WITH dup_cte AS
 (
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off,`date`, stage, country, funds_raised_millions) 
AS row_num
FROM  layoffs_staging
) 
select * 
from dup_cte
WHERE row_num > 1;

-- to check if true for any row of duplicates cmd:
select * from layoffs_staging where company =" Included Health"  ;


-- to create another copy table for row num col inclusion
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
	PARTITION BY company, location, industry, total_laid_off,
    percentage_laid_off,`date`, stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging;

delete 
from layoffs_staging2 
where row_num > 1;

-- standardize data
select * 
from layoffs_staging2;

update layoffs_staging2 
set company= trim(company);

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staging2 order by 1;
-- USA has . at the end so standardize it

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

--  date column is of txt conv to date fmt
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- check null columns and rows 
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

-- if we look at industry it looks like we have some null and empty rows
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- convert blanks to null
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- remove null columns and rows values that we need to
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
-- check example
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';
-- nothing wrong here as it has only 1 row unlike airbnb 1 null and 1 transpiort value

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2;