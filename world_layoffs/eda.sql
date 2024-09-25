-- Exploratory Data Analysis
select * from layoffs_staging2;

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

-- to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, sum(total_laid_off)
FROM layoffs_staging2
group by company
ORDER BY 2 DESC
LIMIT 5;

SELECT industry, sum(total_laid_off)
FROM layoffs_staging2
group by industry
ORDER BY 2 DESC
LIMIT 5;

select min(`date`) , max(`date`) 
from layoffs_staging2;

SELECT country, sum(total_laid_off)
FROM layoffs_staging2
group by country
ORDER BY 2 DESC
LIMIT 5;

SELECT year(`date`), sum(total_laid_off)
FROM layoffs_staging2
group by year(`date`)
ORDER BY 1 DESC
LIMIT 5;

-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(`date`,1,7) as `month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
where SUBSTRING(date,1,7) is not null
GROUP BY `month`
ORDER BY 1 ASC;

-- now use it in a CTE so we can query off of it
WITH rolling_cte AS 
(
SELECT SUBSTRING(`date`,1,7) as `month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
where SUBSTRING(date,1,7) is not null
GROUP BY `month`
ORDER BY 1 ASC
)
SELECT `month`,total_off
, SUM(total_off) OVER (ORDER BY `month` ASC) as rolling_total_layoffs
FROM rolling_cte
ORDER BY  `month` ASC;

-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. 
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;
