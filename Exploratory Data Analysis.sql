SELECT * 
FROM world_layoffs.layoffs_staging2;

SELECT *,
ROW_NUMBER() OVER()
FROM world_layoffs.layoffs_staging2;

SELECT COUNT(company)
FROM world_layoffs.layoffs_staging2;

SELECT MAX(world_layoffs.layoffs_staging2.percentage_laid_off) AS "MAX_percentage_laid_off", MAX(world_layoffs.layoffs_staging2.total_laid_off) AS "MAX_total_laid_off"
FROM world_layoffs.layoffs_staging2;

SELECT COUNT(world_layoffs.layoffs_staging2.percentage_laid_off) AS "TOTAL_percentage_laid_off = 100"
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT company ,SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY (company)
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

WITH Total_layoff_by_company AS
(
SELECT company ,SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY (company)
ORDER BY 2 DESC
)
SELECT COUNT(company)
FROM Total_layoff_by_company;

SELECT COUNT(company)
FROM world_layoffs.layoffs_staging2
WHERE world_layoffs.layoffs_staging2.total_laid_off >=  0;

SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY (industry)
ORDER BY 2 DESC;

SELECT company ,SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE YEAR(`date`) = 2023
GROUP BY (company)
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY (country)
ORDER BY 1 ASC;

SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY (stage)
ORDER BY 2 DESC;

WITH CTF_2023 AS
(
SELECT company ,SUM(total_laid_off) AS "total_laid_off_2023"
FROM world_layoffs.layoffs_staging2
WHERE YEAR(`date`) = 2023
GROUP BY (company)
ORDER BY 2 DESC
)
SELECT SUM(total_laid_off_2023), COUNT(company) AS "no_companies_that_layoff_in_2023"
FROM CTF_2023;

SELECT `date`, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY (`date`)
ORDER BY 1 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 ASC;

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH` ASC;

-- ORDER BY SUBSTRING(`date`,1,7) ASC

WITH MONTHLY_DATA AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS "total_laid_off"
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH` 
ORDER BY `MONTH` ASC
)
SELECT 
  `MONTH`,
  SUM(total_laid_off) OVER(ORDER BY `MONTH`) AS "Rolling_Laid_Off"
FROM MONTHLY_DATA;

WITH MONTHLY_DATA AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS "total_laid_off"
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH` 
ORDER BY `MONTH` ASC
)
SELECT 
  `MONTH`,
  SUM(total_laid_off) OVER(PARTITION BY SUBSTRING(`MONTH`,1,4) ORDER BY `MONTH`) AS "Yearly_Rolling_Laid_Off"
FROM MONTHLY_DATA;

SELECT company, YEAR(`date`), SUM(total_laid_off) AS "Total_Laid_Off"
FROM world_layoffs.layoffs_staging2
GROUP BY (company), YEAR(`date`)
ORDER BY 3 DESC
LIMIT 2000;

WITH Company_Year(company, years, total_laid_off) AS 
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY (company), YEAR(`date`)
LIMIT 2000
)
,
Company_Year_Ranked AS
(
SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL AND total_laid_off IS NOT NULL
LIMIT 2000
)
SELECT *
FROM Company_Year_Ranked
WHERE Ranking <= 5
;