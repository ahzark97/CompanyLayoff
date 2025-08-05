-- Data Cleaning
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT * 
FROM world_layoffs.layoffs;

-- Create a staging table. To clean the data
CREATE TABLE world_layoffs.layoffs_staging4
LIKE world_layoffs.layoffs;

INSERT layoffs_staging4
SELECT * FROM world_layoffs.layoffs;

-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values 
-- 4. remove any columns and rows that are not necessary 

-- 1. Remove Duplicates
SELECT *
FROM world_layoffs.layoffs_staging4
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging4;

SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging4
) duplicates
WHERE 
	row_num > 1;
    
-- Confirm with company 'ODA'
SELECT *
FROM world_layoffs.layoffs_staging4
WHERE company = 'Oda'
;

-- Look at every single row to be accurate not just those 3. Find real duplicates where row_num > 1
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging4
) duplicates
WHERE 
	row_num > 1;

-- DELETE
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging4
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;

WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging4
)
DELETE FROM world_layoffs.layoffs_staging4
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- Not possible, so put it in staging 5, and delete column
-- Another way: Create new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
ALTER TABLE world_layoffs.layoffs_staging4 ADD row_num INT;

SELECT *
FROM world_layoffs.layoffs_staging4
;

CREATE TABLE `world_layoffs`.`layoffs_staging5` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging5`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging4;

-- now with this created table with row_num, i can delete rows were row_num is greater than 2
DELETE FROM world_layoffs.layoffs_staging5
WHERE row_num >= 2;




-- 2. Standardize Data
SELECT * 
FROM world_layoffs.layoffs_staging5;

-- There's some null and empty rows
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging5
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging5
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Look at company with null industry
SELECT *
FROM world_layoffs.layoffs_staging5
WHERE company LIKE 'Bally%';
-- Look at company with empty industry
SELECT *
FROM world_layoffs.layoffs_staging5
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
--  if there is another row with the same company name, it will update it to the non-null industry values

-- Set blanks to nulls 
UPDATE world_layoffs.layoffs_staging5
SET industry = NULL
WHERE industry = '';

-- now check if all null
SELECT *
FROM world_layoffs.layoffs_staging5
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- need to populate those nulls if possible
UPDATE layoffs_staging5 t1
JOIN layoffs_staging5 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM world_layoffs.layoffs_staging5;

-- Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_layoffs.layoffs_staging5
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- I also noticed the Crypto has multiple different variations. Standardize all to "Crypto"
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging5
ORDER BY industry;

UPDATE layoffs_staging5
SET industry = 'Crypto'
WHERE industry LIKE '%Crypto%';

-- Alternative
-- UPDATE layoffs_staging2
-- SET industry = 'Crypto'
-- WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;


-- Some are "United States" and some "United States.". Standardize
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging5
ORDER BY country;

UPDATE layoffs_staging5
SET country = TRIM(TRAILING '.' FROM country);

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging5
ORDER BY country;

-- Fix the date columns:
SELECT *
FROM world_layoffs.layoffs_staging5;

-- use str to date to update this field and convert data type properly
UPDATE layoffs_staging5
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging5
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoffs.layoffs_staging5;

-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. having them null makes it easier for me to do calculations during the EDA phase

-- 4. remove any columns and rows 

SELECT *
FROM world_layoffs.layoffs_staging5
WHERE total_laid_off IS NULL;

SELECT *
FROM world_layoffs.layoffs_staging5
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data can't really use
DELETE FROM world_layoffs.layoffs_staging5
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging5;

ALTER TABLE layoffs_staging5
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_staging5;


-- EDA
-- explore the data and find trends or patterns or anything interesting like outliers

SELECT * 
FROM world_layoffs.layoffs_staging5;


SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging5;


-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging5
WHERE percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging5
WHERE  percentage_laid_off = 1;
-- these are mostly startups it looks like who all went out of business during this time

-- order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM world_layoffs.layoffs_staging5
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- BritishVolt looks like an EV company, Quibi! raised like 2 billion dollars and went under 




-- Companies with the biggest single Layoff in a day
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging5
ORDER BY 2 DESC
LIMIT 5;

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off) AS Total_Layoff_Ever
FROM world_layoffs.layoffs_staging5
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;


-- by location
SELECT location, SUM(total_laid_off) AS Total_Layoff_Ever
FROM world_layoffs.layoffs_staging5
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

SELECT country, SUM(total_laid_off) AS Total_Layoff_Ever
FROM world_layoffs.layoffs_staging5
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off) AS Total_Layoff_Ever
FROM world_layoffs.layoffs_staging5
GROUP BY YEAR(date)
ORDER BY 1 ASC;


SELECT industry, SUM(total_laid_off) AS Total_Layoff_Ever
FROM world_layoffs.layoffs_staging5
GROUP BY industry
ORDER BY 2 DESC;


SELECT stage, SUM(total_laid_off) AS Total_Layoff_Ever
FROM world_layoffs.layoffs_staging5
GROUP BY stage
ORDER BY 2 DESC;



-- Companies with the most Layoffs per year.
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging5
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




-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging5
GROUP BY dates
ORDER BY dates ASC;

-- now use in a CTE to query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging5
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;
