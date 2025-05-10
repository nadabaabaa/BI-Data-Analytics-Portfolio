-- DATA CLEANING 

SELECT *
FROM layoffs ; 

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways


-- 1. Remove Duplicates

 -- never work on real data so we crate a staging area where we do all the work 
 CREATE TABLE layoffs_staging
 LIKE layoffs ; 

SELECT *
FROM layoffs_staging ;  


INSERT layoffs_staging
SELECT *
FROM layoffs ; 

SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company,location , industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
	FROM 
		world_layoffs.layoffs_staging;


WITH duplicate_cte AS
(
SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company,location , industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
	FROM 
		world_layoffs.layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num > 1  ;   

SELECT *
FROM layoffs_staging 
WHERE company='Casper';

-- Tu ne peux pas utiliser ROW_NUMBER() dans un DELETE directement en MySQL (alors qu'on pourrait le faire dans d'autres bases comme PostgreSQL ou SQL Server).

-- Donc on prépare d'abord les données avec les row_num dans une nouvelle table (comme layoffs_staging2) → puis on supprime les doublons avec une requête simple.

 -- 1 Créer une table identique à la table principale layoffs, mais avec une colonne en plus row_num :

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
`row_num` INT
);

SELECT * 
FROM layoffs_staging2;

-- 2 Remplir cette nouvelle table avec les données de la table originale + la valeur générée par ROW_NUMBER()

INSERT INTO `world_layoffs`.`layoffs_staging2`
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;
        
-- 3 Supprimer toutes les lignes qui sont des doublons        
DELETE   
FROM layoffs_staging2
WHERE row_num > 1  ;

SELECT *   
FROM layoffs_staging2
WHERE row_num > 1  ;
-- standarizing data find errors and fix it 

SELECT trim(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

UPDATE layoffs_staging2
SET industry = 'Crypto'
where industry LIKE 'Crypto%';

Select industry
FROM layoffs_staging2
where industry LIKE 'Crypto%';

SELECT DISTINCT  location
FROM layoffs_staging2
ORDER BY 1	;

Select country
FROM layoffs_staging2
where country LIKE 'United States%';

SELECT DISTINCT country , TRIM(TRAILING '.' FROM  country)
FROM layoffs_staging2;
 
 UPDATE layoffs_staging2
SET  country = TRIM(TRAILING '.' FROM  country)
where country LIKE 'United States%';

 UPDATE layoffs_staging2
SET  `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN  `date`  DATE ;


-- 3. Look at null values and fix it

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE  company = 'Airbnb';


UPDATE  layoffs_staging2
SET  industry = 'Travel'
where company = 'Airbnb';

-- now we need to populate those nulls if possible

SELECT *
FROM layoffs_staging2
WHERE  company = 'Airbnb';

SELECT  industry , company
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT  t1.industry , t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE  layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET  t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;



-- Delete Useless null data we can't really use

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;  

DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num ;

SELECT *
FROM  layoffs_staging2 ;
-- Looking at Percentage to see how big these layoffs were


-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;
-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC; 


SELECT Max(total_laid_off), max(percentage_laid_off)
FROM  layoffs_staging2 ;

-- Companies with the biggest single Layoff
SELECT company , sum(total_laid_off)
FROM  layoffs_staging2
group by company
order by 2 DESC
LIMIT 5;

-- industries with the biggest single Layoff
SELECT industry , sum(total_laid_off)
FROM  layoffs_staging2
group by industry
order by 2 DESC
LIMIT 5;

-- countries with the biggest single Layoff
SELECT country , sum(total_laid_off)
FROM  layoffs_staging2
group by country
order by 2 DESC
LIMIT 5;
 
  -- 2 → SUM(total_laid_off)
  -- when this laid off happended corona 
SELECT MIN(`date`) , MAX(`date`)
FROM  layoffs_staging2;

SELECT year(`date`) , sum(total_laid_off)
FROM  layoffs_staging2
group by year(`date`)
order by 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stagedate
ORDER BY 2 DESC;


with Rolling_total AS
(
SELECT SUBSTRING(`date`,1,7) as `Month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL 
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month` ,total_off, SUM(total_off)  OVER (order by `Month` ) As rolling_total
FROM Rolling_total;

with company_year  (company , years , total_laid_off )AS
(
SELECT company, year(`date`), SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY company, year(`date`)
), Company_year_Rank AS
 (select * , dense_rank() over (partition by years ORDER BY total_laid_off DESC ) AS ranking
  FROM  company_year
  WHERE years IS NOT NULL
) select * 
  FROM  Company_year_Rank
  WHERE ranking <= 5 ;

-- 1. Trend Analysis Over Time

SELECT 
    DATE_FORMAT(`date`, '%Y-%m') AS month,
    SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
GROUP BY month
ORDER BY month;

-- Pre vs Post-COVID Layoffs (Assuming COVID starts ~2020-03)

SELECT 
    CASE 
        WHEN `date` < '2020-03-01' THEN 'Pre-COVID'
        ELSE 'Post-COVID'
    END AS period,
    SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
GROUP BY period;

--  . Startup vs Corporate Impact

SELECT 
    stage,
    COUNT(*) AS events,
    SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
WHERE stage IS NOT NULL
GROUP BY stage
ORDER BY total_layoffs DESC;

-- Country / Region Focus

SELECT 
    country,
    SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY country
ORDER BY total_layoffs DESC;

SELECT 
    CASE 
        WHEN country = 'United States' THEN 'North America'
        WHEN country IN ('Germany', 'France', 'United Kingdom', 'Spain', 'Netherlands') THEN 'Europe'
        WHEN country IN ('India', 'China', 'Japan', 'Singapore') THEN 'Asia'
        ELSE 'Other'
    END AS region,
    SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY region;

-- Companies That Shut Down (100% layoffs and closed)

SELECT 
    company,
    total_laid_off,
    percentage_laid_off
    
FROM layoffs_staging2
WHERE percentage_laid_off = 1 
ORDER BY total_laid_off DESC;




















    

 






