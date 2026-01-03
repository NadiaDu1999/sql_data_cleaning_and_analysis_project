SELECT *
FROM job;

set sql_safe_updates = 0;

-- Data Cleaning Processes
-- 1. Check duplicate rows
-- 2. Removing irrelevant values from a column
-- 3. Standardizing the data
-- 4. Create new columns 
-- 5. Text-to-numeric transformation (post_date)
-- 6. Remove unnecessary columns
-- 7. Missing value imputation
-- 8. Relocate columns


-- 1. Check duplicate rows
# Create a copy table
CREATE TABLE `job_copy` (
  `job_title` text,
  `seniority_level` text,
  `status` text,
  `company` text,
  `location` text,
  `post_date` text,
  `headquarter` text,
  `industry` text,
  `ownership` text,
  `company_size` text,
  `revenue` text,
  `salary` text,
  `skills` text,
  rn int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO job_copy
SELECT *, 
ROW_NUMBER() OVER(Partition BY job_title, seniority_level, `status`, company, location, post_date, headquarter, industry, ownership, 
company_size, revenue, salary, skills) as rn
FROM job;

DELETE
FROM job_copy
WHERE rn >1; # There is no duplicates

SELECT *
FROM job_copy;
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 2. Removing irrelevant values from a column
SELECT 
    location,
    TRIM(SUBSTRING_INDEX(location, '.', -1)) AS last_part,
    TRIM(
        SUBSTRING(location, 
                  1, 
                  LENGTH(location) - LENGTH(SUBSTRING_INDEX(location, '.', -1)) - 1)
    ) AS cleaned_location
FROM job_copy
WHERE location LIKE '%.%'
  AND (
       LOWER(TRIM(SUBSTRING_INDEX(location, '.', -1))) LIKE '%remote%'
    OR LOWER(TRIM(SUBSTRING_INDEX(location, '.', -1))) LIKE '%hybrid%'
    OR LOWER(TRIM(SUBSTRING_INDEX(location, '.', -1))) LIKE '%on-site%'
  );
  
UPDATE job_copy
SET location = TRIM(
        SUBSTRING(location, 
                  1, 
                  LENGTH(location) - LENGTH(SUBSTRING_INDEX(location, '.', -1)) - 1)
    )
WHERE location LIKE '%.%'
  AND (
       LOWER(TRIM(SUBSTRING_INDEX(location, '.', -1))) LIKE '%remote%'
    OR LOWER(TRIM(SUBSTRING_INDEX(location, '.', -1))) LIKE '%hybrid%'
    OR LOWER(TRIM(SUBSTRING_INDEX(location, '.', -1))) LIKE '%on-site%'
  );

# Change some rows is still 'Hybrid, remote and On-site', they will be changed to 'Not provided'
SELECT location
FROM job_copy
WHERE LOWER(TRIM(location)) IN ('hybrid', 'fully remote', 'on-site', 'remote', '');

UPDATE job_copy
SET location = 'Not provided'
WHERE LOWER(TRIM(location)) IN ('hybrid', 'fully remote', 'on-site', 'remote', '');
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 3. Standardizing the data
# remove characters from the columns
# company_size
SELECT distinct company_size
from job_copy; # contain characters > change it to NULL

UPDATE job_copy
SET company_size = CASE
	WHEN company_size LIKE '%B%' OR company_size IN ('Private', '')
    THEN NULL
    ELSE company_size
END;

# revenue
SELECT distinct revenue
from job_copy;

SELECT revenue
from job_copy
where revenue LIKE '%K%';

UPDATE job_copy
SET revenue = CASE
	WHEN revenue IN ('Private', 'Nonprofit', 'Education', 'Public')
    THEN NULL
	WHEN RIGHT(TRIM(revenue), 1) = 'T'
    THEN CONCAT(CAST(REPLACE(REPLACE(revenue, '€', ''), 'T', '') AS DECIMAL(20,0)) * 1000000000000)
    WHEN RIGHT(TRIM(revenue),1) = 'B'
    THEN CONCAT(CAST(REPLACE(REPLACE(revenue, '€', ''), 'B', '') AS DECIMAL(20,0)) *1000000000)
    WHEN RIGHT(TRIM(revenue), 1) = 'M'
    THEN CONCAT(CAST(REPLACE(REPLACE(revenue, '€', ''), 'M', '') AS DECIMAL(20,0)) *1000000)
    ELSE revenue
END;

# company_size
UPDATE job_copy
SET company_size = REPLACE(TRIM(company_size), ',' ,'');

ALTER TABLE job_copy
MODIFY COLUMN company_size INT;

SELECT *
from job_copy;
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 4. Create new columns 
# hq_country
ALTER TABLE job_copy
ADD hq_country text NOT NULL;

UPDATE job_copy
SET hq_country = TRIM(SUBSTRING_INDEX(headquarter, ',', -1))
WHERE headquarter LIKE '%,%';

# hq_state
UPDATE job_copy
SET headquarter = TRIM(SUBSTRING(headquarter, 1, LENGTH(headquarter) - LENGTH(SUBSTRING_INDEX(headquarter, ',', -1)) -1))
WHERE headquarter LIKE '%,%';

ALTER TABLE job_copy
RENAME COLUMN headquarter TO hq_state; 

# create a min_salary column, using salary column
ALTER TABLE job_copy
ADD min_salary text;

UPDATE job_copy
SET min_salary = CASE
	WHEN salary LIKE '%-%'
    THEN REPLACE(REPLACE(TRIM(SUBSTRING_INDEX(salary, '-', 1)), ',',''), '€', '')
    ELSE REPLACE(REPLACE(salary, ',',''), '€','')
END;

ALTER TABLE job_copy
MODIFY COLUMN min_salary INT;

# create a max_salary column
ALTER TABLE job_copy
ADD max_salary text;

UPDATE job_copy
SET max_salary = CASE
	WHEN salary LIKE '%-%'
    THEN REPLACE(REPLACE(TRIM(SUBSTRING_INDEX(salary, '-', -1)), ',',''), '€', '')
    ELSE REPLACE(REPLACE(salary, ',',''), '€','')
END;

ALTER TABLE job_copy
MODIFY COLUMN max_salary INT;

# create an avg_salary column
ALTER TABLE job_copy
ADD avg_salary INT;

UPDATE job_copy
SET avg_salary = (max_salary + min_salary) / 2;

# Create binary features for important skills, so it is easy to visualize
ALTER TABLE job_copy
ADD `sql` BIT NOT NULL DEFAULT 0,
ADD `python` BIT NOT NULL DEFAULT 0,
ADD `r` BIT NOT NULL DEFAULT 0,
ADD `java` BIT NOT NULL DEFAULT 0,
ADD `aws` BIT NOT NULL DEFAULT 0,
ADD `ML` BIT NOT NULL DEFAULT 0,
ADD `tableau` BIT NOT NULL DEFAULT 0,
ADD `powerbi` BIT NOT NULL DEFAULT 0,
ADD `excel` BIT NOT NULL DEFAULT 0,
ADD `azure` BIT NOT NULL DEFAULT 0;

UPDATE job_copy
SET 
    `sql` = CASE WHEN skills LIKE '%sql%' THEN 1 ELSE 0 END,
    `python` = CASE WHEN skills LIKE '%python%' THEN 1 ELSE 0 END,
    `r` = CASE WHEN skills LIKE '%r%' THEN 1 ELSE 0 END,
    `java` = CASE WHEN skills LIKE '%java%' THEN 1 ELSE 0 END,
    `aws` = CASE WHEN skills LIKE '%aws%' THEN 1 ELSE 0 END,
    `ML` = CASE WHEN skills LIKE '%ml%' THEN 1 ELSE 0 END,
    `tableau` = CASE WHEN skills LIKE '%tableau%' THEN 1 ELSE 0 END,
    `powerbi` = CASE WHEN skills LIKE '%powerbi%' THEN 1 ELSE 0 END,
    `excel` = CASE WHEN skills LIKE '%excel%' THEN 1 ELSE 0 END,
    `azure` = CASE WHEN skills LIKE '%azure%' THEN 1 ELSE 0 END;
    
# create a company_size_cat column
SELECT min(company_size) as min_comp_size,
		avg(company_size) as avg_company,
	max(company_size) as max_comp_size
from job_copy;

ALTER TABLE job_copy
ADD COLUMN company_size_cat varchar(40);

UPDATE job_copy
SET company_size_cat = CASE
    WHEN company_size > 0 AND company_size <= 5000 THEN 'less than 5000 employees'
    WHEN company_size > 5000 AND company_size <= 10000 THEN '5000-10000 employees'
    WHEN company_size > 10000 AND company_size <= 30000 THEN '10000-30000 employees'
    WHEN company_size > 30000 AND company_size <= 50000 THEN '30000-50000 employees'
    WHEN company_size > 50000 AND company_size <= 100000 THEN '50000-100000 employees'
    WHEN company_size > 100000 THEN 'more than 100000 employees'
    ELSE NULL
END;
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 5. Text-to-numeric transformation (post_date)
ALTER TABLE job_copy
ADD column post_days INT;

UPDATE job_copy
SET post_days = CASE
	WHEN LOWER(post_date) LIKE '%hour%' 
    THEN 0
    WHEN LOWER(post_date) LIKE '%a day%'
    THEN 1
    WHEN LOWER(post_date) LIKE '%a month%'
    THEN 30
    WHEN LOWER(post_date) LIKE '%a year%'
    THEN 365
    WHEN LOWER(post_date) LIKE '%day%' 
    THEN CAST(SUBSTRING_INDEX(post_date, ' ', 1) as UNSIGNED)
    WHEN LOWER(post_date) LIKE '%month%'
    THEN CAST(SUBSTRING_INDEX(post_date, ' ',1) as UNSIGNED) * 30
    WHEN LOWER(post_date) LIKE '%year%'
    THEN CAST(SUBSTRING_INDEX(post_date, ' ',1) as UNSIGNED) * 365
    ELSE NULL
END;

# remove old posts
DELETE
FROM job_copy
WHERE post_days > 365;
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 6. Remove unnecessary columns
ALTER TABLE job_copy
DROP COLUMN rn;

ALTER TABLE job_copy
DROP COLUMN post_date;

ALTER TABLE job_copy
DROP COLUMN company_size;
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 7. Missing value imputation
# check missing value
SELECT 
    COUNT(*) AS total_rows,

    SUM(job_title IS NULL OR job_title = '') AS job_title_null,
    SUM(seniority_level IS NULL OR seniority_level = '') AS seniority_level_null,
    SUM(`status` IS NULL OR `status` = '') AS status_null,
    SUM(company IS NULL OR company = '') AS company_null,
    SUM(location IS NULL OR location = '') AS location_null,
    SUM(hq_state IS NULL OR hq_state = '') AS hq_state_null,
    SUM(industry IS NULL OR industry = '') AS industry_null,
    SUM(ownership IS NULL OR ownership = '') AS ownership_null,
    SUM(revenue IS NULL OR revenue = '') AS revenue_null,
    SUM(salary IS NULL OR salary = '') AS salary_null,
    SUM(skills IS NULL OR skills = '') AS skills_null,
    SUM(hq_country IS NULL OR hq_country = '') AS hq_country_null,
    SUM(min_salary IS NULL OR min_salary = '') AS min_salary_null,
    SUM(max_salary IS NULL OR max_salary = '') AS max_salary_null,
    SUM(avg_salary IS NULL OR avg_salary = '') AS avg_salary_null,
    SUM(post_days IS NULL OR post_days = '') AS post_days_null,
    SUM(company_size_cat IS NULL OR company_size_cat = '') AS company_size_cat_null,
    SUM(`sql` IS NULL OR `sql` = '') AS sql_null,
    SUM(python IS NULL OR python = '') AS python_null,
    SUM(`r` IS NULL OR `r` = '') AS r_null,
    SUM(java IS NULL OR java = '') AS java_null,
    SUM(aws IS NULL OR aws = '') AS aws_null,
    SUM(ML IS NULL OR ML = '') AS ml_null,
    SUM(tableau IS NULL OR tableau = '') AS tableau_null,
    SUM(powerbi IS NULL OR powerbi = '') AS powerbi_null,
    SUM(excel IS NULL OR excel = '') AS excel_null,
    SUM(azure IS NULL OR azure = '') AS azure_null
FROM job_copy;

# Remove nulls only in job_title column, and keep the rest for analysis
DELETE
FROM job_copy
WHERE job_title IS NULL OR job_title = '';

# standardizing null values for all columns
UPDATE job_copy
# If it is categorical variable is null > then 'Not provided'
SET seniority_level = CASE WHEN seniority_level IS NULL OR seniority_level = '' THEN 'Not provided' ELSE seniority_level END,
	`status` = CASE WHEN `status` IS NULL OR `status` = '' THEN 'Not provided' ELSE `status` END,
    ownership = CASE WHEN ownership IS NULL OR ownership = '' THEN 'Not provided' ELSE ownership END,
    hq_country = CASE WHEN hq_country IS NULL OR hq_country = '' THEN 'Not provided' ELSE hq_country END,
    company_size_cat = CASE WHEN company_size_cat IS NULL OR company_size_cat = '' THEN 'Not provided' ELSE company_size_cat END,
    # If it is numerical variable is null > then NULL
    post_days = CASE WHEN post_days IS NULL OR post_days = '' THEN NULL ELSE post_days END;

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 8. Relocate columns
ALTER TABLE job_copy
MODIFY hq_country text AFTER hq_state;

ALTER TABLE job_copy
MODIFY post_days INT AFTER company;

ALTER TABLE job_copy
MODIFY company_size_cat text AFTER company; 

ALTER TABLE job_copy
MODIFY min_salary INT AFTER salary,
MODIFY max_salary INT AFTER min_salary, 
MODIFY avg_salary INT AFTER max_salary;
    
select *
from job_copy;


# -----------------------
-- Data Analysis
# -----------------------
# 1. Which seniority levels are most common? (junior, mid, senior, etc.)
SELECT seniority_level, count(*) as count
FROM job_copy
GROUP BY seniority_level
ORDER BY count(*) DESC;

# 2. Which locations / countries have the highest number of job postings?
SELECT hq_state, hq_country, count(*) as count
FROM job_copy
GROUP BY hq_state, hq_country
ORDER BY count(*) DESC;

# 3. What is the distribution of jobs by work mode (Remote, Hybrid, On-site)?
SELECT `status`, count(*) as job_count, ROUND(COUNT(*) * 100 / (SELECT COUNT(*) FROM job_copy), 2) AS percentage
from job_copy
GROUP BY `status`
ORDER BY count(*) DESC;

# 4. How does company size correlate with number of job openings?
SELECT company_size_cat, count(*) job_count
FROM job_copy
GROUP BY company_size_cat
ORDER BY count(*) DESC;

# 5. What are the top 3 skills required across all jobs?
SELECT skill, total_jobs
FROM (
    SELECT 'SQL' AS skill, SUM(`sql`) AS total_jobs
    FROM job_copy
    UNION ALL
    SELECT 'Python', SUM(`python`)
    FROM job_copy
    UNION ALL
    SELECT 'R', SUM(`r`)
    FROM job_copy
    UNION ALL
    SELECT 'Java', SUM(`java`)
    FROM job_copy
    UNION ALL
    SELECT 'AWS', SUM(`aws`)
    FROM job_copy
    UNION ALL
    SELECT 'ML', SUM(`ML`)
    FROM job_copy
    UNION ALL
    SELECT 'Tableau', SUM(`tableau`)
    FROM job_copy
    UNION ALL
    SELECT 'PowerBI', SUM(`powerbi`)
    FROM job_copy
    UNION ALL
    SELECT 'Excel', SUM(`excel`)
    FROM job_copy
    UNION ALL
    SELECT 'Azure', SUM(`azure`)
    FROM job_copy
) AS skills_count
ORDER BY total_jobs DESC
LIMIT 3;

# 6. Which skills are most requested for senior-level positions?
SELECT skill, job_count
FROM (
    SELECT 'SQL' AS skill, SUM(`sql`) job_count
    FROM job_copy WHERE seniority_level = 'senior'
    UNION ALL
    SELECT 'Python', SUM(`python`)
    FROM job_copy WHERE seniority_level = 'senior'
    UNION ALL
    SELECT 'R', SUM(`r`)
    FROM job_copy WHERE seniority_level = 'senior'
    UNION ALL
    SELECT 'Java', SUM(`java`)
    FROM job_copy WHERE seniority_level = 'senior'
    UNION ALL
    SELECT 'AWS', SUM(`aws`)
    FROM job_copy WHERE seniority_level = 'senior'
    UNION ALL
    SELECT 'ML', SUM(`ML`)
    FROM job_copy WHERE seniority_level = 'senior'
    UNION ALL
    SELECT 'Tableau', SUM(`tableau`)
    FROM job_copy WHERE seniority_level = 'senior'
    UNION ALL
    SELECT 'PowerBI', SUM(`powerbi`)
    FROM job_copy WHERE seniority_level = 'senior'
    UNION ALL
    SELECT 'Excel', SUM(`excel`)
    FROM job_copy WHERE seniority_level = 'senior'
    UNION ALL
    SELECT 'Azure', SUM(`azure`)
    FROM job_copy WHERE seniority_level = 'senior'
    ) AS t
ORDER BY job_count DESC
LIMIT 1;

# 7. How does skill demand vary by company size?
SELECT company_size_cat as company_size, skill, total_job
FROM (
    SELECT company_size_cat, 'SQL' AS skill, SUM(`sql`) AS total_job
    FROM job_copy
    WHERE company_size_cat != 'Not Provided'
    GROUP BY company_size_cat
    UNION ALL
    SELECT company_size_cat, 'Python', SUM(`python`)
    FROM job_copy
    WHERE company_size_cat != 'Not Provided'
    GROUP BY company_size_cat
    UNION ALL
    SELECT company_size_cat, 'R', SUM(`r`)
    FROM job_copy
    WHERE company_size_cat != 'Not Provided'
    GROUP BY company_size_cat
    UNION ALL
    SELECT company_size_cat, 'Java', SUM(`java`)
    FROM job_copy
    WHERE company_size_cat != 'Not Provided'
    GROUP BY company_size_cat
    UNION ALL
    SELECT company_size_cat, 'AWS', SUM(`aws`)
    FROM job_copy
    WHERE company_size_cat != 'Not Provided'
    GROUP BY company_size_cat
    UNION ALL
    SELECT company_size_cat, 'ML', SUM(`ML`)
    FROM job_copy
    WHERE company_size_cat != 'Not Provided'
    GROUP BY company_size_cat
    UNION ALL
    SELECT company_size_cat, 'Tableau', SUM(`tableau`)
    FROM job_copy
    WHERE company_size_cat != 'Not Provided'
    GROUP BY company_size_cat
    UNION ALL
    SELECT company_size_cat, 'PowerBI', SUM(`powerbi`)
    FROM job_copy
    WHERE company_size_cat != 'Not Provided'
    GROUP BY company_size_cat
    UNION ALL
    SELECT company_size_cat, 'Excel', SUM(`excel`)
    FROM job_copy
    WHERE company_size_cat != 'Not Provided'
    GROUP BY company_size_cat
    UNION ALL
    SELECT company_size_cat, 'Azure', SUM(`azure`)
    FROM job_copy
    WHERE company_size_cat != 'Not Provided'
    GROUP BY company_size_cat
) AS skills_count
ORDER BY company_size_cat DESC, total_job DESC;

# 8. What is the average, min, max salary by seniority level?
SELECT seniority_level, ROUND(avg(avg_salary), 2) as average_salary
FROM job_copy
WHERE seniority_level != 'Not provided'
GROUP BY seniority_level
ORDER BY avg(avg_salary) DESC;

# 9. Are remote jobs paid differently than on-site jobs?
SELECT `status`, min(min_salary) as minSalary, max(max_salary) as maxSalary, avg(avg_salary) as avgSalary
FROM job_copy
WHERE `status` != 'Not provided'
GROUP BY `status`
ORDER BY avgSalary DESC;

# 10. Which locations and skills combinations pay the highest salaries?
SELECT job_title, seniority_level, location, skills, avg_salary
FROM job_copy
ORDER BY avg_salary DESC
LIMIT 1;

# 11. Do certain company types prefer certain seniorities or skills?
SELECT ownership, industry, company_size_cat,
		SUM(`sql`) as SQL_count,
        SUM(python) as Python_count,
        SUM(r) as R_count,
        SUM(java) as Java_count,
        SUM(aws) as AWS_count,
        SUM(ML) as ML_count,
        SUM(tableau) as Tableau_count,
        SUM(powerbi) as PowerBI_count,
        SUM(excel) as Excel_count,
        SUM(azure) as Azure_count
FROM job_copy
WHERE ownership != 'Not provided'
GROUP BY ownership, industry, company_size_cat
ORDER BY ownership, industry, company_size_cat;

# 12. How many jobs are posted in the last 7 / 30 / 90 days? (post_days)
SELECT post_days, count(*) as job_count
FROM job_copy
WHERE post_days <= 90
GROUP BY post_days
ORDER BY post_days;

# -----------------------------------------------------------------------------------------------------------------------------------------------------

    
    
    
    
    









