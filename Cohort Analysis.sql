/* Why Cohort Analysis? It is an analysis to understand the behavior of customers you do business with. 
You do this to see patterns and trends. A cohort is basically a group of people with something in common or common characteristics.
A cohort analysis is simply an analysis of several different cohorts to get a better understanding of behaviors, patterns, and trends. 
This project focuses on retention based analysis. Depending on your type of cohort you could have a time-based cohort, segment-based cohort, or a size-based cohort.
We are doing a Time-based Cohort where we are looking at the time a certain group of people purchased or did something on my site, and see their behavior after that first instance of activity
The data from the time based cohort will be used to do a retention based analysis
*/




/* Cleaning Data
--All tables were imported in camelcase from the csv which gave errors while queryng. They had to be renamed to lowercase
ALTER TABLE retail 
RENAME COLUMN "InvoiceNo" TO invoice_no;
RENAME COLUMN "StockCode" to stock_code;
RENAME COLUMN "Description" to description;
RENAME COLUMN "Quantity" to quantity;
RENAME COLUMN "InvoiceDate" to invoice_date;
RENAME COLUMN "UnitPrice" to unit_price;
RENAME COLUMN "CustomerID" to customer_id;
RENAME COLUMN "Country" to country; */

-- Total Records = 541909
--select count(*) from retail;

-- Some records have null customer_id, and some Quantities have negative values 
--(mostly indicating they are returned items)
-- There are 135,080 values with no customer id
Select count(*) 
from retail
where customer_id isnull

-- Here we will be using records with only valid customer_id and that amounts to 406,829 records
-- We will put this information into a CTE, keep refining and cleaning the data until we get a dataset for a Temp Table
;WITH ONLINE_RETAIL AS
(
	SELECT *
	FROM RETAIL
	WHERE CUSTOMER_ID IS NOT NULL
), ONLINE_RETAIL_2 AS -- creating another CTE 
(
	SELECT *
	FROM ONLINE_RETAIL
	-- For our CTE, we also want to remove all records where Quantity and Unit Price > 0, as other values with negative unit prices are mostly returned items
	--This reduces our records from 406829, to 397884
	WHERE QUANTITY > 0
	AND UNIT_PRICE > 0 ), DUPLICATE_CHECK AS
(
	-- Time for a Duplicate Check
	SELECT *, ROW_NUMBER () OVER (PARTITION BY invoice_no, stock_code, quantity order by invoice_date) as dup_flag
	FROM ONLINE_RETAIL_2
)
--The data is now down to 392669 from 397884. The data is finally clean
-- We will now create a temp table
SELECT *
--INTO RETAIL_MAIN
FROM DUPLICATE_CHECK
WHERE DUP_FLAG = 1

-- CLEAN DATA
-- BEGIN COHORT ANALYSIS
SELECT *
FROM RETAIL_MAIN;

-- In creating a Cohort analysis you need some specific data points such as;
-- 1. Unique identifier for the group you want to make the analysis on - Customer_id
-- 2. Initial start date since we will be doing a retention analysis - First Invoice_date
-- 3. Revenue Data

SELECT CUSTOMER_ID,
	MIN(INVOICE_DATE) AS FIRST_PURCHASE_DATE,
	MAKE_DATE(EXTRACT(YEAR FROM MIN(INVOICE_DATE))::integer,
			  EXTRACT(MONTH FROM MIN(INVOICE_DATE))::integer, 1) AS COHORT_DATE
--INTO COHORT  -- creating another temp table
FROM RETAIL_MAIN
GROUP BY 1;

SELECT *
FROM COHORT;
--ORDER BY COHORT_DATE DESC;

-- Create Cohort Index
-- A cohort index is an integer representation of the number of months that has passed since the customers first engagement / purchase

SELECT *,
		(year_diff * 12 + month_diff + 1) as cohort_index
		-- COHORT INDEX FORMULA
--INTO COHORT_RETENTION
-- MOVE THE COHORT RETENTION DATA INTO A TEMP TABLE
FROM
	(SELECT *,
			(INVOICE_YEAR - COHORT_YEAR) AS YEAR_DIFF,
			(INVOICE_MONTH - COHORT_MONTH) AS MONTH_DIFF
	FROM
			(SELECT m.*, 
			 C.COHORT_DATE,
				   EXTRACT(year FROM m.invoice_date) AS invoice_year, 
				   EXTRACT(month FROM m.invoice_date) AS invoice_month,
				   EXTRACT(year FROM c.cohort_date) AS cohort_year,
				   EXTRACT(month FROM c.cohort_date) AS cohort_month
			FROM retail_main m
			LEFT JOIN cohort c ON m.customer_id = c.customer_id) AS MM) AS MMM
--WHERE (year_diff * 12 + month_diff + 1) > 3
--WHERE CUSTOMER_ID = 14733
--ORDER BY COHORT_INDEX DESC;
	
 
SELECT *
FROM RETAIL
WHERE CUSTOMER_ID = 14733
ORDER BY INVOICE_DATE;

SELECT Cohort_Date, cohort_index, COUNT(DISTINCT Customer_ID)
     FROM cohort_retention
     GROUP BY 1, 2
     ORDER BY 1, 2
SELECT DISTINCT cohort_index FROM cohort_retention ORDER BY 1

-- PIVOT DATA TO SEE THE COHORT TABLE USING POSTGRES CROSSTAB FUNCTION
SELECT *
--INTO COHORT_CROSSTAB
FROM crosstab(
    'SELECT Cohort_Date, cohort_index, COUNT(DISTINCT Customer_ID)
     FROM cohort_retention
     GROUP BY 1, 2
     ORDER BY 1, 2',
    'SELECT DISTINCT cohort_index FROM cohort_retention ORDER BY 1'
) AS pivot_table (
    Cohort_Date date,
    cohort_1 int,
    cohort_2 int,
    cohort_3 int,
    cohort_4 int,
    cohort_5 int,
    cohort_6 int,
    cohort_7 int,
    cohort_8 int,
    cohort_9 int,
    cohort_10 int,
    cohort_11 int,
    cohort_12 int,
    cohort_13 int
);
select *
from COHORT_CROSSTAB
order by 1;


-- HERE, WE WANT TO CREATE A TABLE WHERE WE CAN SEE THE PERCENTAGE OF THE CUSTOMERS THAT CAME BACK  AFTER THEIR FIRST PURCHASE
SELECT Cohort_Date,
    (1.0 * "cohort_1" / "cohort_1") * 100 AS "1",
    1.0 * "cohort_2" / "cohort_1" * 100 AS "2",
    1.0 * "cohort_3" / "cohort_1" * 100 AS "3",
    1.0 * "cohort_4" / "cohort_1" * 100 AS "4",
    1.0 * "cohort_5" / "cohort_1" * 100 AS "5",
    1.0 * "cohort_6" / "cohort_1" * 100 AS "6",
    1.0 * "cohort_7" / "cohort_1" * 100 AS "7",
    1.0 * "cohort_8" / "cohort_1" * 100 AS "8",
    1.0 * "cohort_9" / "cohort_1" * 100 AS "9",
    1.0 * "cohort_10" / "cohort_1" * 100 AS "10",
    1.0 * "cohort_11" / "cohort_1" * 100 AS "11",
    1.0 * "cohort_12" / "cohort_1" * 100 AS "12",
    1.0 * "cohort_13" / "cohort_1" * 100 AS "13"
FROM cohort_crosstab
ORDER BY Cohort_Date;








































select customer_id,
		to_char(cohort_date, 'Mon-YY') as sales_date,
		sum(unit_price::int) as amount
from cohort_retention
group by customer_id, to_char(cohort_date, 'Mon-YY')
order by 1;


SELECT *
from crosstab('select customer_id as customer,
				to_char(cohort_date, ''Mon-YY'') as sales_date,
				sum(unit_price::int) as amount
				from cohort_retention
			  	group by customer_id, to_char(cohort_date, ''Mon-YY'')
				order by 1')
	as (customer integer,  Jan_10 bigint, Feb_10 bigint, Mar_10 bigint, Apr_10 bigint, May_10 bigint, Jun_10 bigint, Jul_10 bigint,
  		 Aug_10 bigint, Sep_10 bigint, Oct_10 bigint, Nov_10 bigint, Dec_10 bigint)


