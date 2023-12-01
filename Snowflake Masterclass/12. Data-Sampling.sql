-- Data Sampling : It is the practice of analyzing a subset of all data
--                 in order to uncover the meaningful information in the larger data set.
-- Use-cases : Query development, data analysis, etc.
-- Faster & more cost efficient (use less compute resources)

-----------------------------------------------------------------------------------------------------------------------

-- Data Sampling Methods

-- Method 1 - ROW OR BERNOULLI method
--  - Includes each row with a probability of p percentage
--  - More 'randomness'
--  - Smaller tables
--  - Relatively slower than BLOCK sampling

-- Method 2 - BLOCK OR SYSTEM method
--  - Includes each block of rows with a probability of p percentage
--  - More effective processing
--  - Larger tables
--  - Relatively faster than ROW sampling

-- Note: Sampling method is optional. If no method is specified, the default is BERNOULLI.
-- Note: SAMPLE and TABLESAMPLE are synonymous and can be used interchangeably.
-- Note: For very large tables, the difference between the two methods should be negligible.

-- SEED: Specifies a seed value to make the sampling deterministic. Can be any integer between [0, 2147483647].
-- Note: If no seed is specified, SAMPLE generates different results when the same query is repeated.
-- Note: SEED and REPEATABLE are synonymous and can be used interchangeably.
-- Note: Sampling without a seed is often faster than sampling with a seed.
-- Note: Sampling with a seed is not supported on views or subqueries.
--       For example, the following query produces an error:
            SELECT * FROM (SELECT * FROM <table_name>) SAMPLE (1) SEED (99);
-----------------------------------------------------------------------------------------------------------------------

-- Run normal query and note execution time
SELECT * FROM <table_name>;

-- Run sampling query and note execution time
SELECT * FROM <table_name>
SAMPLE ROW (1) SEED(27);

-- Run sampling query and note execution time
SELECT * FROM <table_name>
SAMPLE BLOCK (10) SEED(40);

-----------------------------------------------------------------------------------------------------------------------