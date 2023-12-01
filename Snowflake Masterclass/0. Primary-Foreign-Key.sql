-- Primary Key & Foreign Key constraints

CREATE TABLE <parent_table> (
    parent_id INT PRIMARY KEY,
    parent_name STRING
    );

CREATE TABLE <child_table> (
    child_id INT PRIMARY KEY,
    child_name STRING,
    parent_id INT,
    FOREIGN KEY (parent_id) REFERENCES <parent_table>(parent_id)
    );

-----------------------------------------------------------------------------------------------------------------------
