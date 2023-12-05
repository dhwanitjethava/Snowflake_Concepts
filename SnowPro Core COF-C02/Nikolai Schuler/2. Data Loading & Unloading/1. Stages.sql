--Show all named stages (internal stage)
SHOW STAGES;

-- List files in user stage (internal stage)
LIST @~;

-- List files in user stage;
LIST @%LOAN_PAYMENT;

-- Create database
CREATE OR REPLACE DATABASE manage_db;

-- Create Schema
CREATE OR REPLACE SCHEMA external_stages;

--Creating external stage
CREATE OR REPLACE STAGE manage_db.external_stages.aws_stage
    URL = 's3://bucketsnowflakes3'
    CREDENTIALS = (AWS_KEY_ID = 'ABCD_DUMMY_ID' AWS_SECRET_KEY = '1234abcd_key');

--Description of external stage
DESC STAGE manage_db.external_stages.aws_stage; 

-- Alter external stage
ALTER STAGE aws_stage
    SET CREDENTIALS = (AWS_KEY_ID = 'XYZ_DUMMY_ID' AWS_SECRET_KEY = '987xyz');

-- Publicly accessible staging area  
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
    URL = 's3://bucketsnowflakes3';

-- List files in stage
LIST @aws_stage;
