-- Create table 'ROOT_DEPTH'
CREATE OR REPLACE TABLE GARDEN_PLANTS.VEGGIES.ROOT_DEPTH (
   ROOT_DEPTH_ID number(1), 
   ROOT_DEPTH_CODE text(1), 
   ROOT_DEPTH_NAME text(7), 
   UNIT_OF_MEASURE text(2),
   RANGE_MIN number(2),
   RANGE_MAX number(2)
   );

-- Insert Data into 'ROOT_DEPTH'
INSERT INTO ROOT_DEPTH (ROOT_DEPTH_ID, ROOT_DEPTH_CODE, ROOT_DEPTH_NAME, UNIT_OF_MEASURE, RANGE_MIN, RANGE_MAX)
VALUES
	(1,'S','Shallow','cm',30,45);

SELECT * FROM ROOT_DEPTH;

-- Insert Data into 'ROOT_DEPTH'
INSERT INTO ROOT_DEPTH (ROOT_DEPTH_ID, ROOT_DEPTH_CODE, ROOT_DEPTH_NAME, UNIT_OF_MEASURE, RANGE_MIN, RANGE_MAX)
VALUES
	(2,'M','Medium','cm',45,60),
	(3,'D','Deep','cm',60,90);

SELECT * FROM ROOT_DEPTH;
