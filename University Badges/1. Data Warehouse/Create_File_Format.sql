create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW 
    TYPE = 'CSV' --csv is used for any flat file like tsv, pipe-separated, etc.
    FIELD_DELIMITER = '|' --pipes as column separators
    SKIP_HEADER = 1; --one header row to skip
    

create file format garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV'--csv for comma separated files
    SKIP_HEADER = 1 --one header row to skip
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'; --this means that some values will be wrapped in double-quotes but they have commas in them.
