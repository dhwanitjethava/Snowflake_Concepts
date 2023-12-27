create or replace table vegetable_details (
  plant_name varchar(25),
  root_depth_code varchar(1)    
  );

insert into vegetable_details (plant_name, root_depth_code)
values
  ('Artichoke', 'D'),
  ('Arugula', 'S'),
  ('Asparagus', 'D'),
  ('Beans bush', 'M'),
  ('Beans lima (bush)', 'D'),
  ('Beans pole', 'M'),
  ('Beets', 'M'),
  ('Broccoli', 'S'),
  ('Brussel sprouts', 'S'),
  ('Cabbage', 'S'),
  ('Carrots', 'M'),
  ('Cauliflower', 'S'),
  ('Celery','S'),
  ('Chard', 'M'),
  ('Edamame', 'M'),
  ('Corn', 'S'),
  ('Cucumber', 'M'),
  ('Eggplant', 'M'),
  ('Endive', 'S'),
  ('Garlic', 'S'),
  ('Kale', 'M');

insert into vegetable_details (plant_name, root_depth_code)
values
  ('Kohlrabi','S'),
  ('Leeks','S'),
  ('Lettuce','S'),
  ('Okra', 'D'),
  ('Onions','S'),
  ('Parsnips','D'),
  ('Peas','M'),
  ('Peppers hot','M'),
  ('Peppers bell','M'),
  ('Potatoes','S'),
  ('Pumpkin','D'),
  ('Radishes','S'),
  ('Spinach','S'),
  ('Rutabaga','M'),
  ('Spinach','D'),
  ('Squash summer','M'),
  ('Squash winter','D'),
  ('Sweet potato','D'),
  ('Tomatoes','D'),
  ('Turnips','M'),
  ('Zucchini','D');

select * from vegetable_details;

select * from vegetable_details
where plant_name = 'Spinach';

-- Remove only the Spinach row with "D" in the ROOT_DEPTH_CODE column
delete from vegetable_details
where plant_name = 'Spinach' and root_depth_code = 'D';

select * from root_depth;
