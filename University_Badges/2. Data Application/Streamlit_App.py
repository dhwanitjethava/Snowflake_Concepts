# This is Streamlit App Python Code by DHWANIT JETHAVA
# Link for Streamlit App : https://dhwanitjethava-first-streamlit-app-streamlit-app-5ilnag.streamlit.app/

# Required libraries
import streamlit
import requests
import pandas as pd
import snowflake.connector as sf
from urllib.error import URLError 

# Streamlit functions for displaying
streamlit.title("My Mom's New Healthy Diner")

streamlit.header('Breakfast Favourites')

streamlit.text('🥣 Omega 3 & Blueberry Oatmeal')
streamlit.text('🥗 Kale, Spinach & Rocket Smoothie')
streamlit.text('🐔 Hard-Boiled Free-Range Egg')
streamlit.text('🥑🍞 Avocado Toast')

streamlit.header('🍌🥭 Build Your Own Fruit Smoothie 🥝🍇')

# Read CSV file from S3 bucket
my_fruit_list = pd.read_csv("https://uni-lab-files.s3.us-west-2.amazonaws.com/dabw/fruit_macros.txt")

# Choose the Fruit name column as the Index
my_fruit_list = my_fruit_list.set_index('Fruit')

# Let's put a pick list here so they can pick the fruit they want to include
fruits_selected = streamlit.multiselect("Pick some fruits:", list(my_fruit_list.index))
fruits_to_show = my_fruit_list.loc[fruits_selected]

# Display the table on the app page
streamlit.dataframe(fruits_to_show)

# take the JSON version of the response and normalize it, output it the screen as table
def get_fruityvice_data(this_fruit_choice):
  fruityvice_response = requests.get("https://fruityvice.com/api/fruit/"+this_fruit_choice)
  fruityvice_normalized = pd.json_normalize(fruityvice_response.json())
  return fruityvice_normalized

# New section to display fruityvice API response
streamlit.header("Fruityvice Fruit Advice!")
try:
  fruit_choice = streamlit.text_input('What fruit would you like information about?')
  if not fruit_choice:
    streamlit.error("Please select a fruit to get information")
  else:
    fruityvice_data = get_fruityvice_data(fruit_choice)
    streamlit.dataframe(fruityvice_data)
except URLError as e:
  streamlit.error()

streamlit.header("View our fruit list - Add your favorites!")

# Get fruit list from Snowflake Table
def get_fruit_load_list():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("SELECT * FROM fruit_load_list")
    return my_cur.fetchall()  
  
if streamlit.button("Get fruit list"):
  my_cnx = sf.connect(**streamlit.secrets["snowflake"])
  my_data_rows = get_fruit_load_list()
  my_cnx.close()
  streamlit.dataframe(my_data_rows)

# Don't run anything past here while we debug
# streamlit.stop()

# Allow the user to add fruits on the list
def insert_row_snowflake(new_fruit):
  with my_cnx.cursor() as my_cur:
    my_cur.execute("INSERT INTO fruit_load_list values('" + new_fruit + "')")
    return ("Thanks for adding " + new_fruit)

# Adding row of fruits selected into Snowflake
add_my_fruit = streamlit.text_input('Add a fruit to the list')
if streamlit.button("Add a fruit to the list"):
  my_cnx = sf.connect(**streamlit.secrets["snowflake"])
  insert_fruit_response = insert_row_snowflake(add_my_fruit)
  my_cnx.close()
  streamlit.text(insert_fruit_response)