# HoopStats

## TODO

### Scraping & Data

Scraping using the `basketball_reference_web_scraper` Python package or creating separate scraping functions.

1. **Create relational database schema**:
   - Design the database schema (e.g., schedule table, game table, player stats table, etc.) using [dbdiagram.io](https://dbdiagram.io/).
   
2. **Get every season's schedule**:
   - Fetch and store schedules from 1985 to the present in a database or JSON files.
   
3. **Scrape every game**:
   - Develop a robust scraping method, potentially by creating a dedicated class for this purpose.
   - Collect as much data as possible.

4. **Clean the database**:
   - Ensure data quality and consistency by cleaning the collected data.

### ML

1. **Predict NBA champion/play-in tournament teams**:
   - Identify the best machine learning model to predict which team will win the NBA championship or participate in the play-in tournament.

2. **Predict game outcomes and player stats**:
   - Develop a model to predict the outcome of games and player statistics, possibly using data from the current and previous seasons.
   - Optimize the models for maximum efficiency.

### App

- Create a Streamlit app to make all the data and predictions accessible.

### Automation

- **Automate the scraping and data cleaning**:
  - Use Airflow or Snowflake to automate the scraping process and database cleaning based on the schedule. Scrape the website after every game.
  
- **Batch process the ML models**:
  - Update the machine learning models with new processed data.
  
- **Update the Streamlit app automatically**:
  - Ensure the app is updated in real-time with the latest data and predictions.
