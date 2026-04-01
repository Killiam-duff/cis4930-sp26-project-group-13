# cis4930-sp26-project-group-13-project-2

## Group Members

* William Kilduff - FSUID: WRK24 - Github: Killiam-duff
* Ty Officer - FSUID: TRO23 - Github: officety01
* Jack Waite - FSUID: JRW24a - Github: SirCatolot

## Project Description

This project produces weather data for multiple cities using Open-Mateo api.
Our goal is to create an automated data pipeline that grabs local weather data 
for numerous cities and stores that data for future analysis and testing. These 
cities include; Tallahasse, Pensacola, and Navarre. These 3 cities were chosen
for theyre size within the panhandle of Florida. This data should provide an
accurate history of the data in the panhandle over a set time.

## API Source

API source: https://open-meteo.com/en/docs

## Data Pipeline Goals

1. Save collected data with clear column names in a CSV file.
2. print a success/error message for when the project runs to determine whether it was succeeful ot not.
3. Fetch the weather data for at least 3 different cities.
4. Create a simple log message for failed requests so API errors can be reviewed without stopping the pipeline.
5. read/extract weather data from the api and load it into a JSON.

## Data Output File

data/processed/weather_data.csv

## Example Run

python src/pipeline.py

