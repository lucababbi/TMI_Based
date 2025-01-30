from doctest import ELLIPSIS_MARKER
from lib2to3.pgen2.token import PLUS
import re
from sqlite3 import Date
import sys
from tkinter.tix import Tree
from tokenize import Double
import pandas as pd
import numpy as np
import datetime as dt
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay
from datetime import datetime, timedelta
import calendar
import warnings
warnings.filterwarnings("ignore")
import xlsxwriter
import polars as pl

# Read the data
CountryCode = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\TMI_Based\Exchange_Securities\CountriesCodeISO.csv").to_pandas()
SQL = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\TMI_Based\Exchange_Securities\Exchange_Securities.parquet")
# Create Country_Code column
SQL = SQL.with_columns(
    pl.col("GEOLC").alias("Country_Code"),
).to_pandas()

def update_region_mapping(SQL, CountryCode): # Create the REGGC and REGLC columns
    region_map = {}

    for i, j in zip(CountryCode['region'], CountryCode['segments']):
        region_map[i] = j

    region_map = pd.DataFrame.from_dict(region_map, orient="index")
    region_map["Country_Code"] = region_map.index.values
    region_map = region_map.rename(columns={0: "region"})
    region_map = region_map.reset_index(drop=True)

    # Change temporarily the Country_Code column-name
    SQL = SQL.rename(columns = {"Country_Code": "Country_Code_Temp"})

    # Create REGLC colum
    # SQL.rename(columns={'Country_Code': 'Country_Code_Temp'}, inplace=True) - To validate if it needs to be commented
    SQL = pd.merge(SQL, region_map, left_on="GEOLC", right_on="Country_Code", how="left")
    SQL = SQL.rename(columns = {"region_y": "REGLC"})
    SQL = SQL.drop(columns = {"Country_Code"}) # Verify the column name

    # Create REGGC column   
    SQL = pd.merge(SQL, region_map, left_on="GEOGC", right_on="Country_Code", how="left")
    SQL = SQL.rename(columns = {"region": "REGGC"})
    SQL = SQL.drop(columns = {"Country_Code"})
    SQL = SQL.rename(columns = {"Country_Code_Temp": "Country_Code"}) # Verify the column name

    SQL = SQL.rename(columns={"region_x": "region"})
    SQL_Offshore = SQL.query("REGGC == 'Offshore' | REGLC == 'Offshore'")
    SQL = SQL.query("REGGC != 'Offshore' & REGLC != 'Offshore'")

    # Drop NaN values
    SQL = SQL.dropna(subset=["REGLC", "REGGC"])

    SQL['region_check'] = SQL.apply(lambda row: (row['REGLC'] in row['REGGC']) or (row['REGGC'] in row['REGLC']), axis=1)
    SQL = SQL[SQL['region_check']]
    SQL = SQL.drop('region_check', axis=1)

    SQL = SQL[~((SQL['REGLC'] == 'Europe') & (SQL['REGGC'] == 'Eastern Europe'))]
    SQL = SQL[~((SQL['REGLC'] == 'Eastern Europe') & (SQL['REGGC'] == 'Europe'))]
    SQL = SQL[~((SQL['REGLC'] == 'Asia') & (SQL['REGGC'] == 'AsiaPacific'))]
    SQL = SQL[~((SQL['REGLC'] == 'AsiaPacific') & (SQL['REGGC'] == 'Asia'))]  
      
    DataFrameCountry = [SQL, SQL_Offshore]
    NonEmptyDataFrame = [df for df in DataFrameCountry if not df.empty]

    SQL = pd.concat(NonEmptyDataFrame)
    return SQL

# Update the region mapping
SQL = pl.DataFrame(update_region_mapping(SQL, CountryCode))



print('Done.')