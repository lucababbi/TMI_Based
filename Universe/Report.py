import os
import pandas as pd
import polars as pl

# Define the folder path
folder_path = r'C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\TMI_Based\Universe'

# List to hold dataframes
dataframes = []

# Iterate over all files in the folder
for file_name in os.listdir(folder_path):
    if file_name.endswith('.parquet'):
        file_path = os.path.join(folder_path, file_name)
        df = pd.read_parquet(file_path)
        dataframes.append(df)

# Concatenate all dataframes into a single dataframe
combined_df = pd.concat(dataframes, ignore_index=True)

# Keep only relevant columns
combined_df = combined_df[["Close_Day", "Date", "Index_Symbol", "Index_Name", "Index_Currency", "Index_Type", "Internal_Number", "ISIN", 
                           "SEDOL", "RIC", "Instrument_Name", "Country", "Currency", "Exchange", "Free_Float_Close", "Free_Float", "Capfactor", 
                           "Mcap_Units_Index_Currency", "Weight", "ICB"]]

# Convert all columns to String
combined_df = pl.DataFrame(combined_df.astype(str))

# Display the combined dataframe
print(combined_df)

# Fill missing Date value using Close_Day
combined_df = combined_df.with_columns(
    pl.when(pl.col("Date") == "nan")
    .then(pl.col("Close_Day"))
    .otherwise(pl.col("Date"))
    .alias("Date")
).drop("Close_Day")

# Fill missing Free_Float value using Free_Float_Close
combined_df = combined_df.with_columns(
    pl.when(pl.col("Free_Float") == "nan")
    .then(pl.col("Free_Float_Close"))
    .otherwise(pl.col("Free_Float"))
    .alias("Free_Float")
).drop("Free_Float_Close")

# Fill missing Index_Symbol value using Index_Name
combined_df = combined_df.with_columns(
    pl.when(pl.col("Index_Symbol") == "")
    .then(pl.col("Index_Name"))
    .otherwise(pl.col("Index_Symbol"))
    .alias("Index_Symbol")
).drop("Index_Name")

# Fix the different Index_Symbol names
replacement_dict = {
    "SXGDRGV": "SXGDRGV",
    "SGEMGV": "SGEMGV",
    "China GMI V7": "SGCNMGV",
    "TW1GV": "TW1GV",
    "GCC countries 22": "SXGCCGV",
    "SGCNMGV": "SGCNMGV",
    "SXCNPTGV": "SXCNPTGV",
    "SXCNATGV": "SXCNATGV",
    "SXGCCGV": "SXGCCGV",
    "STOXX GMI EUROPE PLUSUK NoMissingSecurities": "SGEMGV"
}

combined_df = combined_df.with_columns(
    pl.when(pl.col("Index_Symbol") == "China GMI V7").then(pl.lit("SGCNMGV"))
    .when(pl.col("Index_Symbol") == "GCC countries 22").then(pl.lit("SXGCCGV"))
    .when(pl.col("Index_Symbol") == "STOXX GMI EUROPE PLUSUK NoMissingSecurities").then(pl.lit("SGEMGV"))
    .otherwise(pl.col("Index_Symbol"))
    .alias("Index_Symbol")
)

# Add Cutoff information
Full_Dates = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\TMI_Based\Dates\Review_Date-QUARTERLY.csv").with_columns(
    pl.col("Review").str.strptime(pl.Date, format="%m/%d/%Y"),
    pl.col("Cutoff").str.strptime(pl.Date, format="%m/%d/%Y")
)

# Convert combined_df Date column to Date type
combined_df = combined_df.with_columns(
    pl.when(pl.col("Date").str.contains(" "))
    .then(pl.col("Date").str.strptime(pl.Date, format="%Y-%m-%d %H:%M:%S", strict=False))
    .otherwise(pl.col("Date").str.strptime(pl.Date, format="%Y-%m-%d", strict=False))
    .alias("Date")
)

# Add Cutoff information to the combined_df
combined_df = combined_df.join(Full_Dates, left_on=["Date"], right_on=["Review"], how="left")

combined_df = combined_df.drop("Index_Currency", "Index_Type", "Weight")

# Add information for the 12M Turnover
Turnover_12M = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\TMI_Based\Turnover\Turnover_12M.parquet").select(
    pl.col(["vd", "stoxxId", "field", "value"])
).with_columns(
    pl.col("vd").cast(pl.Utf8)
)

# Convert vd into a Date type
Turnover_12M = Turnover_12M.with_columns(
    pl.col("vd").str.strptime(pl.Date, format="%Y%m%d")
).filter(pl.col("field")=="TurnoverRatioFO")

# Add information of Turnover_12M to the combined_df
combined_df = combined_df.join(Turnover_12M, left_on=["Cutoff", "Internal_Number"], right_on=["vd", "stoxxId"], how="left")

# Fill missing values
combined_df = combined_df.with_columns(
    pl.when(pl.col("value").is_null())
    .then(pl.lit("0"))
    .otherwise(pl.col("value"))
    .alias("value")
)

# Fill missing values
combined_df = combined_df.with_columns(
    pl.when(pl.col("field") == "")
    .then(pl.lit("TurnoverRatioFO"))
    .otherwise(pl.col("field"))
    .alias("field")
)

# Cast value for Turnover12M
combined_df = combined_df.with_columns(
    pl.col("value").cast(pl.Float64),
    pl.col("Mcap_Units_Index_Currency").cast(pl.Float64)
)

# Group by Date and calculate the total Mcap_Units_Index_Currency for each Date
total_mcap_per_date = combined_df.group_by("Date").agg(
    pl.col("Mcap_Units_Index_Currency").sum().alias("Total_Mcap")
)

# Add total_mcap_per_date to the combined_df
combined_df = combined_df.join(total_mcap_per_date, on="Date", how="left")

# Calculate the Weight for each stock
combined_df = combined_df.with_columns(
    (pl.col("Mcap_Units_Index_Currency") / pl.col("Total_Mcap")).alias("Weight")
)

# Store the Final Frame
combined_df.drop(["field", "value", "Total_Mcap", "Weight"]).write_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\TMI_Based\Universe\Input_Code\Final_Universe_TMI.parquet")