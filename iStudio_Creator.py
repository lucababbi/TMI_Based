import polars as pl
import pandas as pd
from datetime import datetime
import datetime
import os

CN_Target_Percentage = float(os.getenv("CN_Target_Percentage"))
current_datetime = os.getenv("current_datetime")
GMSR_Upper_Buffer = float(os.getenv("GMSR_Upper_Buffer"))
GMSR_Lower_Buffer = float(os.getenv("GMSR_Lower_Buffer"))

# Capfactor from SWACALLCAP
CapFactor = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\Capfactor_SWACALLCAP_Extended.csv").with_columns(
    pl.col("Date").cast(pl.Date),
).select(pl.col(["Date", "Internal_Number", "Capfactor"])).filter(pl.col("Date") < datetime.date(2024,6,24)).to_pandas()

# Capfactor adjusted for JUN-SEP 2024
FreeFloat_TMI = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\Capfactor_TMI.csv", separator=",",
                infer_schema=False).with_columns(
                pl.col("validDate").str.strptime(pl.Date, "%Y%m%d").alias("validDate"),
                pl.col("freeFloat").cast(pl.Float64).alias("Free_Float_TMI")
)

# Select columns to read from the Parquets
Columns = ["Date", "Index_Symbol", "Index_Name", "Internal_Number", "ISIN", "SEDOL", "RIC", "Instrument_Name", 
           "Country", "Currency", "Exchange", "ICB", "Free_Float", "Capfactor", "Shares", "Close_unadjusted_local", "FX_local_to_Index_Currency"]

FreeFloat_SW = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\SWEACGV.parquet", columns=Columns).with_columns([
                            pl.col("Free_Float").cast(pl.Float64),
                            pl.col("Capfactor").cast(pl.Float64),
                            pl.col("Shares").cast(pl.Float64),
                            pl.col("Close_unadjusted_local").cast(pl.Float64),
                            pl.col("FX_local_to_Index_Currency").cast(pl.Float64),
                            pl.col("Date").cast(pl.Date)
                            ]).select(pl.col(["Date", "Internal_Number", "Free_Float"])).filter(pl.col("Date") >= datetime.date(2024,6,24))

CapFactor_JUNSEP = FreeFloat_SW.join(FreeFloat_TMI.select(pl.col(["validDate", "stoxxId", "Free_Float_TMI"])), left_on=["Date", "Internal_Number"],
                                    right_on=["validDate", "stoxxId"], how="left").with_columns(
                                    (pl.col("Free_Float") / pl.col("Free_Float_TMI")).alias("Capfactor")
                                     ).select(pl.col(["Date", "Internal_Number", "Capfactor"])
                                     ).to_pandas()

# GCC Capfactor
GCC_Capfactor = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\GCC.parquet").with_columns(
    [
        pl.col("Date").cast(pl.Date),
        pl.col("Internal_Number").cast(pl.Utf8),
        pl.col("Capfactor").cast(pl.Float64)
    ]
).select(pl.col(["Date", "Internal_Number", "Capfactor"])).to_pandas()

CapFactor = pd.concat([CapFactor, CapFactor_JUNSEP, GCC_Capfactor])

CapFactor = pd.concat([CapFactor, CapFactor_JUNSEP])

# Create the iStudio input
Small_Index = pd.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\Small_Index_Security_Level_CNTarget_{CN_Target_Percentage}_{GMSR_Upper_Buffer}_{GMSR_Lower_Buffer}_" + current_datetime + ".csv", parse_dates=["Date"])

# Filter for needed columns
Frame = Small_Index[["Internal_Number", "SEDOL", "ISIN", "Date"]]

# Create weightFactor
Frame["weightFactor"] = 1

# Add CapFactor from SWACALLCAP
Frame = Frame.merge(CapFactor[["Date", "Internal_Number", "Capfactor"]], on=["Date", "Internal_Number"], how="left")

# Convert column Date
Frame["Date"] = Frame["Date"].dt.strftime('%Y%m%d')

# Renaming to convention
Frame = Frame.rename(columns={"Internal_Number": "STOXXID", "Date": "effectiveDate", "Capfactor": "capFactor"})

# Store the .CSV
# Get current date formatted as YYYYMMDD_HHMMSS
from datetime import datetime
current_datetime = datetime.today().strftime('%Y%m%d')

# Store the .CSV with version and timestamp
Frame.to_csv(
        os.path.join(
            r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests", 
            current_datetime + "_SMALL_ETF.csv"
        ), 
        index=False, 
        lineterminator="\n", 
        sep=";"
    )