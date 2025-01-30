import polars as pl
import datetime
import os

# Import Global Variables
CN_Target_Percentage = float(os.getenv("CN_Target_Percentage"))
current_datetime = os.getenv("current_datetime")
GMSR_Upper_Buffer = float(os.getenv("GMSR_Upper_Buffer"))
GMSR_Lower_Buffer = float(os.getenv("GMSR_Lower_Buffer"))

# Select columns to read from the Parquets
Columns = ["Date", "Index_Symbol", "Index_Name", "Internal_Number", "ISIN", "SEDOL", "RIC", "Instrument_Name", 
           "Country", "Currency", "Exchange", "ICB", "Free_Float", "Capfactor"]

# Emerging Universe
Emerging = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\SWEACGV.parquet", columns=Columns).with_columns([
                            pl.col("Free_Float").cast(pl.Float64),
                            pl.col("Capfactor").cast(pl.Float64),
                            pl.col("Date").cast(pl.Date)
                            ])

Standard_Index = pl.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\Standard_Index_Security_Level_Shadows_CNTarget_{CN_Target_Percentage}_" + current_datetime + ".csv").with_columns(
    pl.col("Date").cast(pl.Date)
).rename({"Size": "Size_Standard", "Shadow_Company": "Shadow_Company_Standard"})

Large = pl.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\Large_Index_Security_Level_CNTarget_{CN_Target_Percentage}_" + current_datetime + ".csv").with_columns(
    pl.col("Date").cast(pl.Date)
).with_columns(pl.lit("Large").alias("Size"))

Large = Large.join(Standard_Index.select(pl.col(["Date", "Internal_Number", "Size_Standard", "Shadow_Company_Standard"])), on=["Date", "Internal_Number"], how="left").filter(pl.col("Shadow_Company_Standard") == False)
Large.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\Large_Index_Security_Level_CNTarget_{CN_Target_Percentage}_" + current_datetime + ".csv")

Mid = Standard_Index.join(Large.select(pl.col(["Date", "Internal_Number", "ISIN", "SEDOL"])), on=["Date", "Internal_Number"], how="anti").filter(pl.col("Shadow_Company_Standard") == False)

# Add SEDOL/ISIN
Mid = Mid.join(Emerging.select(pl.col(["Date", "Internal_Number", "ISIN", "SEDOL"])), on=["Date", "Internal_Number"], how="left")

Mid.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\Tests\Mid_Index_Security_Level_CNTarget_{CN_Target_Percentage}_" + current_datetime + ".csv")