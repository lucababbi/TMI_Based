import polars as pl
import glob
import os

Path = r"C:\Users\lbabbi\Downloads\Universe_as_of_Cutoff_Date\Universe_as_of_Cutoff_Date"
CSV_Files = [f for f in os.listdir(Path) if f.endswith(".csv")]

Frames = [pl.read_csv(os.path.join(Path, f), infer_schema=False) for f in CSV_Files]

Combined_Frame = pl.concat(Frames)

FX = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Exchange_Securities\FX_Historical_with_Dec24.parquet").with_columns(
    pl.col("Cutoff").cast(pl.Date)
)

Combined_Frame = Combined_Frame.with_columns(
    pl.col("cutoff").cast(pl.Date),
    pl.col("NumShrs").cast(pl.Float32),
    pl.col("FreeFloatPct").cast(pl.Float32),
    pl.col("Close_EUR").cast(pl.Float32)
).select(pl.col(["cutoff", "NumShrs", "FreeFloatPct", "region", "infocode", "DsCmpyCode", "Name", "isin", "ISOCurrCode", "Close_EUR"])).with_columns(pl.lit("EUR").alias("Currency_Close_EUR"))

Combined_Frame = Combined_Frame.join(FX, left_on=["cutoff", "Currency_Close_EUR"], right_on=["Cutoff", "Currency"], how="left")

Combined_Frame = Combined_Frame.with_columns(
    (pl.col("NumShrs") * pl.col("FreeFloatPct") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Close_EUR")).alias("FFMCAP_USD"),
    (pl.col("NumShrs") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Close_EUR")).alias("Full_MCAP_USD")
).rename({"FX_local_to_Index_Currency_Cutoff": "FX"})

Combined_Frame.write_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Exchange_Securities\Exchange_Securities_Final.parquet")