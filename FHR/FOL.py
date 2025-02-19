import polars as pl
import glob
import os

# Define the correct schema for all files
schema = {
    "cutoff": pl.Utf8,
    "infocode": pl.Utf8,
    "sedol7": pl.Utf8,
    "isin": pl.Utf8,
    "FOL": pl.Utf8,
}

Path = r"C:\Users\lbabbi\Downloads\Output_Files_for_GCC_FOL\Output_Files_for_GCC_FOL"
CSV_Files = [f for f in os.listdir(Path) if f.endswith(".csv")]

Frames = [pl.read_csv(os.path.join(Path, f), infer_schema=False).select(pl.col(["cutoff", "infocode", "sedol7", "isin", "FOL"])) for f in CSV_Files]

Combined_Frame = pl.concat(Frames)

Combined_Frame = Combined_Frame.select(
    pl.col(["cutoff", "infocode", "sedol7", "isin", "FOL"])
)

Combined_Frame.write_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\FHR\FOL_QAD_GCC.parquet")