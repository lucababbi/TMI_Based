import polars as pl
import glob
import os

Path = r"C:\Users\lbabbi\Downloads\GCC_arpit_files_cutoff\GCC_arpit_files_cutoff"
CSV_Files = [f for f in os.listdir(Path) if f.endswith(".csv")]

Frames = [pl.scan_csv(os.path.join(Path, f)) for f in CSV_Files]

Combined_Frame = pl.concat(Frames).collect()

Combined_Frame = Combined_Frame.select(
    pl.col(["cutoff", "NumShrs", "FreeFloatPct", "FX", "FFMCAP_USD", "Full_MCAP_USD", "region",
    "infocode", "DsCmpyCpde", "Name", "isin"])
)

Combined_Frame.write_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Trading Days\GCC_Trading_Days.parquet")