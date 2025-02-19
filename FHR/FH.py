import polars as pl
import glob
import os

Path = r"C:\Users\lbabbi\Downloads\Input_Output_Files_for_GCC_FH\Input_Output_Files_for_GCC_FH"
CSV_Files = [f for f in os.listdir(Path) if f.endswith(".xlsx")]

Frames = [pl.read_excel(os.path.join(Path, f), sheet_name="Output").select(pl.col(["marketdate", "infocode", "sedol", "isin", "Tot_PctShoutHld"])) for f in CSV_Files]

Combined_Frame = pl.concat(Frames)

Combined_Frame = Combined_Frame.with_columns(pl.col("Tot_PctShoutHld").cast(pl.Float32) / 100)

Combined_Frame.write_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\FHR\FH_QAD_GCC.parquet")