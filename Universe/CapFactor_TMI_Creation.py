import polars as pl

# Read TMI Universe FreeFloat as of Review Date
TMI_FF=pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\TMI_Based\Universe\Input_Code\Final_Universe_TMI.parquet").with_columns(
    pl.col("Date").cast(pl.Date),
    pl.col("Free_Float").cast(pl.Float32)
).select(pl.col(["Date", "Internal_Number", "Free_Float"]))

# Read FOL-FH as of Cutoff from QAD with STOXXID
FHR=pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\FHR\FHFOL_QAD_Final.parquet").filter(pl.col("StoxxId").is_not_null())

# Set FOL = 0 to 100%
FHR = FHR.with_columns(
    pl.when(pl.col("FOL") == 0)
    .then(1)  # Set to 1 if FOL is 0
    .otherwise(pl.col("FOL"))  # Keep original value otherwise
    .alias("FOL")  # Ensure column is replaced
)

# Read Dates
Dates=pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Dates\Review_Date-QUARTERLY.csv").with_columns(
    pl.col("Cutoff").str.strptime(pl.Date, "%m/%d/%Y", strict=False),
    pl.col("Review").str.strptime(pl.Date, "%m/%d/%Y", strict=False))

# Add Review Date to FHR Frame
FHR = FHR.join(Dates, left_on="marketdate", right_on="Cutoff", how="left")

# Add the information to TMI_FF
TMI_FF = TMI_FF.join(FHR.select(pl.col(["Review", "StoxxId", "FH", "FOL"])), left_on=["Date", "Internal_Number"], right_on=["Review", "StoxxId"], how="left")

# Drop duplicates given by the structure of QAD Table
TMI_FF=TMI_FF.unique(subset=["Date", "Internal_Number"], keep="first")

# Adjustment for CHINA A TMI
CN_A=pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Universe\TW1GV-SXCNATGV-SXCNPTGV.parquet").filter(
    pl.col("Index_Symbol")=="SXCNPTGV").with_columns(
    pl.col("Date").cast(pl.Date)
).select(pl.col(["Date", "Internal_Number", "Index_Symbol"]))

# Merge with TMI_FF
TMI_FF = TMI_FF.join(CN_A, on=["Date", "Internal_Number"], how="left")

# Calculat the FOR
TMI_FF = TMI_FF.with_columns(
    pl.when(pl.col("Index_Symbol").is_null())
    .then(
        pl.min_horizontal([
            pl.col("Free_Float"), 
            pl.max_horizontal([pl.lit(0), pl.col("FOL") - pl.col("FH")])
        ])
    )
    .otherwise(
        pl.min_horizontal([
            pl.col("Free_Float") * 0.20,  # Apply multiplication to each column
            pl.max_horizontal([pl.lit(0), (pl.col("FOL") - pl.col("FH")) * 0.20])
        ])
    )
    .alias("FOR_Adjusted_Free_Float")
)

# Calculate the CapFactor
TMI_FF = TMI_FF.with_columns(
    (pl.col("FOR_Adjusted_Free_Float") / pl.col("Free_Float")).alias("CapFactor_TMI")
)

# Store the results
TMI_FF.write_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Universe\CapFactor_Calculated_TMI.parquet")