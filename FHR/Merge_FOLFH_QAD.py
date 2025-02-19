import polars as pl

FH = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\FHR\FH_QAD_GCC.parquet").with_columns(
    pl.col("infocode").cast(pl.Utf8)
    ).rename({"Tot_PctShoutHld": "FH"})
FOL = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\FHR\FOL_QAD_GCC.parquet").with_columns(
                    pl.when(pl.col("cutoff").str.contains(r"^\d{4}-\d{2}-\d{2}$"))  # YYYY-MM-DD format
                    .then(pl.col("cutoff").str.to_date("%Y-%m-%d", strict=False))
                    .otherwise(pl.col("cutoff").str.to_date("%m/%d/%Y", strict=False)),
                    pl.col("FOL").cast(pl.Float32))
FHFOL = FH.join(FOL.select(pl.col(["cutoff", "infocode", "FOL"])), left_on=["marketdate", "infocode"], right_on=["cutoff", "infocode"], how="left").with_columns(
    pl.col("FOL").fill_null(1))

FHFOL.write_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\FHR\FHFOL_QAD_GCC.parquet")