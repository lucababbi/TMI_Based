import polars as pl
import datetime

FOLFH = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\FHR\FHFOL_QAD_GCC.parquet").with_columns(
                               pl.col("marketdate").cast(pl.Date),
                               pl.col("infocode").cast(pl.Utf8)
                           )

STOXXID = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Trading Days\Data_Luca.txt", infer_schema=False).with_columns(
    pl.col("vf").str.to_date("%Y%m%d"),
    pl.col("vt").str.to_date("%Y%m%d")
).with_columns(
    pl.when(pl.col("vt") == datetime.date(9999, 12, 30))  # Check for 9999-12-30
    .then(pl.date(2100, 12, 30))  # Replace with 2100-12-30
    .otherwise(pl.col("vt"))
    .alias("vt")
)

# Create an SQLContext and register tables
sql = pl.SQLContext()
sql.register("df_left", FOLFH)
sql.register("df_right", STOXXID)

filtered_df = sql.execute("""
    SELECT l.*, r.vf, r.vt, r.StoxxId
    FROM df_left AS l
    LEFT JOIN df_right AS r
    ON l.infocode = r.InfoCode
    WHERE (r.vf IS NULL AND r.vt IS NULL) OR 
          (l.marketdate BETWEEN r.vf AND r.vt)
""").collect()  # Collect to execute and get the result

missing_rows = filtered_df.join(FOLFH, on="infocode", how="anti")

filtered_df.write_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\FHR\FHFOL_QAD_GCC_Final.parquet")