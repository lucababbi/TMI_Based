import polars as pl
import datetime
from datetime import date
import os

# Import Global Variables
CN_Target_Percentage = float(os.getenv("CN_Target_Percentage"))
current_datetime = os.getenv("current_datetime")
GMSR_Upper_Buffer = float(os.getenv("GMSR_Upper_Buffer"))
GMSR_Lower_Buffer = float(os.getenv("GMSR_Lower_Buffer"))

AllCap = pl.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\AllCap_Index_Security_Level_{GMSR_Upper_Buffer}_{GMSR_Lower_Buffer}_" + current_datetime + "_NoShadow.csv").with_columns(
    pl.col("Date").cast(pl.Date)
)

# Output Frame
Output = pl.DataFrame(schema=AllCap.schema)

Standard = pl.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\Standard_Index_Security_Level_Shadows_CNTarget_{CN_Target_Percentage}_" + current_datetime + ".csv").with_columns(
    pl.col("Date").cast(pl.Date)
)

def China_A_Small_Removal(AllCap, Standard, date):

    # Slash the Standard_Index_Output_Code
    temp_Standard_Index = Standard.filter((pl.col("Date") == date) & (pl.col("Country") == "CN"))

    # Mark those Securities that are part of the Standard_Index
    TopPercentage_Securities = AllCap.with_columns(
        pl.col("Internal_Number").is_in(temp_Standard_Index.select(pl.col("Internal_Number"))).alias("CN_Standard")
    )

    if datetime.date(2019, 3, 18) <= date < datetime.date(2019, 9, 23):
        TopPercentage_Securities = TopPercentage_Securities.filter(
            ~(
                (pl.col("Country") == "CN") & 
                (
                    pl.col("Instrument_Name").str.contains("'A'") | 
                    pl.col("Instrument_Name").str.contains("(CCS)")
                ) & (pl.col("CN_Standard") == False)
            )
        )

    elif date < datetime.date(2022, 9, 19):
        TopPercentage_Securities = TopPercentage_Securities.filter(
            ~(
                (pl.col("Country") == "CN") & 
                (
                    pl.col("Instrument_Name").str.contains("'A'") | 
                    pl.col("Instrument_Name").str.contains("(CCS)")
                ) & (pl.col("CN_Standard") == False)
            )
        )

    else:

        # Add Exchange Information from Emerging Frame
        TopPercentage_Securities = TopPercentage_Securities.join(Emerging.select(pl.col(["Date", "Internal_Number", "Exchange"])), on=["Date", "Internal_Number"], how="left")

        TopPercentage_Securities = TopPercentage_Securities.filter(
            ~(
                ((pl.col("Exchange") == 'Stock Exchange of Hong Kong - SSE Securities') | 
                (pl.col("Exchange") == 'Stock Exchange of Hong Kong - SZSE Securities')) & 
                (pl.col("Country") == "CN") & (pl.col("CN_Standard") == False)
            )
        )

    try:
        # Drop the CN_Standard column
        TopPercentage_Securities = TopPercentage_Securities.drop(["CN_Standard", "Exchange"])
    except:
        TopPercentage_Securities = TopPercentage_Securities.drop(["CN_Standard"])

    return TopPercentage_Securities

# Select columns to read from the Parquets
Columns = ["Date", "Index_Symbol", "Index_Name", "Internal_Number", "ISIN", "SEDOL", "RIC", "Instrument_Name", 
           "Country", "Currency", "Exchange", "ICB", "Free_Float", "Capfactor", "Shares", "Close_unadjusted_local", "FX_local_to_Index_Currency"]

# Emerging Universe
Emerging = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\SWEACGV_with_Dec24.parquet", columns=Columns).with_columns([
                            pl.col("Free_Float").cast(pl.Float64),
                            pl.col("Capfactor").cast(pl.Float64),
                            pl.col("Shares").cast(pl.Float64),
                            pl.col("Close_unadjusted_local").cast(pl.Float64),
                            pl.col("FX_local_to_Index_Currency").cast(pl.Float64),
                            pl.col("Date").cast(pl.Date)
                            ])

for date in AllCap.select(["Date"]).unique().sort("Date").to_series():

    # Filter AllCap
    temp_AllCap = AllCap.filter(pl.col("Date") == date)

    # Drop the CN A Small Securities
    temp_AllCap = China_A_Small_Removal(temp_AllCap, Standard, date)

    # Stack the Output
    Output = Output.vstack(temp_AllCap)

# Recap Standard Index
Recap_Count_Standard = (
    Output
    .group_by(["Country", "Date"])  # Group by Country and Date
    .agg(pl.col("Internal_Number").count().alias("Sum_Components"))  # Count "Count" column and alias it
    .sort("Date")  # Ensure sorting by Date for proper column ordering in the pivot
    .pivot(
        index="Country",  # Set Country as the row index
        on="Date",        # Create columns for each unique Date
        values="Sum_Components"  # Fill in values with Sum_Components
    )
)

Recap_Weight_Standard = (
    Output
    .group_by(["Country", "Date"])  # Group by Country and Date
    .agg(pl.col("Weight").sum().alias("Weight_Components"))  # Count "Count" column and alias it
    .sort("Date")  # Ensure sorting by Date for proper column ordering in the pivot
    .pivot(
        index="Country",  # Set Country as the row index
        on="Date",        # Create columns for each unique Date
        values="Weight_Components"  # Fill in values with Sum_Components
    )
)

# Get current date formatted as YYYYMMDD_HHMMSS
from datetime import datetime
current_datetime = datetime.today().strftime('%Y%m%d')

# Store the results
Output.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\AllCap_Index_Security_Level_{GMSR_Upper_Buffer}_{GMSR_Lower_Buffer}_" + current_datetime + "_NoShadow_NoChinaASmall.csv")
# Recap_Count_Standard.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\All_Country\Recap_Count_AllCap_Index_Security_Level_ETF_Version_Coverage_Adjustment_" + current_datetime + ".csv")
# Recap_Weight_Standard.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Output\All_Country\Recap_Weight_AllCap_Index_Security_Level_ETF_Version_Coverage_Adjustment_" + current_datetime + ".csv")