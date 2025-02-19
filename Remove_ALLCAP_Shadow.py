import polars as pl
import os

# Import Global Variables
CN_Target_Percentage = float(os.getenv("CN_Target_Percentage"))
current_datetime = os.getenv("current_datetime")
GMSR_Upper_Buffer = float(os.getenv("GMSR_Upper_Buffer"))
GMSR_Lower_Buffer = float(os.getenv("GMSR_Lower_Buffer"))

AllCap = pl.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\AllCap_Index_Security_Level_{GMSR_Upper_Buffer}_{GMSR_Lower_Buffer}_" + current_datetime + ".csv").with_columns(
    pl.col("Date").cast(pl.Date)
)

Standard_Shadow = pl.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\Standard_Index_Security_Level_Shadows_CNTarget_{CN_Target_Percentage}_" + current_datetime + ".csv").with_columns(
    pl.col("Date").cast(pl.Date)
)

Standard_Shadow = Standard_Shadow.filter(pl.col("Shadow_Company")==True)

Output = pl.DataFrame(schema=AllCap.schema)

# Remove Shadow
for date in AllCap.select(pl.col("Date")).unique().to_series():
    temp_AllCap = AllCap.filter(pl.col("Date")==date)
    temp_Standard = Standard_Shadow.filter(pl.col("Date")==date)

    temp_AllCap = temp_AllCap.filter(~pl.col("Internal_Number").is_in(temp_Standard["Internal_Number"]))

    Output = Output.vstack(temp_AllCap)

Output.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\AllCap_Index_Security_Level_{GMSR_Upper_Buffer}_{GMSR_Lower_Buffer}_" + current_datetime + "_NoShadow.csv")

# Create Recap Count AllCap
Recap_Count = (
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

# Get current date formatted as YYYYMMDD_HHMMSS
from datetime import datetime
current_datetime = datetime.today().strftime('%Y%m%d')

Recap_Count.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\Recap_Count_AllCap_Index" + current_datetime + "_NoShadow.csv")