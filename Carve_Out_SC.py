import polars as pl
import datetime
import os

# Import Global Variables
CN_Target_Percentage = float(os.getenv("CN_Target_Percentage"))
current_datetime = os.getenv("current_datetime")
GMSR_Upper_Buffer = float(os.getenv("GMSR_Upper_Buffer"))
GMSR_Lower_Buffer = float(os.getenv("GMSR_Lower_Buffer"))

Standard_Index = pl.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\TMI_Based\Output\Tests\Standard_Index_Security_Level_Shadows_CNTarget_{CN_Target_Percentage}_" + current_datetime + ".csv").with_columns(
    pl.col("Date").cast(pl.Date)
)

All_Cap = pl.read_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\TMI_Based\Output\Tests\AllCap_Index_Security_Level_{GMSR_Upper_Buffer}_{GMSR_Lower_Buffer}_" + current_datetime + "_NoShadow_NoChinaASmall.csv").with_columns(
    pl.col("Date").cast(pl.Date)
)

Small_Index = All_Cap.join(Standard_Index.select(pl.col(["Date", "Internal_Number"])), on=["Date", "Internal_Number"], how="anti")

# Get current date formatted as YYYYMMDD_HHMMSS
from datetime import datetime
current_datetime = datetime.today().strftime('%Y%m%d')

Small_Index.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\TMI_Based\Output\Tests\Small_Index_Security_Level_CNTarget_{CN_Target_Percentage}_{GMSR_Upper_Buffer}_{GMSR_Lower_Buffer}_" + current_datetime + ".csv")