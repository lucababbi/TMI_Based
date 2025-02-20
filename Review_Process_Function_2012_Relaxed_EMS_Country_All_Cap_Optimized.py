import polars as pl
import pandas as pd
from datetime import date
import matplotlib.pyplot as plt
from pandasql import sqldf
import datetime
import glob
import os
import numpy as np
import math
import time

start_time = time.time()

##################################
###########Parameters#############
##################################
Starting_Date = date(2012, 6, 18)
Upper_Limit = 1.15
Lower_Limit = 0.50

Percentage = 0.99
Left_Limit = Percentage - 0.005
Right_Limit = 1.00

Threshold_NEW = 0.15
Threshold_OLD = 0.05

Threshold_NEW_12M = 0.15
Threshold_OLD_12M = 0.10

Trading_Days_NEW = 0.80
Trading_Days_OLD = 0.70

FOR_FF_Screen = 0.15

Screen_TOR = True

# MSCI GMSR Mar_2012
GMSR_Upper_Buffer = float(os.getenv("GMSR_Upper_Buffer"))
GMSR_Lower_Buffer = float(os.getenv("GMSR_Lower_Buffer"))
CN_Target_Percentage = float(os.getenv("CN_Target_Percentage"))
current_datetime = os.getenv("current_datetime")
GMSR_MSCI = np.float64(330 * 1_000_000)

# Country Adjustment based on MSCI Mar_2012
MSCI_Curve_Adjustment = pl.DataFrame({"Country": ["AU", "BG", "BR", "CA", "CL", "CN", "CO", "EG", "HK", "HU", "ID", "IL", "IN", "JP", "KR", "MA", "MX", 
                                    "MY", "PH", "PL", "RU", "SG", "TH", "TR", "TW", "US", "ZA", "DK", "IE", "CH", "GB", "NL", "SE", "AT", 
                                    "GR", "NO", "FR", "ES", "DE", "FI", "IT", "BE", "PT", "CZ", "GR", "NZ"],
                                    "Coverage": [0.860, 0.860, 0.950, 0.846, 0.901, 0.901, 0.990, 0.860, 0.875, 0.975, 0.821, 0.810, 0.869, 0.841, 
                                    0.878, 0.910, 0.950, 0.830, 0.815, 0.890, 0.995, 0.837, 0.825, 0.815, 0.810, 0.862, 0.875, 0.900, 
                                    0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 
                                    0.900, 0.85, 0.85, 0.85]})

# MSCI Equity_Minum_Size
MSCI_Equity_Minimum_Size = (130 * 1_000_000)

# Index Creation Country Coverage Adjustment for TMI Universe
Coverage_Adjustment = False

# Excel Setter
Excel_Recap = False
Excel_Recap_Rebalancing = False

# Full List Securities
FullListSecurities = True

Country_Plotting = "BR"
Output_File = rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\TopPercentage_Report_Rebalancing_{Country_Plotting}.xlsx"

# ETFs SPDR-iShares
ETF = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Universe\ETFs_STANDARD-SMALL.csv", separator=";")

##################################
###############ADR################
##################################
def ADR_Removal(Frame: pl.DataFrame, Emerging, Developed, Segment: pl.Utf8):
    # Compute count of each ENTITY_QID
    Count_Frame = Frame.group_by("ENTITY_QID").len().rename({"len": "Occurrence_Count"})

    # Merge the info with the main Frame
    Frame = Frame.join(Count_Frame, on="ENTITY_QID", how="left")

    if Segment == "Emerging":
        # Add Index_Symbol
        Frame = Frame.join(Emerging.select(pl.col(["Date", "Index_Symbol", "Internal_Number"])), on=["Date", "Internal_Number"], how="left")
    else:
        # Add Index_Symbol
        Frame = Frame.join(Developed.select(pl.col(["Date", "Index_Symbol", "Internal_Number"])), on=["Date", "Internal_Number"], how="left")

    # ADR Selection
    ADR_Frame = Frame.filter(pl.col("Occurrence_Count") > 1)

    # Removal_Frame
    Removal_Frame = pl.DataFrame()

    # Loop through each unique ENTITIY_QID
    for Entity in ADR_Frame.select("ENTITY_QID").unique().to_series():
        temp_Frame = ADR_Frame.filter(pl.col("ENTITY_QID")==Entity)

        # Check for SXGDRGV (STOXX WORLD DR)
        if (len(temp_Frame.filter(pl.col("Index_Symbol") == "SXGDRGV"))) >= 1 and len(temp_Frame) > 1:
            # Keep only the one to remove from the final Frame
            temp_Frame = temp_Frame.filter(pl.col("Index_Symbol") == "SXGDRGV")

            Removal_Frame = Removal_Frame.vstack(temp_Frame)

    # Remove not valid ADR from the Frame
    try:
        Frame = Frame.filter(~pl.col("Internal_Number").is_in(Removal_Frame.select("Internal_Number")))
    except:
        next
    return Frame

##################################
###########Trading Days###########
##################################
def Trading_Frequency(Frame: pl.DataFrame, Trading_Days, date, Starting_Date, Segment):

    # List of Unique Dates
    Dates_List = Pivot_TOR.index.to_list()
    IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
    Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

    # Filter for current date and drop eventual duplicates
    temp_Trading_Days = Trading_Days.filter(pl.col("Review")==date).drop("cutoff").unique("StoxxId")

    # Merge the Frames
    Frame = Frame.join(temp_Trading_Days, left_on="Internal_Number", right_on="StoxxId", how="left").drop("Review")

    # Calculate the Trading Ratio
    Frame = Frame.with_columns(
        (pl.col("Trades") / pl.col("MaxTrades")).alias("Trading_Ratio")
    )

    # Determine the Threshold for each Internal_Number
    if date == Starting_Date:
        Frame = Frame.with_columns(pl.lit(Trading_Days_NEW).alias("Threshold_Trading_Days"))

    else:

        if Segment == "Emerging":
            Frame = Frame.with_columns(
                                        pl.when(
                                            pl.col("Internal_Number").is_in(
                                                Screened_Securities.filter(pl.col("Date") == Previous_Date).select(pl.col("Internal_Number"))
                                            )
                                        )
                                        .then(pl.lit(Trading_Days_OLD))
                                        .otherwise(pl.lit(Trading_Days_NEW))
                                        .alias("Threshold_Trading_Days")
                                    )
        else:
            Frame = Frame.with_columns(
                                        pl.when(
                                            pl.col("Internal_Number").is_in(
                                                Screened_Securities.filter(pl.col("Date") == Previous_Date).select(pl.col("Internal_Number"))
                                            )
                                        )
                                        .then(pl.lit(Trading_Days_OLD + 0.10))
                                        .otherwise(pl.lit(Trading_Days_NEW + 0.10))
                                        .alias("Threshold_Trading_Days")
                                    )

    # Verify Securities Passing the Screens
    Results = Frame.with_columns(
        (pl.col("Trading_Ratio") >= pl.col("Threshold_Trading_Days")).alias("Status_Trading")
    )

    return Results.select(pl.col(["Internal_Number", "Status_Trading"]))

##################################
#########Index Continuity#########
##################################
def Index_Continuity(TopPercentage_Securities, TopPercentage, Segment: pl.Utf8, temp_Emerging, country, Standard_Index):

    # Check if there are at least 3 NON-NEW Companies
    if (Segment == "Emerging") & (len(TopPercentage_Securities.filter(pl.col("Shadow_Company") == False)) < 3):

        # Keep only Non-Shadow Securities
        TopPercentage_Securities = TopPercentage_Securities.filter(pl.col("Shadow_Company") == False) 

        # Take all Securities passing the Screens
        temp_Emerging_Country = temp_Emerging.filter(pl.col("Country")==country).sort("Free_Float_MCAP_USD_Cutoff", descending=True)

        Previous_Date = Standard_Index.filter(pl.col("Country") == country) \
            .unique(subset=["Date"]) \
            .select(pl.col("Date").max()) \
            .to_numpy()[0, 0] 
        
        if isinstance(Previous_Date, np.datetime64):
            Previous_Date = Previous_Date.astype('M8[D]').astype(datetime.date) 

        # Securities to pump 1.5 Free_Float_MCAP_USD_Cutoff (excluding the Securities who are already )
        temp_Emerging_Current = temp_Emerging_Country.filter(
                    pl.col("Internal_Number").is_in(
                        Standard_Index.filter(
                            (pl.col("Country") == country) & (pl.col("Date") == Previous_Date)
                        ).select(pl.col("Internal_Number"))
                    )
                ).with_columns(
                    (pl.col("Free_Float_MCAP_USD_Cutoff") * 1.5).alias("Free_Float_MCAP_USD_Cutoff")
                ).filter(~pl.col("Internal_Number").is_in(TopPercentage_Securities.select(pl.col("Internal_Number"))))

        # All Securities not included in the Standard Index for the Previous Date       
        temp_Emerging_Non_Current = temp_Emerging_Country.filter(
                    (~pl.col("Internal_Number").is_in(temp_Emerging_Current.select(pl.col("Internal_Number")))) &
                    (~pl.col("Internal_Number").is_in(TopPercentage_Securities.select(pl.col("Internal_Number"))))
                )

        # Stack the Frames
        temp_Emerging_Country = temp_Emerging_Current.vstack(temp_Emerging_Non_Current).with_columns(
            pl.lit("All_Cap").alias("Size"),
            pl.lit(False).alias("Shadow_Company")
        )

        if len(temp_Emerging_Country) >= (3 - len(TopPercentage_Securities)):

            # Keep the Securities needed to get the minimum number of Securities
            temp_Emerging_Country = temp_Emerging_Country.sort("Free_Float_MCAP_USD_Cutoff", descending = True).head(3 - len(TopPercentage_Securities)).with_columns(pl.lit(False).alias("1Y_Exclusion"),
                                                                                                                                                                     pl.lit(None).alias("Exclusion_Date"))

            TopPercentage_Securities = TopPercentage_Securities.vstack(temp_Emerging_Country.select(TopPercentage_Securities.columns))

            # Fix the columns
            TopPercentage_Securities = TopPercentage_Securities.select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country"])).with_columns(
                pl.lit("All_Cap").alias("Size"),
                pl.lit(False).alias("Shadow_Company")
            )

            TopPercentage = TopPercentage_Securities.group_by(["Date", "ENTITY_QID"]).agg([
                pl.col("Country").first().alias("Country")
            ]).with_columns([
                pl.lit("All_Cap").alias("Size"),
                pl.lit("Maintenance").alias("Case")
            ])

        else:
            TopPercentage_Securities = TopPercentage_Securities.head(0)

    return TopPercentage_Securities, TopPercentage

##################################
#########FOR Screening############
##################################
def FOR_Screening(Screened_Securities, Frame: pl.DataFrame, Full_Frame: pl.DataFrame, Pivot_TOR, Standard_Index, Small_Index, date, Segment: pl.Utf8, LIF_Stored) -> pl.DataFrame:

    # List of Unique Dates
    Dates_List = Pivot_TOR.index.to_list()
    IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
    Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

    Screened_Frame = pl.DataFrame()

    # Loop for all the Countries
    for country in Frame.select(["Country"]).unique().sort("Country").to_series():

        # List of the current traded Companies 
        Full_Frame_Country = Full_Frame.filter(
                (pl.col("Date") == date) & (pl.col("Country") == country)
            ).with_columns(
                pl.col("Full_MCAP_USD_Cutoff").sum().over("ENTITY_QID").alias("Full_MCAP_USD_Cutoff_Company"),
                pl.col("Free_Float_MCAP_USD_Cutoff").sum().over("ENTITY_QID").alias("Free_Float_MCAP_USD_Cutoff_Company")
            ).select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number", "Instrument_Name", "Free_Float_MCAP_USD_Cutoff_Company",
                                "Full_MCAP_USD_Cutoff_Company"]))
        

        #TODO: In the future, to determine if a Security was included in the Index, should look at Standard_Index not Screened_Securities
        if Segment == "Developed":
            # Screened Securities filtered for Date and Country
            Temp_Screened_Securities = Screened_Securities.filter((pl.col("Country")==country) & (pl.col("Date")==Previous_Date))

            # Add ENTITY_QID to Temp_Screened_Securities for JOIN
            Temp_Screened_Securities = Temp_Screened_Securities.join(Entity_ID,
                                left_on="Internal_Number",
                                right_on="STOXX_ID",
                                how="left"
                            ).unique(["Date", "Internal_Number"]).sort("Date", descending=False).with_columns(
                            pl.col("ENTITY_QID").fill_null(pl.col("Internal_Number"))).drop({"RELATIONSHIP_VALID_FROM", "RELATIONSHIP_VALID_TO"})
        
        else:
            # Screened Securities filtered for Date and Country
            Temp_Screened_Securities = Standard_Index.filter((pl.col("Country")==country) & (pl.col("Date")==Previous_Date))

            # Add ENTITY_QID to Temp_Screened_Securities for JOIN
            Temp_Screened_Securities = Temp_Screened_Securities.join(Entity_ID,
                                left_on="Internal_Number",
                                right_on="STOXX_ID",
                                how="left"
                            ).unique(["Date", "Internal_Number"]).sort("Date", descending=False).with_columns(
                            pl.col("ENTITY_QID").fill_null(pl.col("Internal_Number"))).drop({"RELATIONSHIP_VALID_FROM", "RELATIONSHIP_VALID_TO"})

        # Add information for Securities previously included in the Screened Universe
        temp_Frame = Frame.filter(pl.col("Country") == country).with_columns(
                (pl.col("Internal_Number").is_in(Temp_Screened_Securities.select(pl.col("Internal_Number")))).alias("InPrevScreened_Universe")
            )
        
        if date == Starting_Date:

            # Calculate LIF for the Refresehed Universe
            temp_Frame = temp_Frame.with_columns(
                                        # Main case where FOR is higher than 25%
                                        pl.when(pl.col("foreign_headroom") >= 0.25)
                                        .then(pl.lit(1))
                                        # Case 1 for New Constituents in between limits
                                        .when((pl.col("foreign_headroom") >= 0.15) & (pl.col("foreign_headroom") < 0.25) & (pl.col("InPrevScreened_Universe") == False))
                                        .then(pl.lit(0.5))
                                        # Case 2 for Current Constituents in between limits
                                        .when((pl.col("foreign_headroom") >= 0.0375) & (pl.col("foreign_headroom") < 0.25) & (pl.col("InPrevScreened_Universe") == True))
                                        .then(pl.lit(0.5))
                                        # Case 3 for New Constituents below the limit
                                        .when((pl.col("foreign_headroom") < 0.15) & (pl.col("InPrevScreened_Universe") == False))
                                        .then(pl.lit(0))
                                        # Case 4 for Current Constituents below the limit
                                        .when((pl.col("foreign_headroom") < 0.0375) & (pl.col("InPrevScreened_Universe") == True))
                                        .then(pl.lit(0))
                                        .otherwise(None)  # Ensure all cases are handled
                                        .alias("LIF")
                                    )
            
            # Stack the new LIF_Stored data
            LIF_Stored_Data = temp_Frame.select(pl.col(["Date", "Internal_Number", "LIF", "foreign_headroom"])).with_columns(
                                pl.lit(False).alias("1Y_Exclusion"),
                                pl.lit(None).alias("Exclusion_Date"))

            LIF_Stored = LIF_Stored.vstack(LIF_Stored_Data)
            
        
        else:

            # In case of new Review date, table for Increase/Decrease of Foreign Headroom applies
            temp_LIF_Stored = LIF_Stored.filter(pl.col("Date")==Previous_Date).rename({"LIF": "Current_LIF"})

            # Separate Current Constituents from New Ones
            temp_Frame = temp_Frame.join(temp_LIF_Stored.select(pl.col(["Internal_Number", "Current_LIF", "1Y_Exclusion"])), on=["Internal_Number"], how="left").with_columns(
                pl.when(pl.col("InPrevScreened_Universe")==False)
                .then(pl.lit(None))
                .otherwise(pl.col("Current_LIF"))
                .alias("Current_LIF")).with_columns(
                pl.col("1Y_Exclusion").fill_null(False)) # Fill with False for Securities appearing only now in the main Frame
            
            # Check for 1Y_Exclusion
            if len(temp_Frame.filter(pl.col("1Y_Exclusion") == True)) > 0:

                # Add Exclusion Date
                temp_Frame = temp_Frame.join(temp_LIF_Stored.select(pl.col(["Internal_Number", "Exclusion_Date"])), on=["Internal_Number"], how="left")

                # Get the previou Date and Current Date
                Relevant_Dates = pl.Series(Dates_List[max(0, IDX_Current - 4): IDX_Current + 1])

                # Convert Relevant_Dates to DataFrame for joining
                Relevant_Dates_df = pl.DataFrame({"Date": Relevant_Dates}).with_columns(pl.col("Date").cast(pl.Date))

                # Get minimum Date
                Min_Date = Relevant_Dates.str.strptime(pl.Date, "%Y-%m-%d").min()

                # Check if Minimum_Date is same as Exclusion_Date to see if 1Y has elapsed
                temp_Frame = temp_Frame.with_columns(
                    pl.when(pl.col("Exclusion_Date") <= Min_Date)
                    .then(pl.lit(True))
                    .when(pl.col("Exclusion_Date") > Min_Date)
                    .then(pl.lit(False))
                    .otherwise(None)
                    .alias("1Y_Check")
                )

                if len(temp_Frame.filter(pl.col("1Y_Check") == True)) > 0:

                    if Segment == "Emerging":
                        # Find 1Y ago ForeignHeadroom values of Securities
                        Old_ForeignHeadroom = Emerging.filter(pl.col("Date")==Min_Date).select(pl.col(["Date", "Internal_Number", "foreign_headroom"])).rename({"foreign_headroom": "OLD_foreign_headroom"})
                    else:
                        Old_ForeignHeadroom = Developed.filter(pl.col("Date")==Min_Date).select(pl.col(["Internal_Number", "foreign_headroom"])).rename({"foreign_headroom": "OLD_foreign_headroom"})

                    # Add the value to main Frame
                    temp_Frame = temp_Frame.join(Old_ForeignHeadroom.select(pl.col(["Internal_Number", "OLD_foreign_headroom"])), on=["Internal_Number"], how="left")

                    # Check if Newer foreign_headroom is >= 15% (Minimum for New Constituents)
                    temp_Frame = temp_Frame.with_columns(
                        pl.when((pl.col("1Y_Exclusion") == True) & (pl.col("foreign_headroom") >= 0.15))
                        .then(pl.lit(False))
                        .otherwise(pl.col("1Y_Exclusion"))
                        .alias("1Y_Exclusion")
                    )

                    # Store the Securities which still fail the Screens
                    LIF_Stored_Data = temp_Frame.select(pl.col(["Date", "Internal_Number", "foreign_headroom", "InPrevScreened_Universe", "1Y_Exclusion", "Exclusion_Date"])).filter(pl.col("1Y_Exclusion")).with_columns(
                        pl.lit(0).cast(pl.Float64).alias("LIF")
                    )

                    # Stack the results
                    LIF_Stored = LIF_Stored.vstack(LIF_Stored_Data.select(pl.col(["Date", "Internal_Number", "LIF", "foreign_headroom", "1Y_Exclusion", "Exclusion_Date"])))

                    # Exclude the Securities that are 1Y_Exclusion == True
                    temp_Frame = temp_Frame.filter(pl.col("1Y_Exclusion") == False)

                else:

                    # Store the Securities which still fail the Screens
                    LIF_Stored_Data = temp_Frame.select(pl.col(["Date", "Internal_Number", "foreign_headroom", "InPrevScreened_Universe", "1Y_Exclusion", "Exclusion_Date"])).filter(pl.col("1Y_Exclusion")).with_columns(
                        pl.lit(0).cast(pl.Float64).alias("LIF")
                    )

                    # Stack the results
                    LIF_Stored = LIF_Stored.vstack(LIF_Stored_Data.select(pl.col(["Date", "Internal_Number", "LIF", "foreign_headroom", "1Y_Exclusion", "Exclusion_Date"])))

                    # Exclude the Securities that are 1Y_Exclusion == True
                    temp_Frame = temp_Frame.filter(pl.col("1Y_Exclusion") == False)

                # Drop unnecessary columns
                temp_Frame = temp_Frame.drop(["Exclusion_Date", "1Y_Check"])

            # Calculate LIF for the Refresehed Universe
            temp_Frame = temp_Frame.with_columns(
                # Current Adjustment Factor == 1
                pl.when((pl.col("Current_LIF") == 1) & (pl.col("foreign_headroom") >= 0.25))
                .then(pl.lit(1))
                .when((pl.col("Current_LIF") == 1) & (pl.col("foreign_headroom") >= 0.15) & (pl.col("foreign_headroom") < 0.25))
                .then(pl.lit(1))
                .when((pl.col("Current_LIF") == 1) & (pl.col("foreign_headroom") >= 0.075) & (pl.col("foreign_headroom") < 0.15))
                .then(pl.lit(0.5))
                .when((pl.col("Current_LIF") == 1) & (pl.col("foreign_headroom") >= 0.0375) & (pl.col("foreign_headroom") < 0.075))
                .then(pl.lit(0.25))
                .when((pl.col("Current_LIF") == 1) & (pl.col("foreign_headroom") < 0.0375))
                .then(pl.lit(0))
                # Current Adjustment Factor == 0.5
                .when((pl.col("Current_LIF") == 0.5) & (pl.col("foreign_headroom") >= 0.25))
                .then(pl.lit(1))
                .when((pl.col("Current_LIF") == 0.5) & (pl.col("foreign_headroom") >= 0.15) & (pl.col("foreign_headroom") < 0.25))
                .then(pl.lit(0.5))
                .when((pl.col("Current_LIF") == 0.5) & (pl.col("foreign_headroom") >= 0.075) & (pl.col("foreign_headroom") < 0.15))
                .then(pl.lit(0.5))
                .when((pl.col("Current_LIF") == 0.5) & (pl.col("foreign_headroom") >= 0.0375) & (pl.col("foreign_headroom") < 0.075))
                .then(pl.lit(0.25))
                .when((pl.col("Current_LIF") == 0.5) & (pl.col("foreign_headroom") < 0.0375))
                .then(pl.lit(0))
                # Current Adjustment Factor == 0.25
                .when((pl.col("Current_LIF") == 0.25) & (pl.col("foreign_headroom") >= 0.25))
                .then(pl.lit(1))
                .when((pl.col("Current_LIF") == 0.25) & (pl.col("foreign_headroom") >= 0.15) & (pl.col("foreign_headroom") < 0.25))
                .then(pl.lit(0.5))
                .when((pl.col("Current_LIF") == 0.25) & (pl.col("foreign_headroom") >= 0.075) & (pl.col("foreign_headroom") < 0.15))
                .then(pl.lit(0.25))
                .when((pl.col("Current_LIF") == 0.25) & (pl.col("foreign_headroom") >= 0.0375) & (pl.col("foreign_headroom") < 0.075))
                .then(pl.lit(0.25))
                .when((pl.col("Current_LIF") == 0.25) & (pl.col("foreign_headroom") < 0.0375))
                .then(pl.lit(0))
                # New Constituents
                .when((pl.col("Current_LIF").is_null()) & (pl.col("foreign_headroom") >= 0.25))
                .then(pl.lit(1))
                .when((pl.col("Current_LIF").is_null()) & (pl.col("foreign_headroom") >= 0.15) & (pl.col("foreign_headroom") < 0.25))
                .then(pl.lit(0.5))
                .when((pl.col("Current_LIF").is_null()) & (pl.col("foreign_headroom") < 0.15))
                .then(pl.lit(0))
                # Ensure all cases are handled
                .otherwise(None)  
                .alias("LIF")
            )

            # Stack the new LIF_Stored data and apply the 1Y_Exclusion rule
            LIF_Stored_Data = temp_Frame.select(pl.col(["Date", "Internal_Number", "LIF", "foreign_headroom", "InPrevScreened_Universe"])).with_columns(
                    pl.when((pl.col("LIF")==0) & (pl.col("InPrevScreened_Universe") == True))
                    .then(pl.lit(True))
                    .otherwise(pl.lit(False))
                    .alias("1Y_Exclusion"),
                    pl.when((pl.col("LIF")==0) & (pl.col("InPrevScreened_Universe") == True))
                    .then(pl.lit(date))
                    .otherwise(pl.lit(None))
                    .alias("Exclusion_Date")
                )

            # Stack the results
            LIF_Stored = LIF_Stored.vstack(LIF_Stored_Data.select(pl.col(["Date", "Internal_Number", "LIF", "foreign_headroom", "1Y_Exclusion", "Exclusion_Date"])))
        

        # Calculate FIF for the resulting Frame
        temp_Frame = temp_Frame.with_columns(
            (pl.col("FOR") * pl.col("LIF")).alias("FIF")
        )

        # Filter for those Securities failing the FOR_Screen
        Failing_Securities = temp_Frame.with_columns(
                                    pl.when(
                                        (pl.col("InPrevScreened_Universe") == False) & (pl.col("FIF") < FOR_FF_Screen) & (pl.col("FIF") > 0)
                                    ).then(pl.lit("To_Review"))
                                    .when(
                                        (pl.col("InPrevScreened_Universe") == False) & (pl.col("FIF") == 0)
                                    ).then(pl.lit("Automatically_Excluded"))
                                    .otherwise(pl.lit("Eligible"))
                                    .alias("FIF_Screened")
                                ).filter((pl.col("FIF_Screened") == "To_Review") | (pl.col("FIF_Screened") == "Automatically_Excluded"))
        
        # Remove Securities which are Automatically_Excluded == TRUE
        temp_Frame = temp_Frame.filter(~pl.col("Internal_Number").is_in(Failing_Securities.filter(pl.col("FIF_Screened")=="Automatically_Excluded").select(pl.col("Internal_Number"))))

        if len(Failing_Securities.filter((pl.col("FIF_Screened") == "To_Review"))) > 0:
            IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
            Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

            if Segment == "Developed":

                # Take the full Index based on the previous date/country that are still traded (Security level) to calculate the Country Cutoff
                Investable_Index = Temp_Screened_Securities.join(
                                        Full_Frame_Country.select(pl.col(["Internal_Number", "Full_MCAP_USD_Cutoff_Company"])), 
                                        on=["Internal_Number"], how="left").filter((pl.col("Full_MCAP_USD_Cutoff_Company") > 0)
                                        ).sort("Full_MCAP_USD_Cutoff_Company", descending=True)                          
            else:

                # Take the full Index based on the previous date/country that are still traded (Security level) to calculate the Country Cutoff
                Investable_Index = Standard_Index.filter((pl.col("Country")==country) & (pl.col("Date")==Previous_Date)).join(
                                        Full_Frame_Country.select(pl.col(["Internal_Number", "Full_MCAP_USD_Cutoff_Company"])), 
                                        on=["Internal_Number"], how="left").filter((pl.col("Full_MCAP_USD_Cutoff_Company") > 0)
                                        ).sort("Full_MCAP_USD_Cutoff_Company", descending=True)     

            # Case where Investable_Index is NULL
            if len(Investable_Index) > 0:
                
                if Segment == "Emerging":
                    # Count the Companies that made it in the previous Basket
                    Previous_Count = len(Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date))) - 1
                
                else:
                    # TODO: Change when Developed will be run together with Emerging
                    # Count the Companies that made it in the previous Basket
                    Previous_Count = -1

                # Ensure Previous_Count is within bounds
                if Previous_Count >= Investable_Index.height:
                    # Take the last row if Previous_Count exceeds the DataFrame length
                    Previous_Count = Investable_Index.height - 1

                # Find the Country_Cutoff ["Full_MCAP_USD_Cutoff_Company"]
                Country_Cutoff = Investable_Index.sort("Full_MCAP_USD_Cutoff_Company", descending=True).row(Previous_Count)[Investable_Index.columns.index("Full_MCAP_USD_Cutoff_Company")] / 2 * 1.8

                # Recalculate Free_Float MCAP using FIF
                Failing_Securities = Failing_Securities.with_columns(
                    (pl.col("Close_unadjusted_local_Cutoff") * pl.col("Shares_Cutoff") * pl.col("FIF") * pl.col("FX_local_to_Index_Currency_Cutoff")).alias("Free_Float_MCAP_USD_Cutoff")
                )

                # Check if the Failing_Securities have a Free_Float_MCAP_USD_Cutoff >= than the Country_Cutoff
                Failing_Securities = Failing_Securities.with_columns(
                                            (pl.col("Free_Float_MCAP_USD_Cutoff") >= Country_Cutoff).alias("Screen"))

                # Filter out the Securities not passing the Screen
                temp_Frame = temp_Frame.filter(
                        ~pl.col("Internal_Number").is_in(
                            Failing_Securities.filter(pl.col("Screen") == False).select("Internal_Number").to_series()
                        )
                    )

            else: # If Country is newly added

                # Check if the Failing_Securities have a Free_Float_MCAP_USD_Cutoff >= than the Country_Cutoff
                Failing_Securities = Failing_Securities.with_columns(
                                            (pl.col("Free_Float_MCAP_USD_Cutoff") >= FOR_FF_Screen).alias("Screen")
                )

                # Filter out the Securities not passing the Screen
                temp_Frame = temp_Frame.filter(
                        ~pl.col("Internal_Number").is_in(
                            Failing_Securities.filter(pl.col("Screen") == False).select("Internal_Number").to_series()
                        )
                    )

        try:
            # Stack the resulting Frame
            Screened_Frame = Screened_Frame.vstack(temp_Frame)
        except:
            Screened_Frame = Screened_Frame.vstack(temp_Frame.select(Screened_Frame.columns))
            
    return Screened_Frame, LIF_Stored

##################################
#######China A Securities#########
##################################
def China_A_Securities(Frame: pl.DataFrame) -> pl.DataFrame:

    Results = pl.DataFrame({"Date": pl.Series(dtype=pl.Date),
                            "Internal_Number": pl.Series(dtype=pl.Utf8),
                            "Capfactor": pl.Series(dtype=pl.Float64),
                            "Instrument_Name": pl.Series(dtype=pl.Utf8),
                            "Country": pl.Series(dtype=pl.Utf8),
                            "Capfactor_CN": pl.Series(dtype=pl.Float64),
                            "Adjustment": pl.Series(dtype=pl.Float64)
                            })

    for Date in Frame.select(["Date"]).unique().sort("Date").to_series():
        # Filter for the given date
        temp_Frame = Frame.filter(pl.col("Date") == Date)

        if  datetime.date(2019,3,18) <= Date < datetime.date(2019,9,23):
            Chinese_Securities = temp_Frame.filter(
                                (
                                    (pl.col("Country") == "CN") & (pl.col("Capfactor") < 1) &
                                    (
                                        pl.col("Instrument_Name").str.contains("'A'") |
                                        pl.col("Instrument_Name").str.contains("(CCS)")
                                    ))
                            ).select(pl.col(["Date", "Internal_Number", "Capfactor", "Instrument_Name", "Country"])).with_columns(
                                        (pl.col("Capfactor") / 0.1).alias("Capfactor_CN"),
                                        (pl.lit(0.10).alias("Adjustment"))
                                    )

        elif Date < datetime.date(2022,9,19):
            Chinese_Securities = temp_Frame.filter(
                                (
                                    (pl.col("Country") == "CN") & (pl.col("Capfactor") < 1) &
                                    (
                                        pl.col("Instrument_Name").str.contains("'A'") |
                                        pl.col("Instrument_Name").str.contains("(CCS)")
                                    ))
                            ).select(pl.col(["Date", "Internal_Number", "Capfactor", "Instrument_Name", "Country"])).with_columns(
                                        (pl.col("Capfactor") / 0.2).alias("Capfactor_CN"),
                                        (pl.lit(0.20).alias("Adjustment"))
                                    )
        else:
            Chinese_Securities = temp_Frame.filter(
                                ((pl.col("Exchange") == 'Stock Exchange of Hong Kong - SSE Securities') |
                                (pl.col("Exchange") == 'Stock Exchange of Hong Kong - SZSE Securities')) & 
                                (pl.col("Country") == "CN")
                            ).select(
                                ["Date", "Internal_Number", "Capfactor", "Instrument_Name", "Country"]
                            ).with_columns(
                                (pl.col("Capfactor") / 0.2).alias("Capfactor_CN"),
                                (pl.lit(0.20).alias("Adjustment"))
                            )
        
        Results = Results.vstack(Chinese_Securities)
            
    return pl.DataFrame(Results)

##################################
#######Quarterly Turnover#########
##################################
def Turnover_Check(Frame: pl.DataFrame, Pivot_TOR: pl.DataFrame, Threshold_NEW, Threshold_OLD, date, Starting_Date, Segment: pl.Utf8) -> pl.DataFrame:

    # List of Unique Dates
    Dates_List = Pivot_TOR.index.to_list()

    # Output
    Results = []
    Status = {}

    for Row in Frame.select(pl.col("Date")).unique().iter_rows(named=True):
        Date = Row["Date"].strftime("%Y-%m-%d")
        IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
        Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

        # Find the index of Date in Pivot_TOR
        try:
            IDX_Current = Dates_List.index(Date)
        except ValueError:
            # If the date is not found, skip this row
            continue

        # Get the previou Date and Current Date
        Relevant_Dates = pl.Series(Dates_List[max(0, IDX_Current): IDX_Current + 1])

        # Convert Relevant_Dates to DataFrame for joining
        Relevant_Dates_df = pl.DataFrame({"Date": Relevant_Dates}).with_columns(pl.col("Date").cast(pl.Date))

        # Keep only the needed Dates
        Relevant_TOR = pl.DataFrame(Turnover).with_columns(pl.col("Date").cast(pl.Date)).join(Relevant_Dates_df, on="Date", how="right")

        # Join the previous four quarters
        Frame = (
                    Frame.join(Relevant_TOR, on="Internal_Number", how="left")
                    .drop("Date")  # Drop the original Date column from Frame
                    .rename({"Date_right": "Date"})  # Rename Date from Relevant_TOR to Date
                ).filter(~pl.col("Date").is_null())

        # Pivot the Frame
        Frame = Frame.pivot(values="Turnover_Ratio",
                            index="Internal_Number",
                            on="Date"
                        )
        
        # Determine the Threshold for each Internal_Number
        if date == Starting_Date:
            Frame = Frame.with_columns(pl.lit(Threshold_NEW).alias("Threshold"))

        else:

            if Segment == "Emerging":
                Frame = Frame.with_columns(
                                            pl.when(
                                                pl.col("Internal_Number").is_in(
                                                    Screened_Securities.filter(pl.col("Date") == Previous_Date).select(pl.col("Internal_Number"))
                                                )
                                            )
                                            .then(pl.lit(Threshold_OLD))
                                            .otherwise(pl.lit(Threshold_NEW))
                                            .alias("Threshold")
                                        )
            else:
                Frame = Frame.with_columns(
                                            pl.when(
                                                pl.col("Internal_Number").is_in(
                                                    Screened_Securities.filter(pl.col("Date") == Previous_Date).select(pl.col("Internal_Number"))
                                                )
                                            )
                                            .then(pl.lit(Threshold_OLD))
                                            .otherwise(pl.lit(Threshold_NEW))
                                            .alias("Threshold")
                                        )
            
        # Determine Columns Date
        date_columns = [col for col in Frame.columns if col not in ["Threshold", "Internal_Number"]]
        sorted_date_columns = sorted(date_columns)  # Ensure columns are in chronological order

        # Identify the most recent date column
        most_recent_date_col = sorted_date_columns[-1]

        # Add a column to check if all Columns are filled
        Frame = Frame.with_columns(
            pl.all_horizontal([pl.col(col).is_not_null() for col in date_columns]).alias("All_Dates_Available")
        )

        # Step 2: For rows where All_Dates_Available is False, fill missing date columns with the value from most_recent_date_col
        Frame = Frame.with_columns(
            [
                pl.when(pl.col("All_Dates_Available") == False)
                .then(pl.col(most_recent_date_col))
                .otherwise(pl.col(col))
                .alias(col)
                for col in date_columns if col != most_recent_date_col  # Apply this to all date columns except the most recent one
            ]
        )
        
        # Screened Frame
        Results = (Frame.with_columns(
            pl.min_horizontal(date_columns).alias("Min_Date_Value")
        )).with_columns(
            (pl.col("Min_Date_Value") >= pl.col("Threshold")).alias("Status_TOR")
        )

    # Return results as Polars
    return Results.select(pl.col(["Internal_Number", "Status_TOR"]))

##################################
##########12M Turnover############
##################################
def Turnover_Check_12M(Frame: pl.DataFrame, Pivot_TOR_12M: pl.DataFrame, Threshold_NEW_12M, Threshold_OLD_12M, date, Starting_Date, Segment: pl.Utf8) -> pl.DataFrame:

    # List of Unique Dates
    Dates_List = [d.strftime("%Y-%m-%d") for d in Pivot_TOR_12M.index.date]

    # Output
    Results = []
    Status = {}

    for Row in Frame.select(pl.col("Date")).unique().iter_rows(named=True):
        Date = Row["Date"].strftime("%Y-%m-%d")
        IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
        Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

        # Find the index of Date in Pivot_TOR
        try:
            IDX_Current = Dates_List.index(Date)
        except ValueError:
            # If the date is not found, skip this row
            continue

        # Get the previou Date and Current Date
        Relevant_Dates = pl.Series(Dates_List[max(0, IDX_Current): IDX_Current + 1])

        # Convert Relevant_Dates to DataFrame for joining
        Relevant_Dates_df = pl.DataFrame({"Date": Relevant_Dates}).with_columns(pl.col("Date").cast(pl.Date))

        # Keep only the needed Dates
        Relevant_TOR = pl.DataFrame(Turnover12M).with_columns(pl.col("Date").cast(pl.Date)).join(Relevant_Dates_df, on="Date", how="right")

        # Join the previous four quarters
        Frame = (
                    Frame.join(Relevant_TOR, on="Internal_Number", how="left")
                    .drop("Date")  # Drop the original Date column from Frame
                    .rename({"Date_right": "Date"})  # Rename Date from Relevant_TOR to Date
                ).filter(~pl.col("Date").is_null())

        # Pivot the Frame
        Frame = Frame.pivot(values="Turnover_Ratio",
                            index="Internal_Number",
                            on="Date"
                        )
        
        # Determine the Threshold for each Internal_Number
        if date == Starting_Date:
            Frame = Frame.with_columns(pl.lit(Threshold_NEW_12M).alias("Threshold"))

        else:

            if Segment == "Emerging":
                Frame = Frame.with_columns(
                                            pl.when(
                                                pl.col("Internal_Number").is_in(
                                                    Screened_Securities.filter(pl.col("Date") == Previous_Date).select(pl.col("Internal_Number"))
                                                )
                                            )
                                            .then(pl.lit(Threshold_OLD_12M))
                                            .otherwise(pl.lit(Threshold_NEW_12M))
                                            .alias("Threshold")
                                        )
            else:
                Frame = Frame.with_columns(
                                            pl.when(
                                                pl.col("Internal_Number").is_in(
                                                    Screened_Securities.filter(pl.col("Date") == Previous_Date).select(pl.col("Internal_Number"))
                                                )
                                            )
                                            .then(pl.lit(Threshold_OLD_12M))
                                            .otherwise(pl.lit(Threshold_NEW_12M))
                                            .alias("Threshold")
                                        )
            
        # Determine Columns Date
        date_columns = [col for col in Frame.columns if col not in ["Threshold", "Internal_Number"]]
        sorted_date_columns = sorted(date_columns)  # Ensure columns are in chronological order

        # Identify the most recent date column
        most_recent_date_col = sorted_date_columns[-1]

        # Add a column to check if all Columns are filled
        Frame = Frame.with_columns(
            pl.all_horizontal([pl.col(col).is_not_null() for col in date_columns]).alias("All_Dates_Available")
        )

        # Step 2: For rows where All_Dates_Available is False, fill missing date columns with the value from most_recent_date_col
        Frame = Frame.with_columns(
            [
                pl.when(pl.col("All_Dates_Available") == False)
                .then(pl.col(most_recent_date_col))
                .otherwise(pl.col(col))
                .alias(col)
                for col in date_columns if col != most_recent_date_col  # Apply this to all date columns except the most recent one
            ]
        )
        
        # Screened Frame
        Results = (Frame.with_columns(
            pl.min_horizontal(date_columns).alias("Min_Date_Value")
        )).with_columns(
            (pl.col("Min_Date_Value") >= pl.col("Threshold")).alias("Status_TOR")
        )

    # Return results as Polars
    return Results.select(pl.col(["Internal_Number", "Status_TOR"]))

##################################
#######Equity Minimum Size########
##################################
def Equity_Minimum_Size(temp_TMI, df: pl.DataFrame, Pivot_TOR, EMS_Frame, date, Segment: pl.Utf8, Screened_Securities, temp_Exchanges_Securities, FullListSecurities) -> pl.DataFrame:
    # List to hold results
    results = []

    # List of Unique Dates
    Dates_List = Pivot_TOR.index.to_list()

    try:
        IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
        if (date < datetime.date(2023, 3, 20) and (date.month == 6 or date.month == 12)) or (date >= datetime.date(2023, 3, 20)):
            Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()
        else:
            Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 2)], "%Y-%m-%d").date()
            
        previous_rank = EMS_Frame.filter((pl.col("Date") == Previous_Date)).select(pl.col("Rank")).to_numpy()[0][0]
    except:
        previous_rank = None
    final_df = pl.DataFrame()

    # Add ENTITY_QID to Screened_Securities
    Temp_Screened_Securities = Screened_Securities.join(
                            Entity_ID,
                            left_on="Internal_Number",
                            right_on="STOXX_ID",
                            how="left"
                          ).unique(["Date", "Internal_Number"]).sort("Date", descending=False).with_columns(
                              pl.col("ENTITY_QID").fill_null(pl.col("Internal_Number"))).drop({"RELATIONSHIP_VALID_FROM", "RELATIONSHIP_VALID_TO"})
    
    # Store the column list
    Security_Level_DF_Columns = df.columns
    
    # Create a copy of DF at Security_Level and add a column for Cumulative Full_MCAP_USD_Cutoff
    Security_Level_DF = df.with_columns(
                            df
                            .group_by("ENTITY_QID")
                            .agg(pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company"))
                            .join(df, on="ENTITY_QID", how="right")
                            .get_column("Full_MCAP_USD_Cutoff_Company")
                        ).sort("Full_MCAP_USD_Cutoff_Company", descending = True)

    # Setter for FullListSecurities
    if FullListSecurities == False:

        # Aggregate Securities by ENTITY_QID (Choose between DF and temp_TMI)
        df = temp_TMI.select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                        ["Date", "ENTITY_QID"]).agg([
                                                            pl.col("Country").first().alias("Country"),
                                                            pl.col("Internal_Number").first().alias("Internal_Number"),
                                                            pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                            pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company"),
                                                            pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company")
                                                        ]).sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending=True)
        
    else:

        df = temp_Exchanges_Securities.select(pl.col(["Date", "Instrument_Name", "DsCmpyCode", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                        ["Date", "DsCmpyCode"]).agg([
                                                            pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                            pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company"),
                                                            pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company")
                                                        ]).sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending=True)
    
    # Calculate cumulative sums and coverage
    df_date = df.with_columns([
        pl.col("Free_Float_MCAP_USD_Cutoff_Company").cum_sum().alias("Cumulative_Free_Float_MCAP_USD_Cutoff_Company"),
        (pl.col("Free_Float_MCAP_USD_Cutoff_Company").cum_sum() / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Cumulative_Coverage_Cutoff")
    ])

    if Segment == "Developed":

        # Add the MSCI Equity_Minimum_Size for JUN_2012
        if date == Starting_Date:
            
            equity_universe_min_size = float(MSCI_Equity_Minimum_Size)
            previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

        else:
    
            total_market_cap = df_date.select(pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).to_numpy()[0][0]
            
            if previous_rank is None:
                # Initial calculation
                min_size_company = df_date.filter(pl.col("Cumulative_Coverage_Cutoff") >= 0.99).select("Full_MCAP_USD_Cutoff_Company").head(1)
                equity_universe_min_size = min_size_company[0, 0]
                previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

                df_date1 = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).with_columns([
                    pl.lit(equity_universe_min_size).alias("EUMSR"),
                    pl.lit(previous_rank).alias("EUMSR_Rank")
                ])
            else:
                # Ensure previous_rank - 1 is within the bounds
                if previous_rank - 1 < len(df_date):
                    previous_row = df_date.row(previous_rank - 1)
                    previous_coverage = previous_row[df_date.columns.index("Cumulative_Free_Float_MCAP_USD_Cutoff_Company")] / total_market_cap
                    
                    if 0.995 <= previous_coverage <= 0.9975:
                        equity_universe_min_size = previous_row[df_date.columns.index("Full_MCAP_USD_Cutoff_Company")]

                    elif previous_coverage < 0.995:
                        min_size_company = df_date.filter(pl.col("Cumulative_Coverage_Cutoff") >= 0.995).select("Full_MCAP_USD_Cutoff_Company").head(1)
                        equity_universe_min_size = min_size_company[0, 0]
                        previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

                    else:
                        min_size_company = df_date.filter(pl.col("Cumulative_Coverage_Cutoff") >= 0.9975).select("Full_MCAP_USD_Cutoff_Company").head(1)
                        equity_universe_min_size = min_size_company[0, 0]
                        previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

                else:
                    print("OUTISDE RANGE EMS")
                    min_size_company = df_date.filter(pl.col("Cumulative_Coverage_Cutoff") >= 0.9975).select("Full_MCAP_USD_Cutoff_Company").head(1)
                    equity_universe_min_size = min_size_company[0, 0]
                    previous_rank = df_date.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size).height

        # Stack the information to the frame
        EMS_Frame = EMS_Frame.vstack(pl.DataFrame({
                                    "Date": [date],
                                    "Segment": [Segment],
                                    "EMS": [equity_universe_min_size],
                                    "Rank": [previous_rank],
                                    "Total": [len(temp_Exchanges_Securities)],
                                    "FreeFloatMCAP_Minimum_Size": [equity_universe_min_size / 2]
        }))

        if date != Starting_Date:
            # Add information about those securities that were inlcuded in the previous Screened_Universe
            Security_Level_DF = Security_Level_DF.with_columns(
                (pl.col("Internal_Number").is_in(Temp_Screened_Securities.select(pl.col("Internal_Number")))).alias("InPrevScreened_Universe")
            )

            Security_Level_DF = Security_Level_DF.with_columns(
            # Create a mask column that applies the screen only if InPrevScreened_Universe is FALSE
                pl.when(
                    (pl.col("InPrevScreened_Universe") == False) &
                    (pl.col("Full_MCAP_USD_Cutoff_Company") < equity_universe_min_size)
                )
                    .then(pl.lit(None))  # Mark as null (to exclude later)
                    .otherwise(pl.col("Full_MCAP_USD_Cutoff_Company"))
                    .alias("Full_MCAP_USD_Cutoff_Company_Screened")
                ).filter(
                    pl.col("Full_MCAP_USD_Cutoff_Company_Screened").is_not_null()
                )

        else: # Initial Date
            Security_Level_DF = Security_Level_DF.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size) 
        
    elif Segment == "Emerging":

        equity_universe_min_size = EMS_Frame.filter(pl.col("Date") == date).select(pl.col("EMS")).to_numpy()[0][0]
        final_df = df.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size)

        if date != Starting_Date:
            # Add information about those securities that were inlcuded in the previous Screened_Universe
            Security_Level_DF = Security_Level_DF.with_columns(
                (pl.col("Internal_Number").is_in(Temp_Screened_Securities.select(pl.col("Internal_Number")))).alias("InPrevScreened_Universe")
            )

            Security_Level_DF = Security_Level_DF.with_columns(
            # Create a mask column that applies the screen only if InPrevScreened_Universe is FALSE
                pl.when(
                    (pl.col("InPrevScreened_Universe") == False) &
                    (pl.col("Full_MCAP_USD_Cutoff_Company") < equity_universe_min_size)
                )
                    .then(pl.lit(None))  # Mark as null (to exclude later)
                    .otherwise(pl.col("Full_MCAP_USD_Cutoff_Company"))
                    .alias("Full_MCAP_USD_Cutoff_Company_Screened")
                ).filter(
                    pl.col("Full_MCAP_USD_Cutoff_Company_Screened").is_not_null()
                )
        else:
            Security_Level_DF = Security_Level_DF.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= equity_universe_min_size) 

    # Apply the original list of columns
    Security_Level_DF = Security_Level_DF.select(Security_Level_DF_Columns)
    
    return Security_Level_DF, EMS_Frame, Temp_Screened_Securities, equity_universe_min_size

##################################
###########MatplotLib#############
##################################
def Curve_Plotting(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR):
    # Convert the necessary columns to numpy arrays for plotting
    X_Axis = TopPercentage.select("CumWeight_Cutoff").to_numpy().flatten()
    Y_Axis = TopPercentage.select("Full_MCAP_USD_Cutoff_Company").to_numpy().flatten()

    # Convert temp_Country to numpy arrays for plotting
    X_Axis_Temp = temp_Country.select("CumWeight_Cutoff").to_numpy().flatten()
    Y_Axis_Temp = temp_Country.select("Full_MCAP_USD_Cutoff_Company").to_numpy().flatten()

    # Create the plot
    plt.figure(figsize=(12, 8))
    plt.plot(X_Axis, Y_Axis, marker='o', linestyle='-', color='b')
    plt.plot(X_Axis_Temp, Y_Axis_Temp, linestyle='-', linewidth = 0.5, color='black', label='Temp Country') 

    # Add horizontal lines for Lower_GMSR and Upper_GMSR
    plt.axhline(y=Lower_GMSR, color='r', linestyle='solid', label=f'Lower GMSR = {Lower_GMSR}')
    plt.axhline(y=Upper_GMSR, color='g', linestyle='dotted', label=f'Upper GMSR = {Upper_GMSR}')

    # Add vertical lines at 80%, 85%, and 90%
    plt.axvline(x=0.80, color='orange', linestyle='--', label='80%')
    plt.axvline(x=0.85, color='purple', linestyle='--', label='85%')
    plt.axvline(x=0.90, color='cyan', linestyle='--', label='90%')

    # Adjust limits as needed
    plt.xlim(0, 1.0)  

    # Add labels and title
    plt.xlabel("Cumulative Weight (Cutoff)")
    plt.ylabel("Full MCAP USD (Cutoff)")
    plt.title("Cumulative Weight vs Full MCAP USD Cutoff")

    # Disable scientific notation on y-axis
    plt.ticklabel_format(style='plain', axis='y')

    # Optionally, format y-axis tick labels with commas
    plt.gca().get_yaxis().set_major_formatter(plt.matplotlib.ticker.StrMethodFormatter('{x:,.0f}'))

    # Add label for the last point
    last_x = X_Axis[-1]
    last_y = Y_Axis[-1]
    label = f"Full MCAP: {last_y:,.0f}\nCumWeight: {last_x:.2%}"

    # Adjust the label and arrow to be above the last point for better visibility
    plt.text(last_x, last_y * 5.45, label, fontsize=9, verticalalignment='bottom', horizontalalignment='right', color='black')

    # Adjust the arrow to point from the middle of the label to the actual point
    plt.annotate('', xy=(last_x, last_y), xytext=(last_x, last_y * 5.5),
                 arrowprops=dict(facecolor='black', arrowstyle='->', connectionstyle='arc3,rad=0.3'))

    plt.grid(False)
    plt.tight_layout()
    # Save the figure
    chart_file = f"chart_{TopPercentage['Date'][0]}_{TopPercentage['Country'][0]}.png"
    plt.savefig(chart_file)
    plt.close()  # Close the plot to free up memory
    return chart_file

##################################
###########Chairs Rule############
##################################
def Fill_Chairs(temp_Country, Companies_To_Fill, Country_Cutoff, Country_Cutoff_Upper, Country_Cutoff_Lower):

    # Sort Companies
    temp_Country = temp_Country.sort("Full_MCAP_USD_Cutoff_Company", descending=True)

    # Check for those Companies above Country_Cutoff 1.5X
    priority1 = temp_Country.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= Country_Cutoff)

    if priority1.height >= Companies_To_Fill:
        return priority1.head(Companies_To_Fill).with_columns(
            pl.col("Shadow_Company").fill_null(True)
        )

    # Second priority: Add priority 2 to priority 1
    priority2 = temp_Country.filter(
                (pl.col("Full_MCAP_USD_Cutoff_Company") >= Country_Cutoff_Lower) & 
                (pl.col("Full_MCAP_USD_Cutoff_Company") < Country_Cutoff) & 
                (pl.col("Size") == "All_Cap")
                )
    
    if len(priority1) + len(priority2) >= Companies_To_Fill:
        TopPercentage = priority1.vstack(priority2)

        return TopPercentage.head(Companies_To_Fill).with_columns(
            pl.col("Shadow_Company").fill_null(True)
        )
    
    print(f"Not enough Securities to fill the Basket for" + country + "!")

    # Last Case where we need to pick all the remaining Securities
    priority3 = temp_Country.filter(~pl.col("ENTITY_QID").is_in(priority1.vstack(priority2).select(pl.col("ENTITY_QID"))))

    if len(priority1) + len(priority2) + len(priority3) >= Companies_To_Fill:
        TopPercentage = priority1.vstack(priority2).vstack(priority3)

        return TopPercentage.head(Companies_To_Fill).with_columns(
            pl.col("Shadow_Company").fill_null(True)
        )

##################################
##Minimum FreeFloatCountry Level##
##################################
def Minimum_FreeFloat_Country(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, Segment: pl.Utf8, Original_MCAP_Emerging, LIF_Stored):
    # Check if last Company Full_MCAP_USD_Cutoff_Company is in between Upper and Lower GMSR

    # No Buffer for the Starting Date
    if (date == Starting_Date) | (len(Output_Standard_Index.filter(pl.col("Country") == country)) == 0):

        # Case inside the box
        if (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] <= Upper_GMSR) & (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] >= Lower_GMSR):
        
            # Country_Cutoff is the Full_MCAP_USD_Cutoff_Company
            Country_Cutoff_Shadow = TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] / 2
            Country_Cutoff_Upper_Shadow = Country_Cutoff_Shadow * 1.5
            Country_Cutoff_Lower_Shadow = Country_Cutoff_Shadow * (2/3)

        # Case above the box
        elif (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] > Upper_GMSR):

            # Country_Cutoff is the Upper_GMSR
            Country_Cutoff_Shadow = Upper_GMSR / 2
            Country_Cutoff_Upper_Shadow = Country_Cutoff_Shadow * 1.5
            Country_Cutoff_Lower_Shadow = Country_Cutoff_Shadow * (2/3)

        # Case below the box
        else:

            # Country_Cutoff is the GMSR
            Country_Cutoff_Shadow = Lower_GMSR / 2
            Country_Cutoff_Upper_Shadow = Country_Cutoff_Shadow * 1.5
            Country_Cutoff_Lower_Shadow = Country_Cutoff_Shadow * (2/3)

        # Transform TopPercentage from Companies to Securities level
        TopPercentage_Securities = temp_Emerging.select(pl.col("Date", "Internal_Number", "Instrument_Name",
                                "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff")).filter(pl.col("ENTITY_QID").is_in(TopPercentage.select(
                                    pl.col("ENTITY_QID").unique()
                                ))).with_columns(
                                    pl.lit("All_Cap").alias("Size")
                                )
        
        # Calculate Country_Cutoff to remove too big Securities
        Country_Cutoff = Country_Cutoff_Shadow * 2

        # Check for Companies inside the ETFs
        TopPercentage_Securities = (
                            TopPercentage_Securities
                            .drop("Size")
                            .join(
                                ETF.select(pl.col(["Internal_Number", "Size"])),
                                on=["Internal_Number"],
                                how="left"
                            )
                        )
        
        if date == Starting_Date:
        
            # Add information for Company_Full_MCAP to the Security level Frame
            TopPercentage_Securities = TopPercentage_Securities.join(TopPercentage.select(pl.col(["ENTITY_QID", "Full_MCAP_USD_Cutoff_Company"])),
                                                                    on=["ENTITY_QID"], how="left")
            
            # Remove those Companies where Size == NULL and Full_MCAP_USD_Cutoff_Company > Country_Cutoff
            TopPercentage_Securities = TopPercentage_Securities.filter(~(pl.col("Size").is_null()) & 
                                        (pl.col("Full_MCAP_USD_Cutoff_Company") > Country_Cutoff)).drop("Full_MCAP_USD_Cutoff_Company")
        
        # Check for Shadow_Company
        TopPercentage_Securities = TopPercentage_Securities.with_columns(
                                    pl.when(pl.col("Size").is_not_null())
                                    .then(False)
                                    .otherwise(
                                        pl.when(pl.col("Free_Float_MCAP_USD_Cutoff") >= Country_Cutoff_Lower_Shadow)
                                        .then(False)
                                        .otherwise(True)
                                    ).alias("Shadow_Company")).with_columns(
                                        pl.lit("All_Cap").alias("Size")
                                    )

        # Check that there are at least 3 Companies
        if len(temp_Emerging.filter(pl.col("Country") == country)) >= 3:

            # Check number of Current Securities
            if len(TopPercentage_Securities.filter(pl.col("Shadow_Company") == False)) < 3:
                # Keep only Non-Shadow Securities
                TopPercentage_Securities = TopPercentage_Securities.filter(pl.col("Shadow_Company") == False).with_columns(
                    pl.lit("All_Cap").alias("Size"),
                    pl.lit("Index_Creation").alias("Case")
                    )

                # Check for Index Continuity
                TopPercentage_Securities_Addition = temp_Emerging.filter(pl.col("Country") == country).sort("Free_Float_MCAP_USD_Cutoff", descending=True).select(pl.col([
                    "Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"
                ])).with_columns(
                    pl.lit("All_Cap").alias("Size"),
                    pl.lit("Index_Creation").alias("Case"),
                    pl.lit(False).alias("Shadow_Company")
                ).filter(~pl.col("Internal_Number").is_in(TopPercentage_Securities.select(pl.col("Internal_Number")))).sort("Free_Float_MCAP_USD_Cutoff",
                                                                                                                            descending=True)

                # Stack the two Frames
                TopPercentage_Securities = TopPercentage_Securities.vstack(TopPercentage_Securities_Addition.head(3 - len(TopPercentage_Securities))
                                                                           .select(TopPercentage_Securities.columns)) 
        
        # In case there are not enough Companies
        else:
            TopPercentage_Securities = TopPercentage_Securities.head(0)
            TopPercentage = TopPercentage.head(0)

        TopPercentage = TopPercentage_Securities.group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                    ]).with_columns(
                                                        pl.lit("All_Cap").alias("Size"),
                                                        pl.lit("Index Creation").alias("Case")
                                                    )

    else: 
        # Buffer for Companies
        Companies_To_Fill = TopPercentage.height

        # Create the list of Dates
        Dates_List = Pivot_TOR.index.to_list()
        Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

        # Information at Security Level for Current Country Index transposed into Company Level
        QID_Standard_Index = Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(
            pl.col("ENTITY_QID", "Shadow_Company", "Internal_Number"))

        # Information at Security Level for Current Country Index transposed into Company Level
        QID_Small_Index = Small_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(
            pl.col("ENTITY_QID", "Shadow_Company", "Internal_Number", "Country"))

        # Get which of the Current Index Components are still Investable by checking temp_Emerging/temp_Developed after Screens have been applied to them
        if Segment == "Emerging":
            Security_Standard_Index_Current = QID_Standard_Index.join(temp_Emerging.select(pl.col(["Internal_Number", "Country"])),
                on=["Internal_Number"], how="left")
            
            # Small #
            Security_Small_Index_Current = QID_Small_Index.join(temp_Emerging.select(pl.col(["Internal_Number", "Country"])),
                on=["Internal_Number"], how="left")
        
        # Group them by ENTITY_QID
        Company_Standard_Index_Current = Security_Standard_Index_Current.group_by(
                                                    ["ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Shadow_Company").first().alias("Shadow_Company"),
                                                    ]).with_columns(
                                                        pl.lit("All_Cap").alias("Size")
                                                    )
        
        Company_Small_Index_Current = Security_Small_Index_Current.group_by(
                                            ["ENTITY_QID"]).agg([
                                                pl.col("Country").first().alias("Country"),
                                                pl.col("Shadow_Company").first().alias("Shadow_Company"),
                                            ]).with_columns(
                                                        pl.lit("SMALL").alias("Size"),
                                                        pl.col("Shadow_Company").fill_null(False)
                                                    )
        
        # Create the Current All_Cap Index
        Current_Index = Company_Standard_Index_Current.vstack(Company_Small_Index_Current)

        # Add information of Standard/Small Companies to Refreshed Universe
        temp_Country = temp_Country.join(Current_Index.select(pl.col(["ENTITY_QID", "Shadow_Company", "Size"])), on=["ENTITY_QID"], how="left").with_columns(
            pl.col("Size").fill_null("NEW")
        )
        
        #################
        # Case Analysis #
        #################

        # Case inside the box
        if (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] <= Upper_GMSR) & (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] >= Lower_GMSR):

            if ((date.month == 3 or date.month == 9)) and (date < datetime.date(2023,3,20)):

                # Country_GMSR
                Country_Cutoff = TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0]
                Country_Cutoff_Upper = Country_Cutoff * 1.8
                Country_Cutoff_Lower = Country_Cutoff * (0.5)

            else:

                # Country_GMSR
                Country_Cutoff = TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0]
                Country_Cutoff_Upper = Country_Cutoff * 1.5
                Country_Cutoff_Lower = Country_Cutoff * (2/3)

        # Case above the box
        elif (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] > Upper_GMSR):
            
            if ((date.month == 3 or date.month == 9)) and (date < datetime.date(2023,3,20)):

                # Country_GMSR
                Country_Cutoff = TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0]
                Country_Cutoff_Upper = Country_Cutoff * 1.8
                Country_Cutoff_Lower = Country_Cutoff * (0.5)

            else:

                # Country_GMSR is the Upper_GMSR
                Country_Cutoff = Upper_GMSR
                Country_Cutoff_Upper = Country_Cutoff * 1.5
                Country_Cutoff_Lower = Country_Cutoff * (2/3)

        # Case below the box
        else:

            if ((date.month == 3 or date.month == 9)) and (date < datetime.date(2023,3,20)):

                # Country_GMSR
                Country_Cutoff = TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0]
                Country_Cutoff_Upper = Country_Cutoff * 1.8
                Country_Cutoff_Lower = Country_Cutoff * (0.5)

            else:
                
                # Country_GMSR is the GMSR
                Country_Cutoff = Lower_GMSR
                Country_Cutoff_Upper = Country_Cutoff * 1.5
                Country_Cutoff_Lower = Country_Cutoff * (2/3)

        # Eligibility Rule for All_Cap Index #
        TopPercentage = Fill_Chairs(temp_Country, Companies_To_Fill, Country_Cutoff, Country_Cutoff_Upper, Country_Cutoff_Lower)

        if ((date.month == 3 or date.month == 9)) and (date < datetime.date(2023,3,20)):

            # Transform TopPercentage from Companies to Securities level
            TopPercentage_Securities = temp_Emerging.select(pl.col("Date", "Internal_Number", "Instrument_Name",
                                    "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff")).filter(pl.col("ENTITY_QID").is_in(TopPercentage.select(
                                        pl.col("ENTITY_QID").unique()
                                    ))).with_columns(
                                        pl.lit("All_Cap").alias("Size")
                                    )
            
            # Add information of what was Shadow in Previous Index
            TopPercentage_Securities = TopPercentage_Securities.join(
                                                                Standard_Index
                                                                .filter((pl.col("Date") == Previous_Date) & (pl.col("Country") == country))
                                                                .select(pl.col(["Internal_Number", "Shadow_Company"])),
                                                                on=["Internal_Number"],
                                                                how="left"
                                                            )
            
            # Add LIF columns
            temp_LIF = LIF_Stored.filter(pl.col("Date") == date).with_columns(pl.lit(date).alias("Date"))
            TopPercentage_Securities = TopPercentage_Securities.join(temp_LIF, on=["Date", "Internal_Number"], how="left")

            # Verify for Shadow Securities
            TopPercentage_Securities = TopPercentage_Securities.with_columns(
                    pl.when(pl.col("Shadow_Company") == False)  # Condition: Shadow_Company is False
                    .then((pl.col("Free_Float_MCAP_USD_Cutoff") / pl.col("LIF")) < (Country_Cutoff * 0.5 * (2 / 3)))  # Condition for False case
                    .otherwise(pl.col("Free_Float_MCAP_USD_Cutoff") < (Country_Cutoff * 0.5))  # Condition for True case
                    .alias("Update_Shadow_Company")  # Name of the new column
                ).drop("Shadow_Company").rename({"Update_Shadow_Company": "Shadow_Company"})

            # Variables Shadow for Index Continuity
            Country_Cutoff_Shadow = Country_Cutoff / 2
            Country_Cutoff_Upper_Shadow = Country_Cutoff_Shadow * 1.5
            Country_Cutoff_Lower_Shadow = Country_Cutoff_Shadow * (2/3)

            # Check that there are at least 3 Companies
            if len(temp_Emerging.filter(pl.col("Country") == country)) >= 3:

                # Check for Index Continuity
                TopPercentage_Securities, TopPercentage = Index_Continuity(TopPercentage_Securities, TopPercentage, "Emerging", temp_Emerging, country, Standard_Index)

            else:

                TopPercentage_Securities = TopPercentage_Securities.head(0)
                TopPercentage = TopPercentage.head(0)

            # Adjust the columns
            TopPercentage_Securities = TopPercentage_Securities.with_columns(
                pl.lit("All_Cap").alias("Size"),
                pl.lit("Buffer").alias("Case")
            )

            TopPercentage = TopPercentage.with_columns(
                pl.lit("Buffer").alias("Case"),
                pl.lit("All_Cap").alias("Size")
            )

            # Rule for moving All_Cap Securities into Small Index #

            # Check what was previously All_Cap (and Non-Shadow) - Company Level
            TopPercentage = TopPercentage.with_columns(
                    pl.col("ENTITY_QID").is_in(
                        Standard_Index.filter((pl.col("Date") == Previous_Date) & (pl.col("Country") == country) & (pl.col("Shadow_Company") == False)).select(pl.col("ENTITY_QID"))
                    ).alias("Previously_All_Cap")
                )
            
            # In case Index Continuity kicked in
            if "Full_MCAP_USD_Cutoff_Company" not in TopPercentage.columns:
                if Segment == "Emerging":
                    TopPercentage = TopPercentage.join(Original_MCAP_Emerging, on="ENTITY_QID", how="left").join(TopPercentage_Securities.select(pl.col(["ENTITY_QID", "Shadow_Company"])), on=["ENTITY_QID"],
                                                                                                                how="left")
                else:
                    TopPercentage = TopPercentage.join(Original_MCAP_Developed, on="ENTITY_QID", how="left").join(TopPercentage_Securities.select(pl.col(["ENTITY_QID", "Shadow_Company"])), on=["ENTITY_QID"],
                                                                                                                how="left")
            
            # Filter out New_Shadow which where Standard before
            TopPercentage = TopPercentage.filter(
                                        ~((pl.col("Shadow_Company") == True) & 
                                        (pl.col("Previously_All_Cap") == True) & 
                                        (pl.col("Full_MCAP_USD_Cutoff_Company") >= Country_Cutoff_Lower) & 
                                        (pl.col("Full_MCAP_USD_Cutoff_Company") <= Country_Cutoff))
                                    ).drop(["Previously_All_Cap", "Shadow_Company"])

            # Convert from Company level to Security level
            TopPercentage_Securities = TopPercentage_Securities.filter(pl.col("ENTITY_QID").is_in(TopPercentage.select(pl.col("ENTITY_QID"))))

        else:
        
            # Find if there are new Companies in the Yellow Box
            TopPercentage = TopPercentage.with_columns(
                pl.lit(None).alias("ELIGIBLE")
            )

            # Check if there are NEW Securities in the Yellow Box
            if len(TopPercentage.filter((pl.col("Full_MCAP_USD_Cutoff_Company") >= Country_Cutoff) &
                                (pl.col("Full_MCAP_USD_Cutoff_Company") <= Country_Cutoff_Upper) &
                                (pl.col("Size") == "NEW"))) > 0:
                
                # Check how many Current componets (Previous Index) have fallen below Country Cutoff 2/3
                Fallen_Current_Companies = temp_Country.filter((pl.col("Size") == "All_Cap") & (pl.col("Full_MCAP_USD_Cutoff_Company") < Country_Cutoff_Lower)).height

                # Isolate the NEW Companies in the Yellow box
                Yellow_Box_Companies = TopPercentage.filter((pl.col("Full_MCAP_USD_Cutoff_Company") >= Country_Cutoff) &
                                (pl.col("Full_MCAP_USD_Cutoff_Company") <= Country_Cutoff_Upper) &
                                (pl.col("Size") == "NEW"))
                
                # Remove them from TopPercentage
                TopPercentage = TopPercentage.filter(~pl.col("ENTITY_QID").is_in(Yellow_Box_Companies.select(pl.col("ENTITY_QID")))).with_columns(
                    pl.lit(True).alias("ELIGIBLE")
                )

                # Apply the logic for Eligibility
                if Fallen_Current_Companies > 0:
                    Yellow_Box_Companies = Yellow_Box_Companies.with_columns(
                        pl.when(pl.arange(0, pl.count()) < Fallen_Current_Companies)  # Set True for top `Fallen_Current_Companies` rows
                        .then(True)
                        .otherwise(False)
                        .alias("ELIGIBLE")
                    )
                else:
                    Yellow_Box_Companies = Yellow_Box_Companies.with_columns(
                        pl.lit(False).alias("ELIGIBLE")
                    )

                # Stack Frames
                TopPercentage = TopPercentage.vstack(Yellow_Box_Companies).sort("Full_MCAP_USD_Cutoff_Company", descending=True)

            else:

                # In case there are no NEW Securities in the Yellow Box
                TopPercentage = TopPercentage.with_columns(
                                        pl.when(
                                            (pl.col("Full_MCAP_USD_Cutoff_Company") >= Country_Cutoff) &
                                            (pl.col("Full_MCAP_USD_Cutoff_Company") <= Country_Cutoff_Upper) &
                                            (pl.col("Size") == "NEW")
                                        )
                                        .then(pl.lit(False))
                                        .otherwise(pl.lit(True))
                                        .alias("ELIGIBLE")
                                    )
                
            # Transform TopPercentage to Security level
            TopPercentage_Securities = temp_Emerging.select(pl.col("Date", "Internal_Number", "Instrument_Name",
                                    "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff")).filter(pl.col("ENTITY_QID").is_in(TopPercentage.select(
                                        pl.col("ENTITY_QID").unique()
                                    )))
            
            # Add ELIGIBLE information
            TopPercentage_Securities = TopPercentage_Securities.join(TopPercentage.select(["ENTITY_QID", "ELIGIBLE"]), on="ENTITY_QID", how="left").sort("Full_MCAP_USD_Cutoff", descending=True)
            
            # Add SHADOW information from Previous Standard_Index
            TopPercentage_Securities = TopPercentage_Securities.join(Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(
                ["Internal_Number", "Shadow_Company"]), on=["Internal_Number"], how="left")
            
            # Ensure that there is not Empty Shadow_Company column
            TopPercentage_Securities = TopPercentage_Securities.with_columns(
                                        pl.col("Shadow_Company").fill_null(True).alias("Shadow_Company")
                                        )
            
            # Add LIF columns
            temp_LIF = LIF_Stored.filter(pl.col("Date") == date).with_columns(pl.lit(date).alias("Date"))
            TopPercentage_Securities = TopPercentage_Securities.join(temp_LIF, on=["Date", "Internal_Number"], how="left")

            # Verify for Shadow Securities
            TopPercentage_Securities = TopPercentage_Securities.with_columns(
                                        pl.when((pl.col("Shadow_Company") == False) & (pl.col("ELIGIBLE") == True))
                                        .then((pl.col("Free_Float_MCAP_USD_Cutoff") / pl.col("LIF")) < (Country_Cutoff * 0.5 * (2 / 3)))
                                        .when((pl.col("Shadow_Company") == True) & (pl.col("ELIGIBLE") == True))
                                        .then(pl.col("Free_Float_MCAP_USD_Cutoff") < (Country_Cutoff * 0.5))
                                        .otherwise(True)  # Set to True if none of the conditions match
                                        .alias("Update_Shadow_Company")
                                    ).drop("Shadow_Company", "ELIGIBLE").rename({"Update_Shadow_Company": "Shadow_Company"})

            # Adapt TopPercentage
            TopPercentage = TopPercentage_Securities.group_by(["Date", "ENTITY_QID"]).agg([
                    pl.col("Country").first().alias("Country")
                ]).with_columns([
                    pl.lit("All_Cap").alias("Size"),
                    pl.lit("Maintenance").alias("Case")
                ])

            # Variables Shadow for Index Continuity
            Country_Cutoff_Shadow = Country_Cutoff / 2
            Country_Cutoff_Upper_Shadow = Country_Cutoff_Shadow * 1.5
            Country_Cutoff_Lower_Shadow = Country_Cutoff_Shadow * (2/3)

            # Check that there are at least 3 Companies
            if len(temp_Emerging.filter(pl.col("Country") == country)) >= 3:

                # Check for Index Continuity
                TopPercentage_Securities, TopPercentage = Index_Continuity(TopPercentage_Securities, TopPercentage, "Emerging", temp_Emerging, country, Standard_Index)

            else:

                TopPercentage_Securities = TopPercentage_Securities.head(0)
                TopPercentage = TopPercentage.head(0)

            # Adjust the columns
            TopPercentage_Securities = TopPercentage_Securities.with_columns(
                pl.lit("All_Cap").alias("Size"),
                pl.lit("Buffer").alias("Case")
            )

            TopPercentage = TopPercentage.with_columns(
                pl.lit("Buffer").alias("Case"),
                pl.lit("All_Cap").alias("Size")
            )

    # Return the Frame
    return TopPercentage, TopPercentage_Securities

##################################
##########Index Creation##########
##################################
def Index_Creation_Box(Frame: pl.DataFrame, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap, Percentage, Right_Limit, Left_Limit, Segment: pl.Utf8, writer):

    temp_Country = Frame.filter((pl.col("Date") == date) & (pl.col("Country") == country))

    # Sort in each Country the Companies by Full MCAP USD Cutoff
    temp_Country = temp_Country.sort("Full_MCAP_USD_Cutoff_Company", descending=True)

    # No Country_Adjustment as we are building All_Cap
    Country_Percentage = Percentage

    # Calculate their CumWeight_Cutoff
    temp_Country = temp_Country.with_columns(
                    (pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Weight_Cutoff"),
                    (((pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).cum_sum())).alias("CumWeight_Cutoff")
    )

    # Country Adjustment
    if Coverage_Adjustment == True:
        Country_Adjustment = Country_Coverage.filter(pl.col("Country") == country).select(pl.col("Coverage")).to_numpy()[0][0]
    else:
        Country_Adjustment = 1

    # Check where the top 99% (crossing it) lands us on the Curve
    TopPercentage = (
    temp_Country
    .select([
        "Date", 
        "Internal_Number", 
        "Instrument_Name", 
        "ENTITY_QID", 
        "Country", 
        "Free_Float_MCAP_USD_Cutoff_Company",
        "Full_MCAP_USD_Cutoff_Company", 
        "Weight_Cutoff", 
        "CumWeight_Cutoff"
    ])
    .filter(pl.col("CumWeight_Cutoff") <= (Country_Percentage / Country_Adjustment))
    .vstack(
        temp_Country
        .select([
            "Date", 
            "Internal_Number", 
            "Instrument_Name", 
            "ENTITY_QID", 
            "Country", 
            "Free_Float_MCAP_USD_Cutoff_Company", 
            "Full_MCAP_USD_Cutoff_Company", 
            "Weight_Cutoff", 
            "CumWeight_Cutoff"
        ])
        .filter(pl.col("CumWeight_Cutoff") > (Country_Percentage / Country_Adjustment))
        .head(1)
        )
    )

    # Check that the last Company is not below the Lower_GMSR
    if TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] < Lower_GMSR:
        TopPercentage = TopPercentage.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= Lower_GMSR)

    #### This check should be at Security Level ###
    #### It goes at very last end ###

    # Check that minimum number is respected
    if Segment == "Developed":
        if len(TopPercentage) < 5: 
            TopPercentage = temp_Country.head(5)
            TopPercentage = TopPercentage.with_columns(
                pl.lit("Minimum Number of Companies").alias("Case"),
                pl.lit("Reintroduction").alias("Size")
            ).select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff_Company",
                            "Full_MCAP_USD_Cutoff_Company", "Weight_Cutoff", "CumWeight_Cutoff", "Size", "Case"]))
    elif Segment == "Emerging":
        if len(TopPercentage) < 3: 
            TopPercentage = temp_Country.head(3)
            TopPercentage = TopPercentage.with_columns(
                pl.lit("Index Creation").alias("Case"),
                pl.lit("Minimum Number of Companies").alias("Size")
            ).select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff_Company",
                            "Full_MCAP_USD_Cutoff_Company", "Weight_Cutoff", "CumWeight_Cutoff", "Size", "Case"]))
            
    # Add Case/Size
    TopPercentage = TopPercentage.with_columns(
                    pl.lit("All_Cap").alias("Size"),
                    pl.lit("Index Creation").alias("Case")
    )

    return TopPercentage, temp_Country

##################################
########Index Rebalancing#########
##################################
def Index_Rebalancing_Box(Frame: pl.DataFrame, SW_ACALLCAP, Output_Count_Standard_Index, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap,  Right_Limit, Left_Limit, Segment: pl.Utf8, writer):
    temp_Country = Frame.filter((pl.col("Date") == date) & (pl.col("Country") == country))

    # Sort in each Country the Companies by Full MCAP USD Cutoff
    temp_Country = temp_Country.sort("Full_MCAP_USD_Cutoff_Company", descending=True)
    
    # Calculate their CumWeight_Cutoff
    temp_Country = temp_Country.with_columns(
                    (pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Weight_Cutoff"),
                    (((pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).cum_sum())).alias("CumWeight_Cutoff")
                    ).sort("Full_MCAP_USD_Cutoff_Company", descending=True)

    # Create the list of Dates
    Dates_List = Pivot_TOR.index.to_list()
    Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

    # Information at Company Level
    QID_Standard_Index = Output_Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(pl.col("Date", "ENTITY_QID"))

    # Check which of the QID_Standard_Index Companies is still alive (it is not relevant if it does not pass the Screens)
    # Duplicates are dropped due to ENTITY_QID / Keep only Free_Float_MCAP_USD_Cutoff > 0
    QID_Standard_Index = QID_Standard_Index.join(Emerging.filter((pl.col("Country")==country) & (pl.col("Date")==date)).select(pl.col("Free_Float_MCAP_USD_Cutoff",
                        "ENTITY_QID")), on=["ENTITY_QID"], how="left").unique(subset=["ENTITY_QID"]).filter(pl.col("Free_Float_MCAP_USD_Cutoff") > 0)

    ######################################
    # Adjustment Company_Selection_Count #
    ######################################

    # Sort Old Universe by FullMCAPUSDCutoff by using # of Shares and FreeFloat
    def ChairsSorting(Output_Standard_Index, Standard_Index, Securities_Cutoff, country, Previous_Date, date, FX_Cutoff):

        # Filter for Country and Previous_Date Review at Security_Level #
        # AllCapIndex from Previous Quarter #        
        Starting_Universe = Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(["Date", "Internal_Number", 
                                                                                                                         "ENTITY_QID", "Instrument_Name", "Country"])

        ############
        ## Step 1 ##
        ############

        # Update the FullMCAP_USD as of the new Cutoff date, by refreshing Price & FX_Rate but not Number of Shares which remain same as Previous Quarter #

        # Filter Price_Cutoff as of Current Cutoff
        Price_Cutoff = Securities_Cutoff.filter(pl.col("Review") == date).select(["Review", "validDate", "stoxxId","closePrice", "currency"]).rename({"closePrice": "closePrice_Cutoff"})

        # Add FX_Cutoff as of Previous_Date Review
        Price_FX_Cutoff = Price_Cutoff.join(FX_Cutoff, left_on=["currency", "validDate"], right_on=["Currency", "Cutoff"],
                                                             how="left")
        
        # Retrieve Number of Shares as Previous Quarter
        SharesPreviousQuarter = Securities_Cutoff.filter(pl.col("Review") == Previous_Date).select(["Review", "validDate", "stoxxId", "shares"]).rename({"shares": "shares_previous_quarter"}).select(
            ["stoxxId", "shares_previous_quarter"]
        )

        # Add SharesPreviousQuarter to Price_FX_Cutoff
        Price_FX_Cutoff = Price_FX_Cutoff.join(SharesPreviousQuarter, on=["stoxxId"], how="left")

        # Calculate the FullMCAP as of current Cutoff
        Price_FX_Cutoff = Price_FX_Cutoff.with_columns(
                                                                 (pl.col("closePrice_Cutoff") * pl.col("shares_previous_quarter") * pl.col("FX_local_to_Index_Currency_Cutoff")
                                                                  ).alias("Full_MCAP_USD_Cutoff_Shares_Previous_Quarter")
                                                             )
        
        # Add the Information of FullMCAP to the Current Composition Index
        Starting_Universe = Starting_Universe.join(Price_FX_Cutoff.select(pl.col(["stoxxId", "Full_MCAP_USD_Cutoff_Shares_Previous_Quarter"])),
                                                     left_on=["Internal_Number"], right_on=["stoxxId"], how="left")
        
        # Add the Full_MCAP_USD_Cutoff_Shares_Previous_Quarter sum at the ENTITY_QID level
        Starting_Universe = Starting_Universe.with_columns(
                            Starting_Universe
                            .group_by("ENTITY_QID")
                            .agg(pl.col("Full_MCAP_USD_Cutoff_Shares_Previous_Quarter").sum().alias("Full_MCAP_USD_Cutoff_Shares_Previous_Quarter_Company"))
                            .join(Starting_Universe, on="ENTITY_QID", how="right")
                            .get_column("Full_MCAP_USD_Cutoff_Shares_Previous_Quarter_Company")
                        ).sort("Full_MCAP_USD_Cutoff_Shares_Previous_Quarter_Company", descending = True)
        
        # Add information to Output_Standard_Index to check if any Company has bankrupted
        temp_Output_Standard_Index = (Output_Standard_Index.filter((pl.col("Country")==country) & ((pl.col("Date") == Previous_Date)))
                                      .join(Starting_Universe.unique(subset=["ENTITY_QID"]).select(["ENTITY_QID", "Full_MCAP_USD_Cutoff_Shares_Previous_Quarter_Company"]),
                                                                on="ENTITY_QID", how="left"))
        
        # Count the Chairs from the previous Index
        ChairsCountPrevDate = temp_Output_Standard_Index.filter((pl.col("Country")==country) & ((pl.col("Date") == Previous_Date)) &
                                                           (pl.col("Full_MCAP_USD_Cutoff_Shares_Previous_Quarter_Company") > 0)).height
        
        # Specific condition that would apply only for 2012 given the strarting point of ETF for Standard Segment
        if ChairsCountPrevDate == 0:
            ChairsCountPrevDate = temp_Output_Standard_Index.filter((pl.col("Country")==country) & ((pl.col("Date") == Previous_Date))).height
            
        # Get the position of the Xth Company and the relative FullMCAP
        PositionFullMCAPFirstStep = (Starting_Universe.select(["ENTITY_QID", "Full_MCAP_USD_Cutoff_Shares_Previous_Quarter_Company"])
                                    .unique().select(["Full_MCAP_USD_Cutoff_Shares_Previous_Quarter_Company"])
                                    .sort("Full_MCAP_USD_Cutoff_Shares_Previous_Quarter_Company", descending=True)
                                    .head(ChairsCountPrevDate).tail(1).to_numpy()[0][0])

        ############
        ## Step 2 ##
        ############

        # Refresh the Number of Shares as of Cutoff
        SharesCutoff = Securities_Cutoff.filter(pl.col("Review") == date).select(["Review", "validDate", "stoxxId", "shares"]).rename({"shares": "shares_cutoff"}).select(
            ["stoxxId", "shares_cutoff"]
        )

        # Add SharesPreviousQuarter to Price_FX_Cutoff
        Price_FX_Cutoff = Price_FX_Cutoff.join(SharesCutoff, on=["stoxxId"], how="left")

        # Calculate the FullMCAP as of current Cutoff
        Price_FX_Cutoff = Price_FX_Cutoff.with_columns(
                                                                 (pl.col("closePrice_Cutoff") * pl.col("shares_cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff")
                                                                  ).alias("Full_MCAP_USD_Cutoff_Current_Shares")
                                                             )
        
        # Add the Information of FullMCAP to the Current Composition Index
        Starting_Universe = Starting_Universe.join(Price_FX_Cutoff.select(pl.col(["stoxxId", "Full_MCAP_USD_Cutoff_Current_Shares"])),
                                                     left_on=["Internal_Number"], right_on=["stoxxId"], how="left")
        
        # Add the Full_MCAP_USD_Cutoff_Current_Shares sum at the ENTITY_QID level
        Starting_Universe = Starting_Universe.with_columns(
                            Starting_Universe
                            .group_by("ENTITY_QID")
                            .agg(pl.col("Full_MCAP_USD_Cutoff_Current_Shares").sum().alias("Full_MCAP_USD_Cutoff_Current_Shares_Company"))
                            .join(Starting_Universe, on="ENTITY_QID", how="right")
                            .get_column("Full_MCAP_USD_Cutoff_Current_Shares_Company")
                        ).sort("Full_MCAP_USD_Cutoff_Current_Shares_Company", descending = True)
        
        # Get the position of the Xth Company and the relative FullMCAP
        PositionFullMCAPSecondStep = (Starting_Universe.select(["ENTITY_QID", "Full_MCAP_USD_Cutoff_Current_Shares_Company"])
                                    .unique().select(["Full_MCAP_USD_Cutoff_Current_Shares_Company"])
                                    .sort("Full_MCAP_USD_Cutoff_Current_Shares_Company", descending=True)
                                    .head(ChairsCountPrevDate).tail(1).to_numpy()[0][0])
        
        return PositionFullMCAPSecondStep

    # Check what is the Cutoff to determine the Initial number of Chairs
    PositionFullMCAP = ChairsSorting(Output_Standard_Index, Standard_Index, Securities_Cutoff, country, Previous_Date, date, FX_Cutoff)

    # Take the initial Selection
    TopPercentage = temp_Country.sort("Full_MCAP_USD_Cutoff_Company", descending=True).filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= PositionFullMCAP)

    # Determine the Proximity Areas #
    Lower_Proximity_Bound = (Lower_GMSR, GMSR_Frame.select(["GMSR_Emerging", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0] * 0.575)
    Upper_Proximity_Bound = (GMSR_Frame.select(["GMSR_Emerging", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0], Upper_GMSR)

    # ################################
    # ############# APG ##############
    # ################################

    # # Determine the Proximity Areas #
    # Lower_Proximity_Bound = (Lower_GMSR, GMSR_Frame.select(["GMSR_Developed", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0] * 0.575)
    # Upper_Proximity_Bound = (GMSR_Frame.select(["GMSR_Developed", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0], Upper_GMSR)

    # Analysis Variables
    Last_FullMcap = TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0]
    Last_CumWeight = TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0]

    # Check which of the Companies in the Refreshed Universe are part of the Current Index
    TopPercentage = TopPercentage.with_columns(
        pl.col("ENTITY_QID").is_in(QID_Standard_Index.select(pl.col("ENTITY_QID"))).alias("Currently_Standard")
    )

    # Verify if the last Company Full_MCAP is higher than the 0.5X GMSR
    if Last_FullMcap < Lower_GMSR:
        Company_Selection_Count = TopPercentage.filter(~((pl.col("Currently_Standard") == False) & (pl.col("Full_MCAP_USD_Cutoff_Company") < Lower_GMSR))).height

        # Re-assign the correct number of Chairs
        TopPercentage = temp_Country.head(Company_Selection_Count)

    else:
        # Remove column not needed
        TopPercentage = TopPercentage.drop("Currently_Standard")

    # Re-Calculate Analysis Variables in case TopPercentage has decreased
    Last_FullMcap = TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0]
    Last_CumWeight = TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0]

    # Adjust the Left & Right Limit based on each Country
    Country_Adjustment =  Percentage

    #################
    # Case Analysis #
    #################

    ############
    ############
    #No Changes#    
    ############
    ############

    # Between Upper and Lower GMSR #
    if (
            (Lower_GMSR <= Last_FullMcap <= Upper_GMSR and Left_Limit <= TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0] <= Right_Limit) or  # Between Left and Right Limit
            (Lower_Proximity_Bound[0] <= Last_FullMcap <= Lower_Proximity_Bound[1] and Last_CumWeight <= Right_Limit) or  # Lower Proximity
            (Upper_Proximity_Bound[0] <= Last_FullMcap <= Upper_Proximity_Bound[1] and Last_CumWeight >= Left_Limit)  # Upper Proximity
        ):

            TopPercentage = TopPercentage.with_columns(
                    pl.lit("Standard").alias("Size")
            )
    
            TopPercentage = TopPercentage.with_columns(
                                        pl.lit("No_Changes").alias("Case")
            )

            # Terminate the current case
            return TopPercentage, temp_Country

    ############
    ############
    # Addition #
    ############
    ############

    elif ((Last_FullMcap >= Upper_GMSR and Last_CumWeight >= Left_Limit) or # Last Company FMCAP is higher than GMSR 1.15X and Coverage is more than the Left_Limit
        (Lower_Proximity_Bound[1] < Last_FullMcap < Upper_GMSR and Last_CumWeight < Left_Limit) or # Last Company FMCAP is lower than GMSR 1.15X but higher than Lower Proximity GMSR 0.575X 
                                                                                                    # and Market Coverage Target Range is lower than Left_Limit
        (Last_FullMcap >= Upper_GMSR and Last_CumWeight < Left_Limit)): # Last Company FMCAP is higher than GMSR 1.15X and Coverage is less than the Left_Limit

        def Additions_Rebalancing(temp_Country, Upper_GMSR, Lower_GMSR, Left_Limit, Lower_Proximity_Bound, Last_FullMcap, Last_CumWeight):
            # Case One: Full MCAP > Upper_GMSR and CumWeight > Left_Limit
            if Last_FullMcap > Upper_GMSR and Last_CumWeight > Left_Limit:
                TopPercentage = temp_Country.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= Upper_GMSR).with_columns(
                    pl.lit("Standard").alias("Size"),
                    pl.lit("Addition").alias("Case")
                )
                return TopPercentage, temp_Country

            # Case Two: CumWeight < Left_Limit
            elif Last_CumWeight < Left_Limit:
                # Add all the Companies higher than 1.15X GMSR
                TopPercentage = temp_Country.filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= Upper_GMSR)

                if (len(TopPercentage) > 0 and 
                    TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0] > Left_Limit): # Check if at least there is one Company
                    # If the condition is met, stop
                    TopPercentage = TopPercentage.with_columns(
                        pl.lit("Standard").alias("Size"),
                        pl.lit("Addition").alias("Case")
                    )
                    return TopPercentage, temp_Country
                else:
                    # Iterate to find the appropriate Count
                    for Count in range(1, len(temp_Country) + 1):
                        TopPercentage = temp_Country.head(Count)
                        last_cum_weight = TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0]
                        last_full_mcap = TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0]

                        # Check the conditions
                        if last_cum_weight >= Left_Limit or last_full_mcap <= Lower_Proximity_Bound[1]:
                            TopPercentage = TopPercentage.with_columns(
                                pl.lit("Standard").alias("Size"),
                                pl.lit("Addition").alias("Case")
                            )
                            break  # Exit the loop but continue with additional conditions

                    # Case Three: Check additional conditions
                    if ((TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] > Upper_GMSR) and # Check if FullMCAP is higher than 1.15X GMSR
                        (TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0] < Left_Limit)): # Check if Market Coverage is lower than Left_Limit

                        additional_filter = temp_Country.filter(
                            (pl.col("CumWeight_Cutoff") > Left_Limit) &
                            (pl.col("Full_MCAP_USD_Cutoff_Company") > Lower_Proximity_Bound[1]) &
                            (~pl.col("Internal_Number").is_in(TopPercentage.select(pl.col("Internal_Number"))))
                        ).with_columns(
                                pl.lit("Standard").alias("Size"),
                                pl.lit("Addition").alias("Case")
                            )

                        if len(additional_filter) > 0:
                            TopPercentage_Extension = additional_filter.head(1)
                            TopPercentage = TopPercentage.vstack(TopPercentage_Extension).with_columns(
                                pl.lit("Standard").alias("Size"),
                                pl.lit("Addition").alias("Case")
                            )
                        else:
                            # Default case: Return the top portion
                            TopPercentage = TopPercentage.with_columns(
                                pl.lit("Standard").alias("Size"),
                                pl.lit("Addition").alias("Case")
                            )

                    else:
                        # Remove the last Company breaking one of the two conditions in loop statement
                        TopPercentage = TopPercentage.head(TopPercentage.height - 1)

                    # Final return after all evaluations
                    return TopPercentage, temp_Country

        TopPercentage, temp_Country = Additions_Rebalancing(temp_Country, Upper_GMSR, Lower_GMSR, Left_Limit, Lower_Proximity_Bound, Last_FullMcap, Last_CumWeight)

    ############
    ############
    # Deletion #
    ############
    ############

    elif ((Last_FullMcap < Lower_GMSR) or # Last Company FMCAP is lower than GMSR 0.50X.
    (Lower_GMSR < Last_FullMcap < Upper_Proximity_Bound[0] and Last_CumWeight > Right_Limit)): # Last Company FMCAP is lower than GMSR 1.15X but higher than Lower GMSR 0.50X 
                                                                                               # and Market Coverage Target Range is higher than Right_Limit

        def Deletions(temp_Country, TopPercentage, Upper_GMSR, Lower_GMSR, Left_Limit, Lower_Proximity_Bound, Last_FullMcap, Last_CumWeight):

            # Sort by FullMCAP
            TopPercentage = TopPercentage.sort("Full_MCAP_USD_Cutoff_Company", descending = True)

            # Minimum Deletion is always 2
            if math.ceil(0.05 * TopPercentage.height) < 2:
                Initial_Maximum_Deletion = 2
            else:
                Initial_Maximum_Deletion = math.ceil(0.05 * TopPercentage.height) # Round Up for initial maximum deletion

            # Maximum Deletion of 20% of the Initial Segment
            MaxDeletion20PCT = math.ceil(0.20 * TopPercentage.height) # Round Up for maximum 20% deletion

            # Half Free FFMCAP Area
            FreeFloat_Maximum_Deletion = (TopPercentage.filter(pl.col("Full_MCAP_USD_Cutoff_Company") < Lower_GMSR)
                                          .select(pl.col("Free_Float_MCAP_USD_Cutoff_Company")).sum().to_numpy()[0][0] / 2)
            
            # Counter for deletions
            Counter = 0
            # Counter for FFMCAP
            FFMCAP_Counter = 0

            # Case 1
            if Last_FullMcap < Lower_GMSR: # Below 0.5X GMSR
                
                # Case 1A
                if ((Last_CumWeight < Left_Limit) or # Below Market Coverage Left
                    (Left_Limit < Last_CumWeight < Right_Limit)): # In between Market Coverage
                    
                    # Verifying if removing one Company would bring us in the targetable area
                    while (
                            (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] < Lower_GMSR) and  # FMCAP gets above 0.5X GMSR
                            (Counter < Initial_Maximum_Deletion) and # Counter is within allowed deletion
                            (len(TopPercentage) > 3) # Minimum of 3 Companies
                            ):
                        
                        # Update the FFMCAP_Counter
                        FFMCAP_Counter = TopPercentage.tail(1).select(pl.col("Free_Float_MCAP_USD_Cutoff_Company")).sum().to_numpy()[0][0] + FFMCAP_Counter
                        
                        # Remove 1 by 1 until reaching LowerGMSR or breaking Maximum Deletions
                        TopPercentage = TopPercentage.head(TopPercentage.height - 1)

                        # Increase Counter Deletions
                        Counter = Counter + 1
                
                # Case 1B
                elif Last_CumWeight >= Right_Limit: # Over representation Market Coverage

                    # Verifying if removing one Company would bring us in the targetable area
                    while ((TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] < Lower_GMSR) and # FMCAP gets above 0.5X GMSR
                        (Counter < Initial_Maximum_Deletion) and # Counter for maximum number deletions
                        (TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0] > Left_Limit)): # Can never move from higher than RightLimit to lower than LeftLimit

                        # Update the FFMCAP_Counter
                        FFMCAP_Counter = TopPercentage.tail(1).select(pl.col("Free_Float_MCAP_USD_Cutoff_Company")).sum().to_numpy()[0][0] + FFMCAP_Counter

                        # Remove 1 by 1 until reaching LowerGMSR or breaking Maximum Deletions
                        TopPercentage = TopPercentage.head(TopPercentage.height - 1)

                        # Increase Counter Deletions
                        Counter = Counter + 1


                # Second Step of Deletion Rule #
                if ((TopPercentage.tail(1).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0] >= Lower_GMSR) or # Check if last FULLMCAP is at least equal to LowerGMSR 
                    (FFMCAP_Counter > FreeFloat_Maximum_Deletion)): # Check if the allowed FFMCAP has been removed

                    return TopPercentage # End the process of deletion

                else: # Max 20% deletions of names or 0.5 of FFMCAP can be reached
                    
                    # Maximum of 20% of Names can be deleted or No more than half of the FFMCAP below 0.5X GMSR
                    while ((TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] < Lower_GMSR) and # FMCAP gets above 0.5X GMSR
                           (Counter < MaxDeletion20PCT)): # Counter for maximum number deletions 20%
                        
                        # Update the FFMCAP_Counter
                        FFMCAP_Counter = TopPercentage.tail(1).select(pl.col("Free_Float_MCAP_USD_Cutoff_Company")).sum().to_numpy()[0][0] + FFMCAP_Counter

                        # Check if by removing the last Company, we break the rule for Maximum FreeFloat Deletion
                        if FFMCAP_Counter < FreeFloat_Maximum_Deletion:

                            # Remove 1 by 1 until reaching LowerGMSR or breaking Maximum Deletions
                            TopPercentage = TopPercentage.head(TopPercentage.height - 1)

                            # Increase Counter Deletions
                            Counter = Counter + 1
                        else:
                            break # Interrupt the loop

                    return TopPercentage # End the process of deletion

            # Case 2
            elif (Last_FullMcap >= Lower_GMSR and # Higher 0.5X GMSR
                Last_CumWeight > Right_Limit): # Higher Market Coverage
                # This case does not check for FFMCAP removed

                # Verifying if removing one Company would bring us in the targetable area
                while ((Counter < Initial_Maximum_Deletion) and # Counter for maximum number deletions
                       (TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0] > Left_Limit)): # Can never move from higher than RightLimit to lower than LeftLimit
                        
                        # Exit the loop immediately if this condition is True
                        if Left_Limit < TopPercentage.tail(1).select("CumWeight_Cutoff").to_numpy()[0][0] < Right_Limit:
                            break # Break the loop if CumWeight_Cutoff is in between the range
                    
                        # Remove 1 by 1 until reaching LowerGMSR or breaking Maximum Deletions
                        TopPercentage = TopPercentage.head(TopPercentage.height - 1)

                        # Increase Counter Deletions
                        Counter = Counter + 1

                return TopPercentage
            
            # Case 3
            # Only for testing
            else:
                print("Error! Missing Case")

        # Apply the Deletion Rule according to the Case if there Companies in between the Boundaries (GMSR & Coverage)
        TopPercentage = Deletions(temp_Country, TopPercentage, Upper_GMSR, Lower_GMSR, Left_Limit, Lower_Proximity_Bound, Last_FullMcap, Last_CumWeight)

        TopPercentage = TopPercentage.with_columns(
                            pl.lit("Standard").alias("Size"),
                            pl.lit("Deletion").alias("Case")
                        )

    return TopPercentage, temp_Country

##################################
#Read Developed/Emerging Universe#
##################################

Full_Dates = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Dates\Review_Date-QUARTERLY.csv").with_columns(
    pl.col("Review").str.to_date("%m/%d/%Y"),
    pl.col("Cutoff").str.to_date("%m/%d/%Y")
)

# Select columns to read from the Parquets
Columns = ["Date", "Index_Symbol", "Index_Name", "Internal_Number", "ISIN", "SEDOL", "RIC", "Instrument_Name", 
           "Country", "Currency", "Exchange", "ICB", "Free_Float", "Capfactor"]

# Load TMI with necessary transformations
TMI = (
    pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Universe\Final_Universe_TMI.parquet")
    .with_columns([
        pl.col("Free_Float").cast(pl.Float64),
        pl.col("Capfactor").cast(pl.Float64),
        pl.col("Date").cast(pl.Date)
    ])
    .drop("Mcap_Units_Index_Currency", "ICB", "Cutoff")
)

# Load and combine Emerging and Developed frames with deduplication
SW_Frame = (
    pl.concat([
        pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Universe\SWDACGV_with_Dec24.parquet")
        .with_columns([
            pl.col("Date").cast(pl.Date),
            pl.lit("Developed").alias("Segment")
        ]),
        pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Universe\SWEACGV_with_Dec24.parquet")
        .with_columns([
            pl.col("Date").cast(pl.Date),
            pl.lit("Emerging").alias("Segment")
        ])
    ])
    .select(["Date", "Country", "Segment"])
    .unique()  # Deduplicate combined frame
)

# Perform the join on TMI and SW_Frame and Fill missing values for Frontiers Countries
TMI = TMI.join(SW_Frame, on=["Date", "Country"], how="left").filter(
    pl.col("Segment").is_not_null())

# Create the Developed and Emerging containers
Developed = TMI.filter(pl.col("Segment") == "Developed")
Emerging = TMI.filter(pl.col("Segment")=="Emerging")

# GCC Extra
GCC = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Universe\GCC.parquet").with_columns([
                            pl.col("Free_Float").cast(pl.Float64),
                            pl.col("Capfactor").cast(pl.Float64),
                            pl.col("Date").cast(pl.Date),
                            pl.col("ICB").cast(pl.Utf8),
                            pl.col("Exchange").cast(pl.Utf8)
                            ]).filter(pl.col("Date") >= datetime.date(2019,6,24)).drop("Index_Name").with_columns(
                                pl.lit("Emerging").alias("Segment")
                            ).drop("ICB")

# All Listed Securities
Exchanges_Securities = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Exchange_Securities\Exchange_Securities_Final.parquet").with_columns([
                        pl.col("NumShrs").cast(pl.Float64),
                        pl.col("FreeFloatPct").cast(pl.Float64),
                        pl.col("FX").cast(pl.Float64),
                        pl.col("FFMCAP_USD").cast(pl.Float64),
                        pl.col("Full_MCAP_USD").cast(pl.Float64),
                        pl.col("infocode").cast(pl.Int32)
])

# Import STOXXID to keep only valide Securities
STOXXID = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Trading Days\Data_Luca.txt", infer_schema=False).with_columns(
    pl.col("vf").str.to_date("%Y%m%d"),
    pl.col("vt").str.to_date("%Y%m%d"),
    pl.col("InfoCode").cast(pl.Int32)
).with_columns(
    pl.when(pl.col("vt") == datetime.date(9999, 12, 30))  # Check for 9999-12-30
    .then(pl.date(2100, 12, 30))  # Replace with 2100-12-30
    .otherwise(pl.col("vt"))
    .alias("vt")
)

# Create an SQLContext and register tables
sql = pl.SQLContext()
sql.register("df_left", Exchanges_Securities)
sql.register("df_right", STOXXID)


filtered_df = sql.execute("""
    SELECT l.*, r.vf, r.vt, r.StoxxId
    FROM df_left AS l
    LEFT JOIN df_right AS r
    ON l.infocode = r.InfoCode
    WHERE (r.vf IS NULL AND r.vt IS NULL) OR 
          (l.cutoff BETWEEN r.vf AND r.vt)
""").collect()  # Collect to execute and get the result

# Fill empty FreeFloatPct
Exchanges_Securities = filtered_df.with_columns(
                        pl.col("FreeFloatPct").fill_null(1.0)
)

# Recalculate the FFMCAP_USD
Exchanges_Securities = Exchanges_Securities.with_columns(
                        (pl.col("Full_MCAP_USD") * pl.col("FreeFloatPct")).alias("FFMCAP_USD")
)

# Rename MCAP columns
Exchanges_Securities = Exchanges_Securities.rename({"FFMCAP_USD": "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD": "Full_MCAP_USD_Cutoff"})

# Drop missing FFMCAP securities
Exchanges_Securities = Exchanges_Securities.filter((pl.col("Free_Float_MCAP_USD_Cutoff") > 0) & (~pl.col("Free_Float_MCAP_USD_Cutoff").is_null()) & (pl.col("StoxxId").is_not_null()))

# Add Review Date
Exchanges_Securities = Exchanges_Securities.join(pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Dates\Review_Date-QUARTERLY.csv").with_columns(
                        pl.col("Review").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y"),
                        pl.col("Cutoff").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y")
                      ), left_on="cutoff", right_on="Cutoff", how="left").rename({"Review": "Date", "cutoff": "Cutoff", "isin": "ISIN", "Name": "Instrument_Name", "region": "Country"})

# Trading Days Information
Trading_Days = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Trading Days\Trading_Days_Final.parquet").filter(
    pl.col("StoxxId").is_not_null()
).with_columns(
    pl.col("Trades").cast(pl.Int16),
    pl.col("NoTrades").cast(pl.Int16),
    pl.col("MaxTrades").cast(pl.Int16),
    pl.col("cutoff").cast(pl.Date)
).select(pl.col(["cutoff", "StoxxId", "Trades", "NoTrades", "MaxTrades"])).join(
    Full_Dates, left_on="cutoff", right_on="Cutoff", how="left"
)

# Merge Emerging with GCC
Emerging = Emerging.vstack(GCC)

# Drop Pakistan from DEC-2021
Emerging = Emerging.filter(~((pl.col("Date") >= datetime.date(2021,12,20)) & (pl.col("Country") == "PK")))

# Entity_ID for matching Companies
Entity_ID = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\ENTITY_ID\Entity_ID.parquet").select(pl.col(["ENTITY_QID", "STOXX_ID",
                            "RELATIONSHIP_VALID_FROM", "RELATIONSHIP_VALID_TO"])).with_columns(
                                pl.col("RELATIONSHIP_VALID_FROM").cast(pl.Date()),
                                pl.col("RELATIONSHIP_VALID_TO").cast(pl.Date()))

# SW AC ALLCAP for check on Cutoff
SW_ACALLCAP = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Universe\STXWAGV_Cutoff_with_Dec24.parquet").with_columns([
                                pl.col("Date").cast(pl.Date),
                                pl.col("Mcap_Units_Index_Currency").cast(pl.Float64)
]).filter(pl.col("Mcap_Units_Index_Currency") > 0).join(pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Dates\Review_Date-QUARTERLY.csv").with_columns(
                        pl.col("Review").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y"),
                        pl.col("Cutoff").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y")
                      ), left_on="Date", right_on="Cutoff", how="left")

###################################
#####Filtering from StartDate######
###################################
Emerging = Emerging.filter(pl.col("Date") >= Starting_Date)
Developed = Developed.filter(pl.col("Date") >= Starting_Date)

##################################
######Add Cutoff Information######
##################################
Columns = ["validDate", "stoxxId", "currency", "closePrice", "shares", "freeFloat"]

# Country Coverage for Index Creation
Country_Coverage = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Universe\Country_Coverage.csv", separator=";")

# Read the Parquet and add the Review Date Column 
Securities_Cutoff = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Securities\Securities_Cutoff_with_Dec24.parquet", columns=Columns).with_columns([
                      pl.col("closePrice").cast(pl.Float64),
                      pl.col("freeFloat").cast(pl.Float64),
                      pl.col("shares").cast(pl.Float64),
                      pl.col("validDate").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d")
                      ]).join(pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Dates\Review_Date-QUARTERLY.csv").with_columns(
                        pl.col("Review").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y"),
                        pl.col("Cutoff").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y")
                      ), left_on="validDate", right_on="Cutoff", how="left").rename({"freeFloat": "FreeFloat_Cutoff"})

FX_Cutoff = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Securities\FX_Historical_with_Dec24.parquet").with_columns(
                            pl.col("Cutoff").cast(pl.Date)
)

# Add these information to the Developed and Emerging Universes
Developed = (
    Developed
    .join(
        Securities_Cutoff,
        left_on=["Date", "Internal_Number"],
        right_on=["Review", "stoxxId"],
        how="left"
    )
    .with_columns([
        # Fill null values in "currency" with the values from "Currency"
        pl.col("currency").fill_null(pl.col("Currency")),
    ])
    .drop("Currency")  # Drop the "Currency" column after filling nulls
    .rename({
        "validDate": "Cutoff",
        "closePrice": "Close_unadjusted_local_Cutoff",
        "shares": "Shares_Cutoff",
        "currency": "Currency"
    })
)

# Add FX_Cutoff
Developed = Developed.join(FX_Cutoff, on=["Cutoff", "Currency"], how="left")

Emerging = (
    Emerging
    .join(
        Securities_Cutoff,
        left_on=["Date", "Internal_Number"],
        right_on=["Review", "stoxxId"],
        how="left"
    )
    .with_columns([
        # Fill null values in "currency" with the values from "Currency"
        pl.col("currency").fill_null(pl.col("Currency")),
    ])
    .drop("Currency")  # Drop the "Currency" column after filling nulls
    .rename({
        "validDate": "Cutoff",
        "closePrice": "Close_unadjusted_local_Cutoff",
        "shares": "Shares_Cutoff",
        "currency": "Currency"
    })
)

# Add FX_Cutoff
Emerging = Emerging.join(FX_Cutoff, on=["Cutoff", "Currency"], how="left")

# Add Cutoff information for TMI
TMI = (
    TMI
    .join(
        Securities_Cutoff,
        left_on=["Date", "Internal_Number"],
        right_on=["Review", "stoxxId"],
        how="left"
    )
    .with_columns([
        # Fill null values in "currency" with the values from "Currency"
        pl.col("currency").fill_null(pl.col("Currency")),
    ])
    .drop("Currency")  # Drop the "Currency" column after filling nulls
    .rename({
        "validDate": "Cutoff",
        "closePrice": "Close_unadjusted_local_Cutoff",
        "shares": "Shares_Cutoff",
        "currency": "Currency"
    })
)

# Add FX_Cutoff
TMI = TMI.join(FX_Cutoff, on=["Cutoff", "Currency"], how="left")

##################################
#########Drop Empty Rows##########
##################################
Developed = Developed.filter(~((pl.col("FX_local_to_Index_Currency_Cutoff").is_null()) | (pl.col("Close_unadjusted_local_Cutoff").is_null()) | (pl.col("Shares_Cutoff").is_null())))
Emerging = Emerging.filter(~((pl.col("FX_local_to_Index_Currency_Cutoff").is_null()) | (pl.col("Close_unadjusted_local_Cutoff").is_null()) | (pl.col("Shares_Cutoff").is_null())))
TMI = TMI.filter(~((pl.col("FX_local_to_Index_Currency_Cutoff").is_null()) | (pl.col("Close_unadjusted_local_Cutoff").is_null()) | (pl.col("Shares_Cutoff").is_null())))

##################################
####Read Turnover Information#####
##################################

# TurnOverRatio
Turnover = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Turnover\Turnover_Cutoff_SWALL_with_Dec24.parquet")
# Drop unuseful columns
Turnover = Turnover.drop(["vd", "calcType", "token"])
# Keep only relevant fields
Turnover = Turnover.filter(pl.col("field").is_in(["TurnoverRatioFO", "TurnoverRatioFO_India1"]))
# Transform the table
Turnover = Turnover.pivot(
                values="Turnover_Ratio",
                index=["Date", "Internal_Number"],
                on="field"
                ).rename({"TurnoverRatioFO": "Turnover_Ratio"})
# Fill NA in TurnoverRatioFO_India1
Turnover = Turnover.with_columns(
                                pl.col("TurnoverRatioFO_India1").fill_null(pl.col("Turnover_Ratio"))
                                ).drop("Turnover_Ratio").rename({"TurnoverRatioFO_India1": "Turnover_Ratio"}).to_pandas()
# Add Turnover Information
Pivot_TOR = Turnover.pivot(values="Turnover_Ratio", index="Date", columns="Internal_Number")

# Add ENTITY_QID to main Frames
Developed = Developed.join(
                            Entity_ID,
                            left_on="Internal_Number",
                            right_on="STOXX_ID",
                            how="left"
                          ).unique(["Date", "Internal_Number"]).sort("Date", descending=False).with_columns(
                              pl.col("ENTITY_QID").fill_null(pl.col("Internal_Number"))).drop({"RELATIONSHIP_VALID_FROM", "RELATIONSHIP_VALID_TO"})

Emerging = Emerging.join(
                            Entity_ID,
                            left_on="Internal_Number",
                            right_on="STOXX_ID",
                            how="left"
                          ).unique(["Date", "Internal_Number"]).sort("Date", descending=False).with_columns(
                              pl.col("ENTITY_QID").fill_null(pl.col("Internal_Number"))).drop({"RELATIONSHIP_VALID_FROM", "RELATIONSHIP_VALID_TO"})

TMI = TMI.join(
                            Entity_ID,
                            left_on="Internal_Number",
                            right_on="STOXX_ID",
                            how="left"
                          ).unique(["Date", "Internal_Number"]).sort("Date", descending=False).with_columns(
                              pl.col("ENTITY_QID").fill_null(pl.col("Internal_Number"))).drop({"RELATIONSHIP_VALID_FROM", "RELATIONSHIP_VALID_TO"})

##################################
########Read Turnover 12M#########
##################################

# TurnOverRatio12M
Turnover12M = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Turnover\Turnover_12M.parquet").with_columns(
    pl.col("vd").cast(pl.Utf8).str.to_date("%Y%m%d")
).rename({"vd": "Cutoff"}).join(
    Full_Dates, on="Cutoff", how="left"
).rename({"Review": "Date", "stoxxId": "Internal_Number"})

# Drop unuseful columns
Turnover12M = Turnover12M.drop(["mapDt", "calcType", "token", "__index_level_0__"])
# Keep only relevant fields
Turnover12M = Turnover12M.filter(pl.col("field").is_in(["TurnoverRatioFO", "TurnoverRatioFO_India1"])).rename({"value": "Turnover_Ratio"}).select(pl.col(["Date",
                "Internal_Number", "field", "Turnover_Ratio"]))
Turnover12M = Turnover12M.pivot(
                values="Turnover_Ratio",
                index=["Date", "Internal_Number"],
                on="field"
                ).rename({"TurnoverRatioFO": "Turnover_Ratio"})

# Fill NA in TurnoverRatioFO_India1
Turnover12M = Turnover12M.with_columns(
                                pl.col("TurnoverRatioFO_India1").fill_null(pl.col("Turnover_Ratio"))
                                ).drop("Turnover_Ratio").rename({"TurnoverRatioFO_India1": "Turnover_Ratio"}).to_pandas()
# Add Turnover Information
Pivot_TOR_12M = Turnover12M.pivot(values="Turnover_Ratio", index="Date", columns="Internal_Number")

##################################
###########Read FOL-FH############
##################################
FOL_FH = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\FHR\FHFOL_QAD_Final.parquet").with_columns(
    pl.col("marketdate").cast(pl.Date),
    pl.col("FH").cast(pl.Float64, strict=False),
    pl.col("FOL").cast(pl.Float64, strict=False)).join(
        Full_Dates, left_on=["marketdate"], right_on=["Cutoff"], how="left"
    ).filter(pl.col("StoxxId").is_not_null()).unique(["Review", "StoxxId"])

# Add FOL-FH Information
Emerging = Emerging.join(FOL_FH, left_on=["Date", "Internal_Number"], right_on=["Review", "StoxxId"], how="left").filter(
        pl.col("FH").is_not_null() & pl.col("FOL").is_not_null()).with_columns(
        ((pl.col("FOL") - pl.col("FH")) / pl.col("FOL")).alias("foreign_headroom"))

Developed = Developed.join(FOL_FH, left_on=["Date", "Internal_Number"], right_on=["Review", "StoxxId"], how="left").filter(
        pl.col("FH").is_not_null() & pl.col("FOL").is_not_null()).with_columns(
        ((pl.col("FOL") - pl.col("FH")) / pl.col("FOL")).alias("foreign_headroom"))

# Mask CN Securities
Chinese_CapFactor = China_A_Securities(Emerging)
Chinese_CapFactor_TMI = China_A_Securities(TMI)

# Add the information to Emerging Universe
Emerging = Emerging.join(Chinese_CapFactor.select(pl.col(["Date", "Internal_Number", "Capfactor_CN"])), on=["Date", "Internal_Number"], how="left").with_columns(
                        pl.col("Capfactor_CN").fill_null(pl.col("Capfactor"))).drop("Capfactor").rename({"Capfactor_CN": "Capfactor"})

# Add the information to TMI Universe
TMI = TMI.join(Chinese_CapFactor_TMI.select(pl.col(["Date", "Internal_Number", "Capfactor_CN"])), on=["Date", "Internal_Number"], how="left").with_columns(
                        pl.col("Capfactor_CN").fill_null(pl.col("Capfactor"))).drop("Capfactor").rename({"Capfactor_CN": "Capfactor"})

# Calculate FOR
Developed = Developed.with_columns(
    pl.min_horizontal(
        pl.col("Free_Float"), 
        pl.max_horizontal(pl.lit(0), pl.col("FOL") - pl.col("FH"))
    ).alias("FOR")
)

Emerging = Emerging.with_columns(
    pl.min_horizontal(
        pl.col("Free_Float"), 
        pl.max_horizontal(pl.lit(0), pl.col("FOL") - pl.col("FH"))
    ).alias("FOR")
)

# Calculate Free/Full MCAP USD for Developed Universe
Developed = Developed.with_columns(
                                    (pl.col("FOR") * pl.col("Capfactor") * pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Free_Float_MCAP_USD_Cutoff"),
                                    (pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Full_MCAP_USD_Cutoff")
                                  )

# Calculate Free/Full MCAP USD for Emerging Universe
Emerging = Emerging.with_columns(
                                    (pl.col("FOR") * pl.col("Capfactor") * pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Free_Float_MCAP_USD_Cutoff"),
                                    (pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Full_MCAP_USD_Cutoff")
                                  )

# Calculate Free/Full MCAP USD for TMI Universe
TMI = TMI.with_columns(
                                    (pl.col("Free_Float") * pl.col("Capfactor") * pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Free_Float_MCAP_USD_Cutoff"),
                                    (pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Full_MCAP_USD_Cutoff")
                                  )

# Check if there is any Free_Float_MCAP_USD_Cutoff Empty
Emerging = Emerging.filter((pl.col("Free_Float_MCAP_USD_Cutoff") > 0) & (pl.col("Free_Float_MCAP_USD_Cutoff")).is_not_nan())
Developed = Developed.filter((pl.col("Free_Float_MCAP_USD_Cutoff") > 0) & (pl.col("Free_Float_MCAP_USD_Cutoff")).is_not_nan())
TMI = TMI.filter(
    (pl.col("Free_Float_MCAP_USD_Cutoff") > 0) & pl.col("Free_Float_MCAP_USD_Cutoff").is_not_nan())

###################################
####Creation of main GMSR Frame####
###################################
GMSR_Frame = pl.DataFrame({
                            "Date": pl.Series(dtype=pl.Date),
                            "GMSR_Developed": pl.Series(dtype=pl.Float64),
                            "GMSR_Developed_Upper": pl.Series(dtype=pl.Float64),
                            "GMSR_Developed_Lower": pl.Series(dtype=pl.Float64),
                            "GMSR_Emerging": pl.Series(dtype=pl.Float64),
                            "GMSR_Emerging_Upper": pl.Series(dtype=pl.Float64),
                            "GMSR_Emerging_Lower": pl.Series(dtype=pl.Float64),
                            "Rank": pl.Series(dtype=pl.UInt32)
})

##################################
#########Review Process###########
##################################

Output_Standard_Index = pl.DataFrame({
    "Date": pl.Series([], dtype=pl.Date),
    "ENTITY_QID": pl.Series([], dtype=pl.Utf8),
    "Country": pl.Series([], dtype=pl.Utf8),
    "Size": pl.Series([], dtype=pl.Utf8),
    "Case": pl.Series([], dtype=pl.Utf8)
})

Output_Count_Standard_Index = pl.DataFrame({
    "Country": pl.Series([], dtype=pl.Utf8),
    "Count": pl.Series([], dtype=pl.UInt32),
    "Date": pl.Series([], dtype=pl.Date),
})

Screened_Securities = pl.DataFrame({
                                    "Date": pl.Series([], dtype=pl.Date),
                                    "Internal_Number": pl.Series([], dtype=pl.Utf8),
                                    "Segment": pl.Series([], dtype=pl.Utf8),
                                    "Country": pl.Series([], dtype=pl.Utf8)})

EMS_Frame = pl.DataFrame({
                        "Date": pl.Series([], dtype=pl.Date),
                        "Segment": pl.Series([], dtype=pl.Utf8),
                        "EMS": pl.Series([], dtype=pl.Float64),
                        "Rank": pl.Series([], dtype=pl.Int64),
                        "Total": pl.Series([], dtype=pl.Int64),
                        "FreeFloatMCAP_Minimum_Size": pl.Series([], dtype=pl.Float64)
})

Standard_Index = pl.DataFrame({
    "Date": pl.Series([], dtype=pl.Date),
    "Internal_Number": pl.Series([], dtype=pl.Utf8),
    "Instrument_Name": pl.Series([], dtype=pl.Utf8),
    "ENTITY_QID": pl.Series([], dtype=pl.Utf8),
    "Country": pl.Series([], dtype=pl.Utf8),
    "Size": pl.Series([], dtype=pl.Utf8),
    "Shadow_Company": pl.Series([], dtype=pl.Boolean)
})

Small_Index = pl.DataFrame({
    "Date": pl.Series([], dtype=pl.Date),
    "Internal_Number": pl.Series([], dtype=pl.Utf8),
    "Instrument_Name": pl.Series([], dtype=pl.Utf8),
    "ENTITY_QID": pl.Series([], dtype=pl.Utf8),
    "Country": pl.Series([], dtype=pl.Utf8),
    "Size": pl.Series([], dtype=pl.Utf8),
    "Shadow_Company": pl.Series([], dtype=pl.Boolean)
})

# FullInfo Frame for ChairsSorting
ChairsSortingFrame = pl.DataFrame({
    "Date": pl.Series([], dtype=pl.Date),
    "Internal_Number": pl.Series([], dtype=pl.Utf8),
    "Instrument_Name": pl.Series([], dtype=pl.Utf8),
    "ENTITY_QID": pl.Series([], dtype=pl.Utf8),
    "Country": pl.Series([], dtype=pl.Utf8),
    "Full_MCAP_USD_Cutoff_Company": pl.Series([], dtype=pl.Float64),
    "Shadow_Company": pl.Series([], dtype=pl.Boolean)
})

# FullInfo Frame for Securities Passing the Screens
Eligible_Companies = pl.DataFrame(
    schema={
        "Date": pl.Date,
        "Country": pl.Utf8,
        "Count": pl.Int64
    }
)

# LIF Frame
LIF_Stored = pl.DataFrame(
    schema={
        "Date": pl.Date,
        "Internal_Number": pl.Utf8,
        "LIF": pl.Float64,
        "foreign_headroom": pl.Float64,
        "1Y_Exclusion": pl.Boolean,
        "Exclusion_Date": pl.Date

    }
)

# Frame Screen Market Cap
MarketCapScreen = pl.DataFrame()
# Frame Screen Turnover
TurnoverScreen = pl.DataFrame()
# Frame FOR Turnover
FORScreen = pl.DataFrame()

with pd.ExcelWriter(Output_File, engine='xlsxwriter') as writer:
    for date in Emerging.select(["Date"]).unique().sort("Date").to_series():
        
        start_time_single_date = time.time()
        print(date)

        # Keep only a slice of Frame with the current Date
        temp_Emerging = Emerging.filter(pl.col("Date") == date)
        temp_Developed = Developed.filter(pl.col("Date") == date)
        temp_TMI = TMI.filter((pl.col("Date") == date) & (pl.col("Country").is_in(temp_Developed.select(pl.col("Country")).unique())))
        temp_Exchanges_Securities = Exchanges_Securities.filter((pl.col("Date") == date) & (pl.col("Country").is_in(temp_Developed.select(pl.col("Country")).unique())))

        ###################################
        ################ADR################
        ###################################
        temp_Emerging = ADR_Removal(temp_Emerging, Emerging, Developed, "Emerging").drop("Occurrence_Count", "Index_Symbol_right")
        temp_Developed = ADR_Removal(temp_Developed, Emerging, Developed, "Developed").drop("Occurrence_Count", "Index_Symbol_right")

        # First Review Date where Index is created
        if date == Starting_Date:

            ###################################
            #########FOR FF Screening##########
            ###################################

            # # Filter for FOR FF Screening
            temp_Developed, LIF_Stored = FOR_Screening(Screened_Securities, temp_Developed, Developed, Pivot_TOR, Standard_Index, Small_Index, date, "Developed", LIF_Stored)
            temp_Emerging, LIF_Stored = FOR_Screening(Screened_Securities, temp_Emerging, Emerging, Pivot_TOR, Standard_Index, Small_Index, date, "Emerging", LIF_Stored)

            # Drop Free_Float_MCAP_USD_Cutoff as it needs to be recalculated
            temp_Developed = temp_Developed.drop("Free_Float_MCAP_USD_Cutoff")
            temp_Emerging = temp_Emerging.drop("Free_Float_MCAP_USD_Cutoff")

            # Recalculate Free_Float_MCAP_USD_Cutoff
            temp_Developed = temp_Developed.with_columns(
                (pl.col("Close_unadjusted_local_Cutoff") * pl.col("Shares_Cutoff") * pl.col("FIF") * pl.col("FX_local_to_Index_Currency_Cutoff")).alias("Free_Float_MCAP_USD_Cutoff")).drop(
                    "InPrevScreened_Universe", "LIF")
            
            temp_Emerging = temp_Emerging.with_columns(
                (pl.col("Close_unadjusted_local_Cutoff") * pl.col("Shares_Cutoff") * pl.col("FIF") * pl.col("FX_local_to_Index_Currency_Cutoff")).alias("Free_Float_MCAP_USD_Cutoff")).drop(
                    "InPrevScreened_Universe", "LIF")

            ###################################
            ##########Apply EMS Screen#########
            ###################################

            # Apply the Screens
            temp_Developed, EMS_Frame, Temp_Screened_Securities, equity_universe_min_size = Equity_Minimum_Size(temp_TMI, temp_Developed, Pivot_TOR, EMS_Frame, date, "Developed", Screened_Securities, temp_Exchanges_Securities, FullListSecurities)
            # Screen FreeFloat MarketCap
            temp_Developed = temp_Developed.filter(pl.col("Free_Float_MCAP_USD_Cutoff") > EMS_Frame.filter(pl.col("Date") == date).select(
                pl.col("FreeFloatMCAP_Minimum_Size")).to_numpy()[0][0]).select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number",
                "Instrument_Name", "Free_Float", "Capfactor", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"]))
            
            temp_Emerging, EMS_Frame, Temp_Screened_Securities , equity_universe_min_size= Equity_Minimum_Size(temp_TMI, temp_Emerging, Pivot_TOR, EMS_Frame, date, "Emerging", Screened_Securities, temp_Exchanges_Securities, FullListSecurities)
            # Screen FreeFloat MarketCap
            temp_Emerging = temp_Emerging.filter(pl.col("Free_Float_MCAP_USD_Cutoff") > EMS_Frame.filter(pl.col("Date") == date).select(
                pl.col("FreeFloatMCAP_Minimum_Size")).to_numpy()[0][0]).select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number",
                "Instrument_Name", "Free_Float", "Capfactor", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"]))
            
            ###################################
            ####TurnoverRatio12M Screening#####
            ###################################

            # Apply the Check on Turnover for all Components
            Developed_Screened_12M = Turnover_Check_12M(temp_Developed, Pivot_TOR_12M, Threshold_NEW_12M, Threshold_OLD_12M, date, Starting_Date, "Developed")
            Emerging_Screened_12M = Turnover_Check_12M(temp_Emerging, Pivot_TOR_12M, Threshold_NEW_12M, Threshold_OLD_12M, date, Starting_Date, "Emerging")

            # Remove Securities not passing the screen
            temp_Developed = temp_Developed.join(Developed_Screened_12M, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
            temp_Emerging = temp_Emerging.join(Emerging_Screened_12M, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
            
            ###################################
            ######TurnoverRatio Screening######
            ###################################

            # Apply the Check on Turnover for all Components
            Developed_Screened = Turnover_Check(temp_Developed, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date, "Developed")
            Emerging_Screened = Turnover_Check(temp_Emerging, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date, "Emerging")

            # Remove Securities not passing the screen
            temp_Developed = temp_Developed.join(Developed_Screened, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
            temp_Emerging = temp_Emerging.join(Emerging_Screened, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)

            ###################################
            ###########Trading Days############
            ###################################

            Trading_Developed = Trading_Frequency(temp_Developed, Trading_Days, date, Starting_Date, "Developed")
            Trading_Emerging = Trading_Frequency(temp_Emerging, Trading_Days, date, Starting_Date, "Emerging")

            # Filter out Securities with less than X% of Trading Days
            temp_Developed = temp_Developed.join(Trading_Developed, on=["Internal_Number"], how="left").filter(pl.col("Status_Trading") == True)
            temp_Emerging = temp_Emerging.join(Trading_Emerging, on=["Internal_Number"], how="left").filter(pl.col("Status_Trading") == True)

            ##################################
            #Store Securities Passing Screens#
            ##################################

            Screened_Securities = Screened_Securities.vstack(temp_Developed.with_columns(pl.lit("Developed").alias("Segment")).select(Screened_Securities.columns))
            Screened_Securities = Screened_Securities.vstack(temp_Emerging.with_columns(pl.lit("Emerging").alias("Segment")).select(Screened_Securities.columns))

            ##################################
            #######Aggregate Companies########
            ##################################

            #Re-integrate the Full_MCAP_USD_Cutoff for those Securities that have been excluded
            Original_MCAP_Emerging = Emerging.filter(pl.col("Date") == date).group_by("ENTITY_QID").agg(pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company"))
            Original_MCAP_Developed = Developed.filter(pl.col("Date") == date).group_by("ENTITY_QID").agg(pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company"))

            temp_Developed_Aggregate = temp_Developed.filter(pl.col("Date") == date).select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company")
                                                    ]).join(Original_MCAP_Developed, on=["ENTITY_QID"], how="left").sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)

            temp_Emerging_Aggregate = temp_Emerging.filter(pl.col("Date") == date).select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company")
                                                    ]).join(Original_MCAP_Emerging, on=["ENTITY_QID"], how="left").sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)

            #################################
            #########GMSR Calculation########
            #################################

            temp_Developed_Aggregate = temp_Developed_Aggregate.sort(["Full_MCAP_USD_Cutoff_Company"], descending=True)
            temp_Developed_Aggregate = temp_Developed_Aggregate.with_columns(
                                                            (pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Weight_Cutoff")
            )

            temp_Developed_Aggregate = temp_Developed_Aggregate.with_columns(
                                                            (pl.col("Weight_Cutoff").cum_sum()).alias("CumWeight_Cutoff"),
                                                            (-pl.col("Full_MCAP_USD_Cutoff_Company")).rank("dense").alias("Rank")
            )

            # Check if the MSCI_GMSR is between 99.5% and 100%
            if GMSR_Upper_Buffer <= temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI).head(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] <= GMSR_Lower_Buffer:
                New_Data = pl.DataFrame({
                                            "Date": [date],
                                            "GMSR_Developed": [GMSR_MSCI],
                                            "GMSR_Developed_Upper": [GMSR_MSCI * Upper_Limit],
                                            "GMSR_Developed_Lower": [GMSR_MSCI * Lower_Limit], 
                                            "GMSR_Emerging": [GMSR_MSCI / 2],
                                            "GMSR_Emerging_Upper": [GMSR_MSCI / 2 * Upper_Limit],
                                            "GMSR_Emerging_Lower": [GMSR_MSCI / 2 * Lower_Limit],
                                            "Rank": [temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI).head(1).select(pl.col("Rank")).to_numpy()[0][0]]
                })

            elif temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI).head(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] > GMSR_Lower_Buffer:
                New_Data = pl.DataFrame({
                                            "Date": [date],
                                            "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                            "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                            "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                            "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                            "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                            "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                            "Rank": [
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                            temp_Developed_Aggregate
                                                            .filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer)
                                                            .tail(1)["Full_MCAP_USD_Cutoff_Company"]
                                                            .to_numpy()[0]
                                                        )
                                                        .head(1)["Rank"]
                                                        .to_numpy()[0]
                                                    ]

                })

            elif temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI).head(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] < GMSR_Upper_Buffer:
                New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                        "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                        "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                        "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                        "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                        "Rank": [
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer)
                                                        .head(1)["Full_MCAP_USD_Cutoff_Company"]
                                                        .to_numpy()[0]
                                                    )
                                                    .head(1)["Rank"]
                                                    .to_numpy()[0]
                                                ]
            })

            # Drop the Rank column
            temp_Developed_Aggregate = temp_Developed_Aggregate.drop("Rank")

            GMSR_Frame = GMSR_Frame.vstack(New_Data)

            #################################
            ##Start the Size Classification##
            #################################

            # Get the GMSR
            Lower_GMSR = GMSR_Frame.select(["GMSR_Emerging_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
            Upper_GMSR = GMSR_Frame.select(["GMSR_Emerging_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

            # ##############
            # #### APG #####
            # ##############
            # Lower_GMSR = GMSR_Frame.select(["GMSR_Developed_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
            # Upper_GMSR = GMSR_Frame.select(["GMSR_Developed_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

            # Emerging #
            for country in temp_Emerging_Aggregate.select(pl.col("Country")).unique().sort("Country").to_series():
                
                TopPercentage, temp_Country = Index_Creation_Box(temp_Emerging_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap, Percentage, Right_Limit, Left_Limit, "Emerging", writer)

                # Eligible Companies
                NewEligible = pl.DataFrame({
                    "Date": [date],
                    "Country": [country],
                    "Count": [len(temp_Country)]
                })

                Eligible_Companies = Eligible_Companies.vstack(NewEligible)

                # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging, LIF_Stored)

                # Stack to Output_Standard_Index
                Output_Standard_Index = Output_Standard_Index.vstack(TopPercentage.select(Output_Standard_Index.columns))

                # Create the Output_Count_Standard_Index for future rebalacing
                Output_Count_Standard_Index = Output_Count_Standard_Index.vstack(TopPercentage.group_by("Country").agg(
                        pl.len().alias("Count"),
                        pl.col("Date").first().alias("Date")
                    ).sort("Count", descending=True))

                #################################
                ###########Assign Size###########
                #################################

                # Standard Index #
                Standard_Index = Standard_Index.vstack(TopPercentage_Securities.select(Standard_Index.columns))

                # Small Index #
                Emerging_Small = temp_Emerging.filter((~pl.col("Internal_Number").is_in(TopPercentage_Securities.select(pl.col("Internal_Number")))) & (
                    pl.col("Country") == country)).select(
                    pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])
                ).with_columns(
                    pl.lit("Small").alias("Size"),
                    pl.lit(False).alias("Shadow_Company")).select(Small_Index.columns).head(0)
                
                Small_Index = Small_Index.vstack(Emerging_Small)
            
        # Following Reviews where Index is rebalanced
        else:

            ###################################
            #########FOR FF Screening##########
            ###################################

            # # Filter for FOR FF Screening
            temp_Developed, LIF_Stored = FOR_Screening(Screened_Securities, temp_Developed, Developed, Pivot_TOR, Standard_Index, Small_Index, date, "Developed", LIF_Stored)
            temp_Emerging, LIF_Stored = FOR_Screening(Screened_Securities, temp_Emerging, Emerging, Pivot_TOR, Standard_Index, Small_Index, date, "Emerging", LIF_Stored)

            # Drop Free_Float_MCAP_USD_Cutoff as it needs to be recalculated
            temp_Developed = temp_Developed.drop("Free_Float_MCAP_USD_Cutoff")
            temp_Emerging = temp_Emerging.drop("Free_Float_MCAP_USD_Cutoff")

            # Recalculate Free_Float_MCAP_USD_Cutoff
            temp_Developed = temp_Developed.with_columns(
                (pl.col("Close_unadjusted_local_Cutoff") * pl.col("Shares_Cutoff") * pl.col("FIF") * pl.col("FX_local_to_Index_Currency_Cutoff")).alias("Free_Float_MCAP_USD_Cutoff")).drop(
                    "InPrevScreened_Universe", "Current_LIF")
                
            
            temp_Emerging = temp_Emerging.with_columns(
                (pl.col("Close_unadjusted_local_Cutoff") * pl.col("Shares_Cutoff") * pl.col("FIF") * pl.col("FX_local_to_Index_Currency_Cutoff")).alias("Free_Float_MCAP_USD_Cutoff")).drop(
                    "InPrevScreened_Universe", "Current_LIF")
            
            if (date < datetime.date(2023, 3, 20) and (date.month == 6 or date.month == 12)) or (date >= datetime.date(2023, 3, 20)):

                # Status
                print("Screens applied on " + str(date))

                ###################################
                ##########Apply EMS Screen#########
                ###################################

                # Apply the Screens
                temp_Developed, EMS_Frame, Temp_Screened_Securities, equity_universe_min_size = Equity_Minimum_Size(temp_TMI, temp_Developed, Pivot_TOR, EMS_Frame, date, "Developed", Screened_Securities, temp_Exchanges_Securities, FullListSecurities)

                # Filter Temp_Screened_Securities for the latest Date
                Temp_Screened_Securities_MCAP = Temp_Screened_Securities.filter(pl.col("Date") == Temp_Screened_Securities.select(pl.col("Date").max()).item())

                # Add information about those securities that were inlcuded in the previous Screened_Universe
                temp_Developed = temp_Developed.with_columns(
                    (pl.col("Internal_Number").is_in(Temp_Screened_Securities_MCAP.select(pl.col("Internal_Number")))).alias("InPrevScreened_Universe")
                )

                # Apply the FreeFloat_MCAP Screen only to those Securities which are not part of the Screened_Universe
                temp_Developed = temp_Developed.with_columns(
                # Create a mask column that applies the screen only if InPrevScreened_Universe is FALSE
                    pl.when(
                        (pl.col("InPrevScreened_Universe") == False) &
                        (pl.col("Free_Float_MCAP_USD_Cutoff") < equity_universe_min_size / 2)
                    )
                        .then(pl.lit(None))  # Mark as null (to exclude later)
                        .otherwise(pl.col("Free_Float_MCAP_USD_Cutoff"))
                        .alias("Free_Float_MCAP_USD_Cutoff_Company_Screened")
                    ).filter(
                        pl.col("Free_Float_MCAP_USD_Cutoff_Company_Screened").is_not_null()
                    ).drop("InPrevScreened_Universe", "Free_Float_MCAP_USD_Cutoff_Company_Screened")
                
                # Apply the Screens
                temp_Emerging, EMS_Frame, Temp_Screened_Securities, equity_universe_min_size = Equity_Minimum_Size(temp_TMI, temp_Emerging, Pivot_TOR, EMS_Frame, date, "Emerging", Screened_Securities, temp_Exchanges_Securities, FullListSecurities)
                
                # Add information about those securities that were inlcuded in the previous Screened_Universe
                temp_Emerging = temp_Emerging.with_columns(
                    (pl.col("Internal_Number").is_in(Temp_Screened_Securities_MCAP.select(pl.col("Internal_Number")))).alias("InPrevScreened_Universe")
                )

                # Apply the FreeFloat_MCAP Screen only to those Securities which are not part of the Screened_Universe
                temp_Emerging = temp_Emerging.with_columns(
                # Create a mask column that applies the screen only if InPrevScreened_Universe is FALSE
                    pl.when(
                        (pl.col("InPrevScreened_Universe") == False) &
                        (pl.col("Free_Float_MCAP_USD_Cutoff") < equity_universe_min_size / 2)
                    )
                        .then(pl.lit(None))  # Mark as null (to exclude later)
                        .otherwise(pl.col("Free_Float_MCAP_USD_Cutoff"))
                        .alias("Free_Float_MCAP_USD_Cutoff_Company_Screened")
                    ).filter(
                        pl.col("Free_Float_MCAP_USD_Cutoff_Company_Screened").is_not_null()
                    ).drop("InPrevScreened_Universe", "Free_Float_MCAP_USD_Cutoff_Company_Screened")
                
                ###################################
                ####TurnoverRatio12M Screening#####
                ###################################

                # Apply the Check on Turnover for all Components
                Developed_Screened_12M = Turnover_Check_12M(temp_Developed, Pivot_TOR_12M, Threshold_NEW_12M, Threshold_OLD_12M, date, Starting_Date, "Developed")
                Emerging_Screened_12M = Turnover_Check_12M(temp_Emerging, Pivot_TOR_12M, Threshold_NEW_12M, Threshold_OLD_12M, date, Starting_Date, "Emerging")

                # Remove Securities not passing the screen
                temp_Developed = temp_Developed.join(Developed_Screened_12M, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
                temp_Emerging = temp_Emerging.join(Emerging_Screened_12M, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
                
                ###################################
                ######TurnoverRatio Screening######
                ###################################

                # Apply the Check on Turnover for all Components
                Developed_Screened = Turnover_Check(temp_Developed, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date, "Developed")
                Emerging_Screened = Turnover_Check(temp_Emerging, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date, "Emerging")

                # Remove Securities not passing the screen
                temp_Developed = temp_Developed.join(Developed_Screened, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
                temp_Emerging = temp_Emerging.join(Emerging_Screened, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)

                ###################################
                ###########Trading Days############
                ###################################

                Trading_Developed = Trading_Frequency(temp_Developed, Trading_Days, date, Starting_Date, "Developed")
                Trading_Emerging = Trading_Frequency(temp_Emerging, Trading_Days, date, Starting_Date, "Emerging")

                # Filter out Securities with less than X% of Trading Days
                temp_Developed = temp_Developed.join(Trading_Developed, on=["Internal_Number"], how="left").filter(pl.col("Status_Trading") == True)
                temp_Emerging = temp_Emerging.join(Trading_Emerging, on=["Internal_Number"], how="left").filter(pl.col("Status_Trading") == True)

            ##################################
            #Store Securities Passing Screens#
            ##################################

            Screened_Securities = Screened_Securities.vstack(temp_Developed.with_columns(pl.lit("Developed").alias("Segment")).select(Screened_Securities.columns))
            Screened_Securities = Screened_Securities.vstack(temp_Emerging.with_columns(pl.lit("Emerging").alias("Segment")).select(Screened_Securities.columns))

            ##################################
            #######Aggregate Companies########
            ##################################

            #Re-integrate the Full_MCAP_USD_Cutoff for those Securities that have been excluded
            Original_MCAP_Emerging = Emerging.filter(pl.col("Date") == date).group_by("ENTITY_QID").agg(pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company"))
            Original_MCAP_Developed = Developed.filter(pl.col("Date") == date).group_by("ENTITY_QID").agg(pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company"))

            temp_Developed_Aggregate = temp_Developed.filter(pl.col("Date") == date).select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company")
                                                    ]).join(Original_MCAP_Developed, on=["ENTITY_QID"], how="left").sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)

            temp_Emerging_Aggregate = temp_Emerging.filter(pl.col("Date") == date).select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company")
                                                    ]).join(Original_MCAP_Emerging, on=["ENTITY_QID"], how="left").sort(["Date", "Full_MCAP_USD_Cutoff_Company"], descending = True)

            #################################
            #########GMSR Calculation########
            #################################

            temp_Developed_Aggregate = temp_Developed_Aggregate.sort(["Full_MCAP_USD_Cutoff_Company"], descending=True)
            temp_Developed_Aggregate = temp_Developed_Aggregate.with_columns(
                                                            (pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Weight_Cutoff")
            )

            temp_Developed_Aggregate = temp_Developed_Aggregate.with_columns(
                                                            (pl.col("Weight_Cutoff").cum_sum()).alias("CumWeight_Cutoff"),
                                                            (-pl.col("Full_MCAP_USD_Cutoff_Company")).rank("dense").alias("Rank")
            )

            # Check if Previous Ranking Company lies between GMSR_Upper_Buffer and GMSR_Lower_Buffer
            
            # List of Unique Dates
            Dates_List = Pivot_TOR.index.to_list()
            IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
            Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

            # Retrieve the Previous Rank in the GMSR Frame
            Previous_Rank_GMSR = GMSR_Frame.filter(pl.col("Date") == Previous_Date).select(pl.col("Rank")).to_numpy()[0][0]

            # CumWeight_Cutoff of the Previous Rank_GMSR
            try:
                CumWeight_Cutoff_Rank = temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0]
            except:
                CumWeight_Cutoff_Rank = 1

            if (GMSR_Upper_Buffer) <= CumWeight_Cutoff_Rank <= (GMSR_Lower_Buffer):
                New_Data = pl.DataFrame({   
                                            "Date": [date],
                                            "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0]],
                                            "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0] * Upper_Limit],
                                            "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0] * Lower_Limit], 
                                            "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0] / 2],
                                            "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0] / 2 * Upper_Limit],
                                            "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Full_MCAP_USD_Cutoff_Company")).to_numpy()[0][0] / 2 * Lower_Limit],
                                            "Rank": [temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("Rank")).to_numpy()[0][0]]
                                        })

            elif CumWeight_Cutoff_Rank < (GMSR_Upper_Buffer):
                New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                        "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                        "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                        "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                        "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                        "Rank": [
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer)
                                                        .head(1)["Full_MCAP_USD_Cutoff_Company"]
                                                        .to_numpy()[0]
                                                    )
                                                    .head(1)["Rank"]
                                                    .to_numpy()[0]
                                                ]})
                
            elif CumWeight_Cutoff_Rank > (GMSR_Lower_Buffer):
                New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                        "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                        "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                        "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                        "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                        "Rank": [
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer)
                                                        .head(1)["Full_MCAP_USD_Cutoff_Company"]
                                                        .to_numpy()[0]
                                                    )
                                                    .head(1)["Rank"]
                                                    .to_numpy()[0]
                                                ]})
                    
            # Drop the Rank column
            temp_Developed_Aggregate = temp_Developed_Aggregate.drop("Rank")

            GMSR_Frame = GMSR_Frame.vstack(New_Data)

            #################################
            ##Start the Size Classification##
            #################################

            # Filter only for Country_Plotting
            # temp_Emerging_Aggregate = temp_Emerging_Aggregate.filter(pl.col("Country") == Country_Plotting)

            # Emerging #
            for country in temp_Emerging_Aggregate.select(pl.col("Country")).unique().sort("Country").to_series():

                # List of Unique Dates
                Dates_List = Pivot_TOR.index.to_list()

                IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
                Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

                Lower_GMSR = GMSR_Frame.select(["GMSR_Emerging_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
                Upper_GMSR = GMSR_Frame.select(["GMSR_Emerging_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

                # ##############
                # #### APG #####
                # ##############
                # Lower_GMSR = GMSR_Frame.select(["GMSR_Developed_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
                # Upper_GMSR = GMSR_Frame.select(["GMSR_Developed_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

                # Check if there is already a previous Index creation for the current country
                if len(Output_Count_Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date))) > 0:
                    
                    # Full Review
                    if (date < datetime.date(2023, 3, 20) and (date.month == 6 or date.month == 12)) or (date >= datetime.date(2023, 3, 20)):

                        TopPercentage, temp_Country = Index_Rebalancing_Box(temp_Emerging_Aggregate, SW_ACALLCAP, Output_Count_Standard_Index, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap,  Right_Limit, Left_Limit, "Emerging" ,writer)

                        # Eligible Companies
                        NewEligible = pl.DataFrame({
                            "Date": [date],
                            "Country": [country],
                            "Count": [len(temp_Country)]
                        })

                        Eligible_Companies = Eligible_Companies.vstack(NewEligible)

                        # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                        TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging, LIF_Stored)
                    
                    # Lite Review for Jun and December
                    else:

                        # Step 1 - Take previous date Index Composition (including Shadow Companies)
                        Previous_Composition = Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date))

                        # Step 2 - Update Close and FX as of current Review and keep FF and NOS as of previous Review

                        # Add NOS and FF from Previous Review Date
                        Previous_Composition = Previous_Composition.join(
                            Securities_Cutoff.filter(pl.col("Review") == Previous_Date).select(pl.col(["Review", "stoxxId", "shares"])),
                            left_on=["Internal_Number"],
                            right_on=["stoxxId"],
                            how="left"
                            ).rename({
                            "shares": "Number_of_Shares_Previous_Quarter"
                            })
                        
                        # Update the date value with the current one
                        Previous_Composition = Previous_Composition.with_columns(
                            pl.lit(date).alias("Date")
                        )

                        # Update Price and FX Rate as of Current Review/Cutoff Date
                        Previous_Composition = Previous_Composition.join(
                            Securities_Cutoff.select(pl.col(["Review", "stoxxId", "closePrice", "validDate", "currency"])),
                            left_on=["Date", "Internal_Number"],
                            right_on=["Review", "stoxxId"],
                            how="left"
                            ).rename({
                            "validDate": "Cutoff",
                            "currency": "Currency",
                            "closePrice": "Close_unadjusted_local_Cutoff"
                            }).join(FX_Cutoff, on=["Cutoff", "Currency"], how="left")
                        
                        # Add FreeFloat and CapFactor as of Current Review/Cutoff Date
                        Previous_Composition = Previous_Composition.join(
                            Emerging.filter(pl.col("Date") == date).select(pl.col(["Internal_Number", "Free_Float", "Capfactor"])),
                            on=["Internal_Number"], how="left"
                        )
                        
                        # Step 3 - Calculate Full_MCAP_USD / Free_Float_MCAP_USD
                        Previous_Composition = Previous_Composition.with_columns(
                            (pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Close_unadjusted_local_Cutoff") * pl.col("Number_of_Shares_Previous_Quarter")).
                            alias("Full_MCAP_USD_Cutoff"),
                            (pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Close_unadjusted_local_Cutoff") * pl.col("Number_of_Shares_Previous_Quarter") *
                             pl.col("Free_Float") * pl.col("Capfactor")).alias("Free_Float_MCAP_USD_Cutoff")
                        )

                        # Drop everything where Full_MCAP is NULL or 0
                        Previous_Composition = Previous_Composition.filter(
                            (pl.col("Full_MCAP_USD_Cutoff") > 0) & (pl.col("Free_Float_MCAP_USD_Cutoff") > 0)
                        )

                        # Check that non Shadow securities have at least 5% 3M ATVR
                        Previous_Composition = Previous_Composition.join(pl.DataFrame(Turnover).with_columns(pl.col("Date").cast(pl.Date)).filter(pl.col("Date") == date),
                                                                         on=["Date", "Internal_Number"], how="left")
                        
                        # Create a mask for securities to be removed
                        Frame_Mask = Previous_Composition.filter(
                            (pl.col("Shadow_Company") == False) &
                            (pl.col("Turnover_Ratio") < 0.05))
                        
                        # Remove securities not passing the 3M ATVR screen
                        Previous_Composition = Previous_Composition.filter(
                            ~pl.col("Internal_Number").is_in(Frame_Mask.select(pl.col("Internal_Number")))
                        )

                        # Aggregate Securities by ENTITIY_QID
                        TopPercentage = Previous_Composition.select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company"),
                                                        pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company")
                                                    ]).sort("Full_MCAP_USD_Cutoff_Company", descending = True)

                        # Adjust the columns
                        TopPercentage = TopPercentage.with_columns(
                                                    (pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Weight_Cutoff"),
                                                    (((pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).cum_sum())).alias("CumWeight_Cutoff")
                                                    ).sort("Full_MCAP_USD_Cutoff_Company", descending=True)
                        
                        # Create temp_Country for next function
                        temp_Country = TopPercentage
                        
                        TopPercentage = TopPercentage.with_columns(pl.lit("Standard").alias("Size"),
                                                                   pl.lit("Lite_Review").alias("Case"))
                        
                        # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                        TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging, LIF_Stored)
                        
                    if Excel_Recap_Rebalancing == True and country == Country_Plotting:

                        # Save DataFrame to Excel
                        TopPercentage.to_pandas().to_excel(writer, sheet_name=f'{date}_{country}', index=False)
                        # Create and save the chart
                        chart_file = Curve_Plotting(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR)

                        # Insert the chart into the Excel file
                        workbook = writer.book
                        worksheet = writer.sheets[f'{date}_{country}']
                        worksheet.insert_image('O2', chart_file)

                        # Check that minimum number is respected

                    # Stack to Output_Standard_Index
                    Output_Standard_Index = Output_Standard_Index.vstack(TopPercentage.select(Output_Standard_Index.columns))
                    
                    # Create the Output_Count_Standard_Index for future rebalacing
                    Output_Count_Standard_Index = Output_Count_Standard_Index.vstack(TopPercentage.group_by("Country").agg(
                        pl.len().alias("Count"),
                        pl.col("Date").first().alias("Date")
                    ).sort("Count", descending=True))
                
                # If there is no composition, a new Index will be created
                else:
                    TopPercentage, temp_Country = Index_Creation_Box(temp_Emerging_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Excel_Recap, Percentage, Right_Limit, Left_Limit, "Emerging", writer)
                    
                    # Eligible Companies
                    NewEligible = pl.DataFrame({
                        "Date": [date],
                        "Country": [country],
                        "Count": [len(temp_Country)]
                    })

                    Eligible_Companies = Eligible_Companies.vstack(NewEligible)

                    # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                    TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging, LIF_Stored)

                    if Excel_Recap_Rebalancing == True and country == Country_Plotting:

                        # Save DataFrame to Excel
                        TopPercentage.to_pandas().to_excel(writer, sheet_name=f'{date}_{country}', index=False)
                        # Create and save the chart
                        chart_file = Curve_Plotting(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR)

                        # Insert the chart into the Excel file
                        workbook = writer.book
                        worksheet = writer.sheets[f'{date}_{country}']
                        worksheet.insert_image('O2', chart_file)

                    # Stack to Output_Standard_Index
                    Output_Standard_Index = Output_Standard_Index.vstack(TopPercentage.select(Output_Standard_Index.columns))
                    
                    # Create the Output_Count_Standard_Index for future rebalacing
                    Output_Count_Standard_Index = Output_Count_Standard_Index.vstack(TopPercentage.group_by("Country").agg(
                        pl.len().alias("Count"),
                        pl.col("Date").first().alias("Date")
                    ).sort("Count", descending=True))

                #################################
                ###########Assign Size###########
                #################################

                # Standard Index #
                Standard_Index = Standard_Index.vstack(TopPercentage_Securities.select(Standard_Index.columns))

                # Small Index #
                Emerging_Small = temp_Emerging.filter((~pl.col("Internal_Number").is_in(TopPercentage_Securities.select(pl.col("Internal_Number")))) & (
                    pl.col("Country") == country)).select(
                    pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])
                ).with_columns(
                    pl.lit("Small").alias("Size"),
                    pl.lit(False).alias("Shadow_Company")).select(Small_Index.columns).head(0)
                
                Small_Index = Small_Index.vstack(Emerging_Small)

        end_time_single_date = time.time()

        execution_time_single_date = end_time_single_date - start_time_single_date
        print(f"Execution time: {execution_time_single_date} seconds")

    # Add Recap_GMSR_Frame to Excel
    GMSR_Frame.to_pandas().to_excel(writer, sheet_name="GMSR_Historical", index=False)

### Standard Index ###

# Implement to remove Shadow Company here #
Standard_Index_Shadow = Standard_Index
Standard_Index = Standard_Index.filter(pl.col("Shadow_Company")==False)

# Add SEDOL/ISIN
Standard_Index = Standard_Index.join(Emerging.select(pl.col(["Date", "Internal_Number", "ISIN", "SEDOL"])), on=["Date", "Internal_Number"], how="left")

# Add information of CapFactor/Mcap_Units_Index_Currency
Standard_Index = Standard_Index.join(pl.read_parquet(
    r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Universe\STXWAGV_Review_with_Dec24.parquet").with_columns(
        pl.col("Date").cast(pl.Date),
        pl.col("Mcap_Units_Index_Currency").cast(pl.Float64)
    ), on=["Date", "Internal_Number"], how="left")

# Calculate the Weights for each Date
Standard_Index = Standard_Index.with_columns(
    (pl.col("Mcap_Units_Index_Currency") / pl.col("Mcap_Units_Index_Currency").sum().over("Date")).alias("Weight")
)

### Standard Shadow Index ###
# Add SEDOL/ISIN
Standard_Index_Shadow = Standard_Index_Shadow.join(Emerging.select(pl.col(["Date", "Internal_Number", "ISIN", "SEDOL"])), on=["Date", "Internal_Number"], how="left")

# Add information of CapFactor/Mcap_Units_Index_Currency
Standard_Index_Shadow = Standard_Index_Shadow.join(pl.read_parquet(
    r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Universe\STXWAGV_Review_with_Dec24.parquet").with_columns(
        pl.col("Date").cast(pl.Date),
        pl.col("Mcap_Units_Index_Currency").cast(pl.Float64)
    ), on=["Date", "Internal_Number"], how="left")

# Calculate the Weights for each Date
Standard_Index_Shadow = Standard_Index_Shadow.with_columns(
    (pl.col("Mcap_Units_Index_Currency") / pl.col("Mcap_Units_Index_Currency").sum().over("Date")).alias("Weight")
)

# Recap Standard Index
Recap_Count_Standard = (
    Standard_Index
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
    Standard_Index
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

# Store the Results
Standard_Index.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\AllCap_Index_Security_Level_{GMSR_Upper_Buffer}_{GMSR_Lower_Buffer}_" + current_datetime + ".csv")
Standard_Index_Shadow.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\AllCap_Index_Security_Level_Shadows_{GMSR_Upper_Buffer}_{GMSR_Lower_Buffer}_" + current_datetime + ".csv")
Eligible_Companies.pivot(values="Count", index="Country", columns="Date").write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\AllCap_EligibleSecurities_{GMSR_Upper_Buffer}_{GMSR_Lower_Buffer}_" + current_datetime + ".csv")
EMS_Frame.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\EMS_Frame.csv")
GMSR_Frame.write_csv(rf"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based\Output\Tests\GMSR_Frame.csv")

# Delete .PNG from main folder
Main_path = r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\TMI_Based"

# Use glob to find all PNG files in the folders
PNG_Files = glob.glob(os.path.join(Main_path, '*.PNG'))

# Iterate over the list of PNG files and remove them
for file in PNG_Files:
    try:
        os.remove(file)
        print(f"Deleted: {file}")
    except Exception as e:
        print(f"Error deleting file {file}: {e}")

end_time = time.time()
execution_time = end_time - start_time
print(f"Execution time: {execution_time} seconds")