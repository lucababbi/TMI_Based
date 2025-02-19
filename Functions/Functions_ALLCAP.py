import polars as pl
import datetime
import numpy as np
import math

##################################
###########Trading Days###########
##################################
def Trading_Frequency(Frame: pl.DataFrame, Trading_Days, date, Starting_Date, Segment, Pivot_TOR, Trading_Days_OLD, Trading_Days_NEW, Screened_Securities):

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
            temp_Emerging_Country = temp_Emerging_Country.sort("Free_Float_MCAP_USD_Cutoff", descending = True).head(3 - len(TopPercentage_Securities))

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
def FOR_Screening(Screened_Securities, Frame: pl.DataFrame, Full_Frame: pl.DataFrame, Pivot_TOR, AllCap_Index, date, Segment: pl.Utf8, Entity_ID, FOR_FF_Screen) -> pl.DataFrame:

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
        

        #TODO: In the future, to determine if a Security was included in the Index, should look at AllCap_Index not Screened_Securities
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
            Temp_Screened_Securities = AllCap_Index.filter((pl.col("Country")==country) & (pl.col("Date")==Previous_Date))

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
        
        # Calculate FIF for the Refresehed Universe
        temp_Frame = temp_Frame.with_columns(
                                    # Main case where FOR is higher than 25%
                                    pl.when(pl.col("foreign_headroom") >= 0.25)
                                    .then(pl.col("Free_Float") * 1)
    	                            # Case 1 for New Constituents in between limits
                                    .when((pl.col("foreign_headroom") >= 0.15) & (pl.col("foreign_headroom") < 0.25) & (pl.col("InPrevScreened_Universe") == False))
                                    .then(pl.col("Free_Float") * 0.5)
                                    # Case 2 for Current Constituents in between limits
                                    .when((pl.col("foreign_headroom") >= 0.0375) & (pl.col("foreign_headroom") < 0.25) & (pl.col("InPrevScreened_Universe") == True))
                                    .then(pl.col("Free_Float") * 0.5)
                                    # Case 3 for New Constituents below the limit
                                    .when((pl.col("foreign_headroom") < 0.15) & (pl.col("InPrevScreened_Universe") == False))
                                    .then(pl.col("Free_Float") * 0)
                                    # Case 4 for Current Constituents below the limit
                                    .when((pl.col("foreign_headroom") < 0.0375) & (pl.col("InPrevScreened_Universe") == True))
                                    .then(pl.col("Free_Float") * 0)
                                    .otherwise(None)  # Ensure all cases are handled
                                    .alias("FIF")
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
                Investable_Index = AllCap_Index.filter((pl.col("Country")==country) & (pl.col("Date")==Previous_Date)).join(
                                        Full_Frame_Country.select(pl.col(["Internal_Number", "Full_MCAP_USD_Cutoff_Company"])), 
                                        on=["Internal_Number"], how="left").filter((pl.col("Full_MCAP_USD_Cutoff_Company") > 0)
                                        ).sort("Full_MCAP_USD_Cutoff_Company", descending=True)     

            # Case where Investable_Index is NULL
            if len(Investable_Index) > 0:
                
                if Segment == "Emerging":
                    # Count the Companies that made it in the previous Basket
                    Previous_Count = len(AllCap_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date))) - 1
                
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

        # Stack the resulting Frame
        Screened_Frame = Screened_Frame.vstack(temp_Frame)
            
    return Screened_Frame

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
def Turnover_Check(Frame: pl.DataFrame, Pivot_TOR: pl.DataFrame, Threshold_NEW, Threshold_OLD, date, Starting_Date, Segment: pl.Utf8, Turnover, Screened_Securities) -> pl.DataFrame:

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
def Turnover_Check_12M(Frame: pl.DataFrame, Pivot_TOR_12M: pl.DataFrame, Threshold_NEW_12M, Threshold_OLD_12M, date, Starting_Date, Segment: pl.Utf8, Turnover12M, Screened_Securities) -> pl.DataFrame:

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
def Equity_Minimum_Size(df: pl.DataFrame, Pivot_TOR, EMS_Frame, date, Segment: pl.Utf8, Screened_Securities, temp_Exchanges_Securities, Entity_ID, Starting_Date, MSCI_Equity_Minimum_Size) -> pl.DataFrame:
    # List to hold results
    results = []

    # List of Unique Dates
    Dates_List = Pivot_TOR.index.to_list()

    try:
        IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
        if (date < datetime.date(2023, 3, 20) and (date.month == 3 or date.month == 9)) or (date >= datetime.date(2023, 3, 20)):
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
                min_size_company = df_date.filter(pl.col("Cumulative_Coverage_Cutoff") >= 0.995).select("Full_MCAP_USD_Cutoff_Company").head(1)
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
def Minimum_FreeFloat_Country(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, Segment: pl.Utf8, Original_MCAP_Emerging, LIF_Stored, Starting_Date, Output_AllCap_Index, temp_Emerging, ETF,
                              Pivot_TOR, AllCap_Index):

    # Create the list of Dates
    Dates_List = Pivot_TOR.index.to_list()
    IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
    Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

    # No Buffer for the Starting Date
    if (date == Starting_Date) | (len(Output_AllCap_Index.filter(pl.col("Country") == country)) == 0):

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
            TopPercentage_Securities = TopPercentage_Securities.filter(pl.col("Full_MCAP_USD_Cutoff_Company") > Country_Cutoff).drop("Full_MCAP_USD_Cutoff_Company")
        
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
        QID_Standard_Index = AllCap_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(
            pl.col("ENTITY_QID", "Shadow_Company", "Internal_Number"))

        # Get which of the Current Index Components are still Investable by checking temp_Emerging/temp_Developed after Screens have been applied to them
        if Segment == "Emerging":
            Security_Standard_Index_Current = QID_Standard_Index.join(temp_Emerging.select(pl.col(["Internal_Number", "Country"])),
                on=["Internal_Number"], how="left")
        
        # Group them by ENTITY_QID
        Company_Standard_Index_Current = Security_Standard_Index_Current.group_by(
                                                    ["ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Shadow_Company").first().alias("Shadow_Company"),
                                                    ]).with_columns(
                                                        pl.lit("All_Cap").alias("Size")
                                                    )
        
        # Create the Current All_Cap Index
        Current_Index = Company_Standard_Index_Current

        # Add information of Standard/Small Companies to Refreshed Universe
        temp_Country = temp_Country.join(Current_Index.select(pl.col(["ENTITY_QID", "Shadow_Company", "Size"])), on=["ENTITY_QID"], how="left").with_columns(
            pl.col("Size").fill_null("NEW")
        )
        
        #################
        # Case Analysis #
        #################

        # Case inside the box
        if (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] <= Upper_GMSR) & (TopPercentage.tail(1).select("Full_MCAP_USD_Cutoff_Company").to_numpy()[0][0] >= Lower_GMSR):

            if ((date.month == 6 or date.month == 12)) and (date < datetime.date(2023,3,20)):

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
            
            if ((date.month == 6 or date.month == 12)) and (date < datetime.date(2023,3,20)):

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

            if ((date.month == 6 or date.month == 12)) and (date < datetime.date(2023,3,20)):

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

        if ((date.month == 6 or date.month == 12)) and (date < datetime.date(2023,3,20)):

            # Transform TopPercentage from Companies to Securities level
            TopPercentage_Securities = temp_Emerging.select(pl.col("Date", "Internal_Number", "Instrument_Name",
                                    "ENTITY_QID", "Country", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff")).filter(pl.col("ENTITY_QID").is_in(TopPercentage.select(
                                        pl.col("ENTITY_QID").unique()
                                    ))).with_columns(
                                        pl.lit("All_Cap").alias("Size")
                                    )
            
            # Add information of what was Shadow in Previous Index
            TopPercentage_Securities = TopPercentage_Securities.join(
                                                                AllCap_Index
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
                TopPercentage_Securities, TopPercentage = Index_Continuity(TopPercentage_Securities, TopPercentage, "Emerging", temp_Emerging, country, AllCap_Index)

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
                        AllCap_Index.filter((pl.col("Date") == Previous_Date) & (pl.col("Country") == country) & (pl.col("Shadow_Company") == False)).select(pl.col("ENTITY_QID"))
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
            TopPercentage_Securities = TopPercentage_Securities.join(AllCap_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(
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
                TopPercentage_Securities, TopPercentage = Index_Continuity(TopPercentage_Securities, TopPercentage, "Emerging", temp_Emerging, country, AllCap_Index)

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
def Index_Creation_Box(Frame: pl.DataFrame, Lower_GMSR, Upper_GMSR, country, date, Percentage, Right_Limit, Left_Limit, Segment: pl.Utf8):

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
    .filter(pl.col("CumWeight_Cutoff") <= Country_Percentage)  # Ensure correct type
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
        .filter(pl.col("CumWeight_Cutoff") > Country_Percentage)  # Ensure correct type
        .head(1)  # Correctly placed
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
def Index_Rebalancing_Box(Frame: pl.DataFrame, SW_ACALLCAP, Output_Count_AllCap_Index, Lower_GMSR, Upper_GMSR, country, date, Right_Limit, Left_Limit, Segment: pl.Utf8, Pivot_TOR, Output_AllCap_Index, AllCap_Index, Emerging,
                          Securities_Cutoff, FX_Cutoff, GMSR_Frame, Percentage):
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
    IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
    Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

    # Information at Company Level
    QID_Standard_Index = Output_AllCap_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(pl.col("Date", "ENTITY_QID"))

    # Check which of the QID_Standard_Index Companies is still alive (it is not relevant if it does not pass the Screens)
    # Duplicates are dropped due to ENTITY_QID / Keep only Free_Float_MCAP_USD_Cutoff > 0
    QID_Standard_Index = QID_Standard_Index.join(Emerging.filter((pl.col("Country")==country) & (pl.col("Date")==date)).select(pl.col("Free_Float_MCAP_USD_Cutoff",
                        "ENTITY_QID")), on=["ENTITY_QID"], how="left").unique(subset=["ENTITY_QID"]).filter(pl.col("Free_Float_MCAP_USD_Cutoff") > 0)

    ######################################
    # Adjustment Company_Selection_Count #
    ######################################

    # Sort Old Universe by FullMCAPUSDCutoff by using # of Shares and FreeFloat
    def ChairsSorting(Output_AllCap_Index, AllCap_Index, Securities_Cutoff, country, Previous_Date, date, FX_Cutoff):

        # Filter for Country and Previous_Date Review at Security_Level #
        # AllCapIndex from Previous Quarter #        
        Starting_Universe = AllCap_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(["Date", "Internal_Number", 
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
        temp_Output_Standard_Index = (Output_AllCap_Index.filter((pl.col("Country")==country) & ((pl.col("Date") == Previous_Date)))
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
    PositionFullMCAP = ChairsSorting(Output_AllCap_Index, AllCap_Index, Securities_Cutoff, country, Previous_Date, date, FX_Cutoff)

    # Take the initial Selection
    TopPercentage = temp_Country.sort("Full_MCAP_USD_Cutoff_Company", descending=True).filter(pl.col("Full_MCAP_USD_Cutoff_Company") >= PositionFullMCAP)

    # Determine the Proximity Areas #
    Lower_Proximity_Bound = (Lower_GMSR, GMSR_Frame.select(["GMSR_Emerging", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0] * 0.575)
    Upper_Proximity_Bound = (GMSR_Frame.select(["GMSR_Emerging", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0], Upper_GMSR)

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
