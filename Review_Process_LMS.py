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
import sys

# Folder with Functions
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "Functions"))

start_time = time.time()

##################################
############Functions#############
##################################

# ALL CAP Functions #
from Functions.Functions_ALLCAP import Trading_Frequency as Trading_Frequency_ALLCAP
from Functions.Functions_ALLCAP import China_A_Securities as China_A_Securities_ALLCAP
from Functions.Functions_ALLCAP import Index_Continuity as Index_Continuity_ALLCAP
from Functions.Functions_ALLCAP import FOR_Screening as FOR_Screening_ALLCAP
from Functions.Functions_ALLCAP import Turnover_Check as Turnover_Check_ALLCAP
from Functions.Functions_ALLCAP import Turnover_Check_12M as Turnover_Check_12M_ALLCAP
from Functions.Functions_ALLCAP import Fill_Chairs as Fill_Chairs_ALLCAP
from Functions.Functions_ALLCAP import Minimum_FreeFloat_Country as Minimum_FreeFloat_Country_ALLCAP
from Functions.Functions_ALLCAP import Index_Creation_Box as Index_Creation_Box_ALLCAP
from Functions.Functions_ALLCAP import Index_Rebalancing_Box as Index_Creation_ALLCAP
from Functions.Functions_ALLCAP import Equity_Minimum_Size as Equity_Minimum_Size_ALLCAP
from Functions.Functions_ALLCAP import Index_Rebalancing_Box as Index_Rebalancing_Box_ALLCAP 

# STANDARD Functions #
from Functions.Functions_STANDARD import Trading_Frequency as Trading_Frequency_STANDARD
from Functions.Functions_STANDARD import China_A_Securities as China_A_Securities_STANDARD
from Functions.Functions_STANDARD import Index_Continuity as Index_Continuity_STANDARD
from Functions.Functions_STANDARD import FOR_Screening as FOR_Screening_STANDARD
from Functions.Functions_STANDARD import Turnover_Check as Turnover_Check_STANDARD
from Functions.Functions_STANDARD import Turnover_Check_12M as Turnover_Check_12M_STANDARD
from Functions.Functions_STANDARD import Fill_Chairs as Fill_Chairs_STANDARD
from Functions.Functions_STANDARD import Minimum_FreeFloat_Country as Minimum_FreeFloat_Country_STANDARD
from Functions.Functions_STANDARD import Index_Creation_Box as Index_Creation_Box_STANDARD
from Functions.Functions_STANDARD import Index_Rebalancing_Box as Index_Rebalancing_Box_STANDARD
from Functions.Functions_STANDARD import Equity_Minimum_Size as Equity_Minimum_Size_STANDARD

import polars as pl

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
        if (len(temp_Frame.filter(pl.col("Index_Symbol") == "SXGDRGV"))) > 1 and len(temp_Frame) > 1:
            # Keep only the one to remove from the final Frame
            temp_Frame = temp_Frame.filter(pl.col("Index_Symbol") == "SXGDRGV")

            Removal_Frame = Removal_Frame.vstack(temp_Frame)

    # Remove not valid ADR from the Frame
    Frame = Frame.filter(~pl.col("Internal_Number").is_in(Removal_Frame.select("Internal_Number")))

    return Frame

##################################
###########Parameters#############
##################################

Starting_Date = date(2012, 6, 18)
Upper_Limit = 1.15
Lower_Limit = 0.50

################################
###########L&M CAP##############
################################
Percentage = [0.99, 0.85]
Left_Limit = [Percentage[0] - 0.005, Percentage[1] - 0.05]
Right_Limit = [1, Percentage[1] +  0.05]

###############################
###########ATVR 3M#############
###############################
Threshold_NEW = 0.15
Threshold_OLD = 0.05

###############################
##########ATVR 12M#############
###############################
Threshold_NEW_12M = 0.15
Threshold_OLD_12M = 0.10

###############################
########Trading Days###########
###############################
Trading_Days_NEW = 0.80
Trading_Days_OLD = 0.70

###############################
###########FIF Screen##########
###############################
FOR_FF_Screen = 0.15

###############################
############GMSR###############
###############################
GMSR_Upper_Buffer = [0.99, 0.85]
GMSR_Lower_Buffer = [0.9925, 0.87]
GMSR_MSCI = [np.float64(330 * 1_000_000), np.float64(3_592 * 1_000_000)]

###############################
#############EMS###############
###############################
MSCI_Equity_Minimum_Size = (130 * 1_000_000)

###############################
#######Country Adjustment###### 
###############################
MSCI_Curve_Adjustment = pl.DataFrame({"Country": ["AU", "BG", "BR", "CA", "CL", "CN", "CO", "EG", "HK", "HU", "ID", "IL", "IN", "JP", "KR", "MA", "MX", 
                        "MY", "PH", "PL", "RU", "SG", "TH", "TR", "TW", "US", "ZA", "DK", "IE", "CH", "GB", "NL", "SE", "AT", 
                        "GR", "NO", "FR", "ES", "DE", "FI", "IT", "BE", "PT", "CZ", "GR", "NZ"],
                        "Coverage": [0.860, 0.860, 0.950, 0.846, 0.901, 0.901, 0.990, 0.860, 0.875, 0.975, 0.821, 0.810, 0.869, 0.841, 
                        0.878, 0.910, 0.950, 0.830, 0.815, 0.890, 0.995, 0.837, 0.825, 0.815, 0.810, 0.862, 0.875, 0.900, 
                        0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 0.900, 
                        0.900, 0.85, 0.85, 0.85]})

###############################
###########Triggers############
###############################
#TODO Add a description of the Triggers and verify if anything can be removed 
Coverage_Adjustment = True 
Screen_TOR = True
FullListSecurities = True

###############################
#############Input#############
###############################
# SW AC ALLCAP for check on Cutoff
SW_ACALLCAP = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\STXWAGV_Cutoff_with_Dec24.parquet").with_columns([
                                pl.col("Date").cast(pl.Date),
                                pl.col("Mcap_Units_Index_Currency").cast(pl.Float64)
]).filter(pl.col("Mcap_Units_Index_Currency") > 0).join(pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Dates\Review_Date-QUARTERLY.csv").with_columns(
                        pl.col("Review").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y"),
                        pl.col("Cutoff").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y")
                      ), left_on="Date", right_on="Cutoff", how="left")

# ETF Frame for the initial Review Date
ETF = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\ETFs_STANDARD-SMALL.csv", separator=";")

# List of Cutoff & Review Date
Full_Dates = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Dates\Review_Date-QUARTERLY.csv").with_columns(
    pl.col("Review").str.to_date("%m/%d/%Y"),
    pl.col("Cutoff").str.to_date("%m/%d/%Y")
)

# Country Coverage for Index Creation
Country_Coverage = pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\Country_Coverage.csv", separator=";")

# Base Universe for Index Creation
TMI = (
    pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\TMI_Based\Universe\Input_Code\Final_Universe_TMI.parquet")
    .with_columns([
        pl.col("Free_Float").cast(pl.Float64),
        pl.col("Capfactor").cast(pl.Float64),
        pl.col("Date").cast(pl.Date)
    ])
    .drop("Mcap_Units_Index_Currency", "ICB", "Cutoff")
)

# Read DEVELOPED & EMERGING Securities to determine Countries to keep in TMI
SW_Frame = (
    pl.concat([
        pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\SWDACGV_with_Dec24.parquet")
        .with_columns([
            pl.col("Date").cast(pl.Date),
            pl.lit("Developed").alias("Segment")
        ]),
        pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\SWEACGV_with_Dec24.parquet")
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

# GCC Extra to Vstack prior to September 2022
GCC = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Universe\GCC.parquet").with_columns([
                            pl.col("Free_Float").cast(pl.Float64),
                            pl.col("Capfactor").cast(pl.Float64),
                            pl.col("Date").cast(pl.Date),
                            pl.col("ICB").cast(pl.Utf8),
                            pl.col("Exchange").cast(pl.Utf8)
                            ]).filter(pl.col("Date") >= datetime.date(2019,6,24)).drop("Index_Name").with_columns(
                                pl.lit("Emerging").alias("Segment")
                            ).drop("ICB")

# All Listed Securities used to determine Minimum Capitalization at Company level #TODO Adjust multiple listing for the same Security
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

# Add Review Date column
Exchanges_Securities = Exchanges_Securities.join(pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Dates\Review_Date-QUARTERLY.csv").with_columns(
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

# Drop Pakistan from DEC-2021 to replicate MSCI Portfolio composition
Emerging = Emerging.filter(~((pl.col("Date") >= datetime.date(2021,12,20)) & (pl.col("Country") == "PK")))

# Entity_ID for matching Companies
Entity_ID = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Entity_ID\Entity_ID.parquet").select(pl.col(["ENTITY_QID", "STOXX_ID",
                            "RELATIONSHIP_VALID_FROM", "RELATIONSHIP_VALID_TO"])).with_columns(
                                pl.col("RELATIONSHIP_VALID_FROM").cast(pl.Date()),
                                pl.col("RELATIONSHIP_VALID_TO").cast(pl.Date()))

###################################
#####Filtering from StartDate######
###################################
Emerging = Emerging.filter(pl.col("Date") >= Starting_Date)
Developed = Developed.filter(pl.col("Date") >= Starting_Date)

##################################
######Add Cutoff Information######
##################################
Columns = ["validDate", "stoxxId", "currency", "closePrice", "shares", "freeFloat"]

# Read the Parquet and add the Review Date Column 
Securities_Cutoff = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Securities_Cutoff\Securities_Cutoff_with_Dec24.parquet", columns=Columns).with_columns([
                      pl.col("closePrice").cast(pl.Float64),
                      pl.col("freeFloat").cast(pl.Float64),
                      pl.col("shares").cast(pl.Float64),
                      pl.col("validDate").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d")
                      ]).join(pl.read_csv(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Dates\Review_Date-QUARTERLY.csv").with_columns(
                        pl.col("Review").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y"),
                        pl.col("Cutoff").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y")
                      ), left_on="validDate", right_on="Cutoff", how="left").rename({"freeFloat": "FreeFloat_Cutoff"})

FX_Cutoff = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Securities_Cutoff\FX_Historical_with_Dec24.parquet").with_columns(
                            pl.col("Cutoff").cast(pl.Date)
)

# Add Price / Number of Shares / FX Rate information to the Developed and Emerging Frames
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
    ).join(FX_Cutoff, on=["Cutoff", "Currency"], how="left")

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
    ).join(FX_Cutoff, on=["Cutoff", "Currency"], how="left")


##################################
#########Drop Empty Rows##########
##################################
Developed = Developed.filter(~((pl.col("FX_local_to_Index_Currency_Cutoff").is_null()) | (pl.col("Close_unadjusted_local_Cutoff").is_null()) | (pl.col("Shares_Cutoff").is_null())))
Emerging = Emerging.filter(~((pl.col("FX_local_to_Index_Currency_Cutoff").is_null()) | (pl.col("Close_unadjusted_local_Cutoff").is_null()) | (pl.col("Shares_Cutoff").is_null())))

##################################
####Read Turnover Information#####
##################################
# TurnOverRatio
Turnover = pl.read_parquet(r"C:\Users\lbabbi\OneDrive - ISS\Desktop\Projects\SAMCO\V0_SAMCO\Turnover\Turnover_Cutoff_SWALL_with_Dec24.parquet")
# Drop unuseful columns
Turnover = Turnover.drop(["vd", "calcType", "token"])
# Keep only relevant fields
Turnover = Turnover.filter(pl.col("field").is_in(["TurnoverRatioFO", "TurnoverRatioFO_India1"]))
# Pivot the Frame to simplify checks on the previous four quarters
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
# Add ENTITY_QID to the main Frames
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
# Pivot the Frame to simplify checks on the previous four quarters
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

##################################
####Marking Chinese Securities####
##################################
# Mask CN Securities
Chinese_CapFactor = China_A_Securities_ALLCAP(Emerging)

# Add the information to Emerging Universe
Emerging = Emerging.join(Chinese_CapFactor.select(pl.col(["Date", "Internal_Number", "Capfactor_CN"])), on=["Date", "Internal_Number"], how="left").with_columns(
                        pl.col("Capfactor_CN").fill_null(pl.col("Capfactor"))).drop("Capfactor").rename({"Capfactor_CN": "Capfactor"})

# Calculate Free/Full MCAP USD for Developed Universe
Developed = Developed.with_columns(
                                    (pl.col("Free_Float") * pl.col("Capfactor") * pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Free_Float_MCAP_USD_Cutoff"),
                                    (pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Full_MCAP_USD_Cutoff")
                                  )

# Calculate Free/Full MCAP USD for Emerging Universe
Emerging = Emerging.with_columns(
                                    (pl.col("Free_Float") * pl.col("Capfactor") * pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Free_Float_MCAP_USD_Cutoff"),
                                    (pl.col("Close_unadjusted_local_Cutoff") * pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Shares_Cutoff"))
                                    .alias("Full_MCAP_USD_Cutoff")
                                  )

# Check if there is any Free_Float_MCAP_USD_Cutoff Empty
Emerging = Emerging.filter((pl.col("Free_Float_MCAP_USD_Cutoff") > 0) & (pl.col("Free_Float_MCAP_USD_Cutoff")).is_not_nan())
Developed = Developed.filter((pl.col("Free_Float_MCAP_USD_Cutoff") > 0) & (pl.col("Free_Float_MCAP_USD_Cutoff")).is_not_nan())

###################################
######Frames Support Creation######
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

GMSR_Frame_STANDARD = pl.DataFrame({
    "Date": pl.Series(dtype=pl.Date),
    "GMSR_Developed": pl.Series(dtype=pl.Float64),
    "GMSR_Developed_Upper": pl.Series(dtype=pl.Float64),
    "GMSR_Developed_Lower": pl.Series(dtype=pl.Float64),
    "GMSR_Emerging": pl.Series(dtype=pl.Float64),
    "GMSR_Emerging_Upper": pl.Series(dtype=pl.Float64),
    "GMSR_Emerging_Lower": pl.Series(dtype=pl.Float64),
    "Rank": pl.Series(dtype=pl.UInt32)
})

EMS_Frame = pl.DataFrame({
                        "Date": pl.Series([], dtype=pl.Date),
                        "Segment": pl.Series([], dtype=pl.Utf8),
                        "EMS": pl.Series([], dtype=pl.Float64),
                        "Rank": pl.Series([], dtype=pl.Int64),
                        "Total": pl.Series([], dtype=pl.Int64),
                        "FreeFloatMCAP_Minimum_Size": pl.Series([], dtype=pl.Float64)
})

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

Output_AllCap_Index = pl.DataFrame({
    "Date": pl.Series([], dtype=pl.Date),
    "ENTITY_QID": pl.Series([], dtype=pl.Utf8),
    "Country": pl.Series([], dtype=pl.Utf8),
    "Size": pl.Series([], dtype=pl.Utf8),
    "Case": pl.Series([], dtype=pl.Utf8)
})

Output_Count_AllCap_Index = pl.DataFrame({
    "Country": pl.Series([], dtype=pl.Utf8),
    "Count": pl.Series([], dtype=pl.UInt32),
    "Date": pl.Series([], dtype=pl.Date),
})


Screened_Securities = pl.DataFrame({
    "Date": pl.Series([], dtype=pl.Date),
    "Internal_Number": pl.Series([], dtype=pl.Utf8),
    "Segment": pl.Series([], dtype=pl.Utf8),
    "Country": pl.Series([], dtype=pl.Utf8)
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

AllCap_Index = pl.DataFrame({
    "Date": pl.Series([], dtype=pl.Date),
    "Internal_Number": pl.Series([], dtype=pl.Utf8),
    "Instrument_Name": pl.Series([], dtype=pl.Utf8),
    "ENTITY_QID": pl.Series([], dtype=pl.Utf8),
    "Country": pl.Series([], dtype=pl.Utf8),
    "Size": pl.Series([], dtype=pl.Utf8),
    "Shadow_Company": pl.Series([], dtype=pl.Boolean)
})

AllCap_Index_Shadow = pl.DataFrame({
    "Date": pl.Series([], dtype=pl.Date),
    "Internal_Number": pl.Series([], dtype=pl.Utf8),
    "Instrument_Name": pl.Series([], dtype=pl.Utf8),
    "ENTITY_QID": pl.Series([], dtype=pl.Utf8),
    "Country": pl.Series([], dtype=pl.Utf8),
    "Size": pl.Series([], dtype=pl.Utf8),
    "Shadow_Company": pl.Series([], dtype=pl.Boolean)
})

# LIF Frame
LIF_Stored = pl.DataFrame(
    schema={
        "Date": pl.Date,
        "Internal_Number": pl.Utf8,
        "LIF": pl.Float64
    }
)

###################################
#####Beginning of Review Process###
###################################
for date in Emerging.select(["Date"]).unique().sort("Date").to_series():
            
    start_time_single_date = time.time()
    print(date)

    # Keep only a slice of Frame with the current Date
    temp_Emerging = Emerging.filter(pl.col("Date") == date)
    temp_Developed = Developed.filter(pl.col("Date") == date)
    temp_Exchanges_Securities = Exchanges_Securities.filter((pl.col("Date") == date) & (pl.col("Country").is_in(temp_Developed.select(pl.col("Country")).unique())))

    # First Review Date where Index is created
    if date == Starting_Date: 

        ###################################
        ################ADR################
        ###################################
        temp_Emerging = ADR_Removal(temp_Emerging, Emerging, Developed, "Emerging").drop("Occurrence_Count", "Index_Symbol_right")
        temp_Developed = ADR_Removal(temp_Developed, Emerging, Developed, "Developed").drop("Occurrence_Count", "Index_Symbol_right")

        ###################################
        ##########Apply EMS Screen#########
        ###################################

        # Apply the Screens
        temp_Developed, EMS_Frame, Temp_Screened_Securities, equity_universe_min_size = Equity_Minimum_Size_ALLCAP(temp_Developed, Pivot_TOR, EMS_Frame, date,
                                                        "Developed", Screened_Securities, temp_Exchanges_Securities, Entity_ID, Starting_Date, MSCI_Equity_Minimum_Size)
        # Screen FreeFloat MarketCap
        temp_Developed = temp_Developed.filter(pl.col("Free_Float_MCAP_USD_Cutoff") > EMS_Frame.filter(pl.col("Date") == date).select(
            pl.col("FreeFloatMCAP_Minimum_Size")).to_numpy()[0][0]).select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number",
            "Instrument_Name", "Free_Float", "Capfactor", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"]))
        
        temp_Emerging, EMS_Frame, Temp_Screened_Securities , equity_universe_min_size= Equity_Minimum_Size_ALLCAP(temp_Emerging, Pivot_TOR, EMS_Frame, date, 
                                                        "Emerging", Screened_Securities, temp_Exchanges_Securities, Entity_ID, Starting_Date, MSCI_Equity_Minimum_Size)
        # Screen FreeFloat MarketCap
        temp_Emerging = temp_Emerging.filter(pl.col("Free_Float_MCAP_USD_Cutoff") > EMS_Frame.filter(pl.col("Date") == date).select(
            pl.col("FreeFloatMCAP_Minimum_Size")).to_numpy()[0][0]).select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number",
            "Instrument_Name", "Free_Float", "Capfactor", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"]))

        ###################################
        ####TurnoverRatio12M Screening#####
        ###################################

        # Apply the Check on Turnover for all Components
        Developed_Screened_12M = Turnover_Check_12M_ALLCAP(temp_Developed, Pivot_TOR_12M, Threshold_NEW_12M, Threshold_OLD_12M, date, Starting_Date, "Developed", Turnover12M, Screened_Securities)
        Emerging_Screened_12M = Turnover_Check_12M_ALLCAP(temp_Emerging, Pivot_TOR_12M, Threshold_NEW_12M, Threshold_OLD_12M, date, Starting_Date, "Emerging", Turnover12M, Screened_Securities)

        # Remove Securities not passing the screen
        temp_Developed = temp_Developed.join(Developed_Screened_12M, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
        temp_Emerging = temp_Emerging.join(Emerging_Screened_12M, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
        
        ###################################
        ######TurnoverRatio Screening######
        ###################################

        # Apply the Check on Turnover for all Components
        Developed_Screened = Turnover_Check_ALLCAP(temp_Developed, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date, "Developed", Turnover, Screened_Securities)
        Emerging_Screened = Turnover_Check_ALLCAP(temp_Emerging, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date, "Emerging", Turnover, Screened_Securities)

        # Remove Securities not passing the screen
        temp_Developed = temp_Developed.join(Developed_Screened, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
        temp_Emerging = temp_Emerging.join(Emerging_Screened, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)

        ###################################
        #########FOR FF Screening##########
        ###################################

        # Filter for FOR_FF >= FOR_FF #TODO Adjust with the correct function
        temp_Developed = temp_Developed.with_columns(
                            (pl.col("Free_Float") * pl.col("Capfactor")).alias("FOR_FF")
                            ).filter(pl.col("FOR_FF") >= FOR_FF_Screen)
        temp_Emerging = temp_Emerging.with_columns(
                            (pl.col("Free_Float") * pl.col("Capfactor")).alias("FOR_FF")
                            ).filter(pl.col("FOR_FF") >= FOR_FF_Screen)

        ###################################
        ###########Trading Days############
        ###################################

        Trading_Developed = Trading_Frequency_ALLCAP(temp_Developed, Trading_Days, date, Starting_Date, "Developed", Pivot_TOR, Trading_Days_OLD, Trading_Days_NEW, Screened_Securities)
        Trading_Emerging = Trading_Frequency_ALLCAP(temp_Emerging, Trading_Days, date, Starting_Date, "Emerging", Pivot_TOR, Trading_Days_OLD, Trading_Days_NEW, Screened_Securities)

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

        #################################
        #############ALL CAP#############
        #################################

        # Check if the MSCI_GMSR is between 99% and 99.25%
        if GMSR_Upper_Buffer[0] <= temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI[0]).head(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] <= GMSR_Lower_Buffer[0]:
            New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [GMSR_MSCI[0]],
                                        "GMSR_Developed_Upper": [GMSR_MSCI[0] * Upper_Limit],
                                        "GMSR_Developed_Lower": [GMSR_MSCI[0] * Lower_Limit], 
                                        "GMSR_Emerging": [GMSR_MSCI[0] / 2],
                                        "GMSR_Emerging_Upper": [GMSR_MSCI[0] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [GMSR_MSCI[0] / 2 * Lower_Limit],
                                        "Rank": [temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI[0]).head(1).select(pl.col("Rank")).to_numpy()[0][0]]
            })

        elif temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI[0]).head(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] > GMSR_Lower_Buffer[0]:
            New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[0]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                        "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[0]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                        "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[0]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                        "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[0]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                        "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[0]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[0]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                        "Rank": [
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[0])
                                                        .tail(1)["Full_MCAP_USD_Cutoff_Company"]
                                                        .to_numpy()[0]
                                                    )
                                                    .head(1)["Rank"]
                                                    .to_numpy()[0]
                                                ]

            })

        elif temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI[0]).head(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] < GMSR_Upper_Buffer[0]:
            New_Data = pl.DataFrame({
                                    "Date": [date],
                                    "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                    "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                    "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                    "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                    "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                    "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                    "Rank": [
                                                temp_Developed_Aggregate
                                                .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0])
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

        # Get the GMSR
        Lower_GMSR = GMSR_Frame.select(["GMSR_Emerging_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
        Upper_GMSR = GMSR_Frame.select(["GMSR_Emerging_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

        # Emerging AllCap #
        for country in temp_Emerging_Aggregate.select(pl.col("Country")).unique().sort("Country").to_series():
            
            TopPercentage, temp_Country = Index_Creation_Box_ALLCAP(temp_Emerging_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Percentage[0], Right_Limit[0], Left_Limit[0], "Emerging")

            # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
            TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country_ALLCAP(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging, LIF_Stored,
                                                                                       Starting_Date, Output_AllCap_Index, temp_Emerging, ETF, Pivot_TOR, AllCap_Index)

            # Stack to Output_Standard_Index
            Output_AllCap_Index = Output_AllCap_Index.vstack(TopPercentage.select(Output_AllCap_Index.columns))

            # Create the Output_Count_Standard_Index for future rebalacing
            Output_Count_AllCap_Index = Output_Count_AllCap_Index.vstack(TopPercentage.group_by("Country").agg(
                    pl.len().alias("Count"),
                    pl.col("Date").first().alias("Date")
                ).sort("Count", descending=True))

            #################################
            ###########Assign Size###########
            #################################

            # Standard Index #
            AllCap_Index = AllCap_Index.vstack(TopPercentage_Securities.select(AllCap_Index.columns))

        #################################
        ############STANDARD#############
        #################################

        # Recalculate the Rank
        temp_Developed_Aggregate = temp_Developed_Aggregate.with_columns(
                                                        (pl.col("Weight_Cutoff").cum_sum()).alias("CumWeight_Cutoff"),
                                                        (-pl.col("Full_MCAP_USD_Cutoff_Company")).rank("dense").alias("Rank")
        )

        # Check if the MSCI_GMSR is between 99% and 99.25%
        if GMSR_Upper_Buffer[1] <= temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI[1]).head(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] <= GMSR_Lower_Buffer[1]:
            New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [GMSR_MSCI[1]],
                                        "GMSR_Developed_Upper": [GMSR_MSCI[1] * Upper_Limit],
                                        "GMSR_Developed_Lower": [GMSR_MSCI[1] * Lower_Limit], 
                                        "GMSR_Emerging": [GMSR_MSCI[1] / 2],
                                        "GMSR_Emerging_Upper": [GMSR_MSCI[1] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [GMSR_MSCI[1] / 2 * Lower_Limit],
                                        "Rank": [temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI[1]).head(1).select(pl.col("Rank")).to_numpy()[0][0]]
            })

        elif temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI[1]).head(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] > GMSR_Lower_Buffer[1]:
            New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                        "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                        "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                        "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                        "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                        "Rank": [
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1])
                                                        .tail(1)["Full_MCAP_USD_Cutoff_Company"]
                                                        .to_numpy()[0]
                                                    )
                                                    .head(1)["Rank"]
                                                    .to_numpy()[0]
                                                ]

            })

        elif temp_Developed_Aggregate.filter(pl.col("Full_MCAP_USD_Cutoff_Company") <= GMSR_MSCI[1]).head(1).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0] < GMSR_Upper_Buffer[1]:
            New_Data = pl.DataFrame({
                                    "Date": [date],
                                    "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                    "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                    "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                    "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                    "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                    "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                    "Rank": [
                                                temp_Developed_Aggregate
                                                .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1])
                                                    .head(1)["Full_MCAP_USD_Cutoff_Company"]
                                                    .to_numpy()[0]
                                                )
                                                .head(1)["Rank"]
                                                .to_numpy()[0]
                                            ]
        })

        # Drop the Rank column
        temp_Developed_Aggregate = temp_Developed_Aggregate.drop("Rank")

        GMSR_Frame_STANDARD = GMSR_Frame_STANDARD.vstack(New_Data)

        # Get the GMSR
        Lower_GMSR = GMSR_Frame_STANDARD.select(["GMSR_Emerging_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
        Upper_GMSR = GMSR_Frame_STANDARD.select(["GMSR_Emerging_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

        # Emerging Standard #
        for country in temp_Emerging_Aggregate.select(pl.col("Country")).unique().sort("Country").to_series():
            
            TopPercentage, temp_Country = Index_Creation_Box_STANDARD(temp_Emerging_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Percentage[1], Right_Limit[1], Left_Limit[1], "Emerging")

            # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
            TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country_STANDARD(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging, LIF_Stored,
                                                                                       Starting_Date, Output_Standard_Index, temp_Emerging, ETF, Pivot_TOR, Standard_Index, AllCap_Index_Shadow, Original_MCAP_Developed)

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

        # Remove Shadow AllCap #
        Standard_Shadow = Standard_Index.filter(pl.col("Shadow_Company")==True)

        # Output ALlCap Frame
        Output_Frame_AllCap = pl.DataFrame(schema=AllCap_Index.schema)

        # Remove Shadow
        for date in AllCap_Index.select(pl.col("Date")).unique().to_series():
            temp_AllCap = AllCap_Index.filter(pl.col("Date")==date)
            temp_Standard = Standard_Shadow.filter(pl.col("Date")==date)

            temp_AllCap = temp_AllCap.join(temp_Standard.select(pl.col("Internal_Number", "Shadow_Company")), on="Internal_Number", how="left").with_columns(
                pl.col("Shadow_Company_right").fill_null(pl.col("Shadow_Company").alias("Shadow_Company_right"))
            ).drop("Shadow_Company").rename({"Shadow_Company_right": "Shadow_Company"})

            Output_Frame_AllCap = Output_Frame_AllCap.vstack(temp_AllCap)

        # Remove Shadow from main Frame
        AllCap_Index = Output_Frame_AllCap

        # Store AllCap_Index with Shadow
        AllCap_Index_Shadow = AllCap_Index

        # Remove All Shadow from main Frame
        AllCap_Index = AllCap_Index.filter(pl.col("Shadow_Company")==False)

    # Following Reviews where Index is rebalanced
    else:
            
            ###################################
            ################ADR################
            ###################################
            temp_Emerging = ADR_Removal(temp_Emerging, Emerging, Developed, "Emerging").drop("Occurrence_Count", "Index_Symbol_right")
            temp_Developed = ADR_Removal(temp_Developed, Emerging, Developed, "Developed").drop("Occurrence_Count", "Index_Symbol_right")

            ###################################
            #########FOR FF Screening##########
            ###################################

            # # Filter for FOR FF Screening
            temp_Developed = FOR_Screening_ALLCAP(Screened_Securities, temp_Developed, Developed, Pivot_TOR, AllCap_Index_Shadow, date, "Developed", Entity_ID, FOR_FF_Screen)
            temp_Emerging = FOR_Screening_ALLCAP(Screened_Securities, temp_Emerging, Emerging, Pivot_TOR, AllCap_Index_Shadow, date, "Emerging", Entity_ID, FOR_FF_Screen)

            # Calculate LIF for determining Shadow Company
            temp_Developed = temp_Developed.with_columns((pl.col("FIF") / pl.col("Free_Float")).alias("LIF")).drop("Free_Float_MCAP_USD_Cutoff")
            temp_Emerging = temp_Emerging.with_columns((pl.col("FIF") / pl.col("Free_Float")).alias("LIF")).drop("Free_Float_MCAP_USD_Cutoff")

            temp_LIF_Stored = temp_Developed.select(pl.col(["Date", "Internal_Number", "LIF"])).vstack(temp_Emerging.select(pl.col(["Date", "Internal_Number", "LIF"])))

            LIF_Stored = LIF_Stored.vstack(temp_LIF_Stored)

            # Recalculate Free_Float_MCAP_USD_Cutoff
            temp_Developed = temp_Developed.with_columns(
                (pl.col("Close_unadjusted_local_Cutoff") * pl.col("Shares_Cutoff") * pl.col("FIF") * pl.col("FX_local_to_Index_Currency_Cutoff")).alias("Free_Float_MCAP_USD_Cutoff"))
            
            temp_Emerging = temp_Emerging.with_columns(
                (pl.col("Close_unadjusted_local_Cutoff") * pl.col("Shares_Cutoff") * pl.col("FIF") * pl.col("FX_local_to_Index_Currency_Cutoff")).alias("Free_Float_MCAP_USD_Cutoff"))

            if (date < datetime.date(2023, 3, 20) and (date.month == 3 or date.month == 9)) or (date >= datetime.date(2023, 3, 20)):

                # Status
                print("Screens applied on " + str(date))

                ###################################
                ##########Apply EMS Screen#########
                ###################################

                # Apply the Screens
                temp_Developed, EMS_Frame, Temp_Screened_Securities, equity_universe_min_size = Equity_Minimum_Size_ALLCAP(temp_Developed, Pivot_TOR, EMS_Frame, date,
                                                        "Developed", Screened_Securities, temp_Exchanges_Securities, Entity_ID, Starting_Date, MSCI_Equity_Minimum_Size)

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
                temp_Emerging, EMS_Frame, Temp_Screened_Securities, equity_universe_min_size = Equity_Minimum_Size_ALLCAP(temp_Emerging, Pivot_TOR, EMS_Frame, date,
                                                        "Emerging", Screened_Securities, temp_Exchanges_Securities, Entity_ID, Starting_Date, MSCI_Equity_Minimum_Size)
                
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
                Developed_Screened_12M = Turnover_Check_12M_ALLCAP(temp_Developed, Pivot_TOR_12M, Threshold_NEW_12M, Threshold_OLD_12M, date, Starting_Date, "Developed", Turnover12M, Screened_Securities)
                Emerging_Screened_12M = Turnover_Check_12M_ALLCAP(temp_Emerging, Pivot_TOR_12M, Threshold_NEW_12M, Threshold_OLD_12M, date, Starting_Date, "Emerging", Turnover12M, Screened_Securities)

                # Remove Securities not passing the screen
                temp_Developed = temp_Developed.join(Developed_Screened_12M, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
                temp_Emerging = temp_Emerging.join(Emerging_Screened_12M, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
                
                ###################################
                ######TurnoverRatio Screening######
                ###################################

                # Apply the Check on Turnover for all Components
                Developed_Screened = Turnover_Check_ALLCAP(temp_Developed, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date, "Developed", Turnover, Screened_Securities)
                Emerging_Screened = Turnover_Check_ALLCAP(temp_Emerging, Pivot_TOR, Threshold_NEW, Threshold_OLD, date, Starting_Date, "Emerging", Turnover, Screened_Securities)

                # Remove Securities not passing the screen
                temp_Developed = temp_Developed.join(Developed_Screened, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)
                temp_Emerging = temp_Emerging.join(Emerging_Screened, on=["Internal_Number"], how="left").filter(pl.col("Status_TOR") == True)

                ###################################
                ###########Trading Days############
                ###################################

                Trading_Developed = Trading_Frequency_ALLCAP(temp_Developed, Trading_Days, date, Starting_Date, "Developed", Pivot_TOR, Trading_Days_OLD, Trading_Days_NEW, Screened_Securities)
                Trading_Emerging = Trading_Frequency_ALLCAP(temp_Emerging, Trading_Days, date, Starting_Date, "Emerging", Pivot_TOR, Trading_Days_OLD, Trading_Days_NEW, Screened_Securities)

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

            if (GMSR_Upper_Buffer[0]) <= CumWeight_Cutoff_Rank <= (GMSR_Lower_Buffer[0]):
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

            elif CumWeight_Cutoff_Rank < (GMSR_Upper_Buffer[0]):
                New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                        "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                        "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                        "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                        "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                        "Rank": [
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0])
                                                        .head(1)["Full_MCAP_USD_Cutoff_Company"]
                                                        .to_numpy()[0]
                                                    )
                                                    .head(1)["Rank"]
                                                    .to_numpy()[0]
                                                ]})
                
            elif CumWeight_Cutoff_Rank > (GMSR_Lower_Buffer[0]):
                New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                        "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                        "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                        "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                        "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                        "Rank": [
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[0])
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

            # Emerging #
            for country in temp_Emerging_Aggregate.select(pl.col("Country")).unique().sort("Country").to_series():

                # List of Unique Dates
                Dates_List = Pivot_TOR.index.to_list()

                IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
                Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

                Lower_GMSR = GMSR_Frame.select(["GMSR_Emerging_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
                Upper_GMSR = GMSR_Frame.select(["GMSR_Emerging_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

                # Check if there is already a previous Index creation for the current country
                if len(Output_Count_AllCap_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date))) > 0:
                    
                    # Full Review
                    if (date < datetime.date(2023, 3, 20) and (date.month == 3 or date.month == 9)) or (date >= datetime.date(2023, 3, 20)):

                        TopPercentage, temp_Country = Index_Rebalancing_Box_ALLCAP(temp_Emerging_Aggregate, SW_ACALLCAP, Output_Count_AllCap_Index, Lower_GMSR, Upper_GMSR, country, date, Right_Limit[0], Left_Limit[0], "Emerging",
                                                                                   Pivot_TOR, Output_AllCap_Index, AllCap_Index, Emerging, Securities_Cutoff, FX_Cutoff, GMSR_Frame, Percentage[0])

                        # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                        TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country_ALLCAP(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging, LIF_Stored,
                                                                                    Starting_Date, Output_AllCap_Index, temp_Emerging, ETF, Pivot_TOR, AllCap_Index)
                    
                    # Lite Review for Jun and December
                    else:

                        # Step 1 - Take previous date Index Composition (including Shadow Companies)
                        Previous_Composition = AllCap_Index_Shadow.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date))

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
                        TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country_ALLCAP(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging, LIF_Stored,
                                                                                                   Starting_Date, Output_AllCap_Index, temp_Emerging, ETF, Pivot_TOR, AllCap_Index)
                        
                    # Stack to Output_AllCap_Index
                    Output_AllCap_Index = Output_AllCap_Index.vstack(TopPercentage.select(Output_AllCap_Index.columns))
                    
                    # Create the Output_Count_AllCap_Index for future rebalacing
                    Output_Count_AllCap_Index = Output_Count_AllCap_Index.vstack(TopPercentage.group_by("Country").agg(
                        pl.len().alias("Count"),
                        pl.col("Date").first().alias("Date")
                    ).sort("Count", descending=True))
                
                # If there is no composition, a new Index will be created
                else:
                    TopPercentage, temp_Country = Index_Creation_Box_ALLCAP(temp_Emerging_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Percentage[0], Right_Limit[0], Left_Limit[0], "Emerging")
                    
                    # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                    TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country_ALLCAP(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging, LIF_Stored,
                                                                                                   Starting_Date, Output_AllCap_Index, temp_Emerging, ETF, Pivot_TOR, AllCap_Index)

                    # Stack to Output_AllCap_Index
                    Output_AllCap_Index = Output_AllCap_Index.vstack(TopPercentage.select(Output_AllCap_Index.columns))
                    
                    # Create the Output_Count_AllCap_Index for future rebalacing
                    Output_Count_AllCap_Index = Output_Count_AllCap_Index.vstack(TopPercentage.group_by("Country").agg(
                        pl.len().alias("Count"),
                        pl.col("Date").first().alias("Date")
                    ).sort("Count", descending=True))

                #################################
                ###########Assign Size###########
                #################################

                # Standard Index #
                AllCap_Index = AllCap_Index.vstack(TopPercentage_Securities.select(AllCap_Index.columns))

            #################################
            ############STANDARD#############
            #################################
            # List of Unique Dates
            Dates_List = Pivot_TOR.index.to_list()
            IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
            Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

            # Recalculate the Rank
            temp_Developed_Aggregate = temp_Developed_Aggregate.with_columns(
                                                            (pl.col("Weight_Cutoff").cum_sum()).alias("CumWeight_Cutoff"),
                                                            (-pl.col("Full_MCAP_USD_Cutoff_Company")).rank("dense").alias("Rank")
            )

            # Retrieve the Previous Rank in the GMSR Frame
            Previous_Rank_GMSR = GMSR_Frame_STANDARD.filter(pl.col("Date") == Previous_Date).select(pl.col("Rank")).to_numpy()[0][0]

            # CumWeight_Cutoff of the Previous Rank_GMSR
            CumWeight_Cutoff_Rank = temp_Developed_Aggregate.filter(pl.col("Rank") == Previous_Rank_GMSR).select(pl.col("CumWeight_Cutoff")).to_numpy()[0][0]

            if (GMSR_Upper_Buffer[1]) <= CumWeight_Cutoff_Rank <= (GMSR_Lower_Buffer[1]):
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
                
            elif CumWeight_Cutoff_Rank > (GMSR_Lower_Buffer[1]):
                New_Data = pl.DataFrame({
                                            "Date": [date],
                                            "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                            "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                            "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                            "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                            "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                            "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1]).tail(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                            "Rank": [
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                            temp_Developed_Aggregate
                                                            .filter(pl.col("CumWeight_Cutoff") < GMSR_Lower_Buffer[1])
                                                            .tail(1)["Full_MCAP_USD_Cutoff_Company"]
                                                            .to_numpy()[0]
                                                        )
                                                        .head(1)["Rank"]
                                                        .to_numpy()[0]
                                                    ]

                })

            elif CumWeight_Cutoff_Rank < (GMSR_Upper_Buffer):
                New_Data = pl.DataFrame({
                                        "Date": [date],
                                        "GMSR_Developed": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0]],
                                        "GMSR_Developed_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Upper_Limit],
                                        "GMSR_Developed_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] * Lower_Limit], 
                                        "GMSR_Emerging": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2],
                                        "GMSR_Emerging_Upper": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Upper_Limit],
                                        "GMSR_Emerging_Lower": [temp_Developed_Aggregate.filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1]).head(1)["Full_MCAP_USD_Cutoff_Company"].to_numpy()[0] / 2 * Lower_Limit],
                                        "Rank": [
                                                    temp_Developed_Aggregate
                                                    .filter(pl.col("Full_MCAP_USD_Cutoff_Company") == 
                                                        temp_Developed_Aggregate
                                                        .filter(pl.col("CumWeight_Cutoff") > GMSR_Upper_Buffer[1])
                                                        .head(1)["Full_MCAP_USD_Cutoff_Company"]
                                                        .to_numpy()[0]
                                                    )
                                                    .head(1)["Rank"]
                                                    .to_numpy()[0]
                                                ]})
                    
            # Drop the Rank column
            temp_Developed_Aggregate = temp_Developed_Aggregate.drop("Rank")

            GMSR_Frame_STANDARD = GMSR_Frame_STANDARD.vstack(New_Data)

            # Emerging #
            for country in temp_Emerging_Aggregate.select(pl.col("Country")).unique().sort("Country").to_series():

                # List of Unique Dates
                Dates_List = Pivot_TOR.index.to_list()

                IDX_Current = Dates_List.index(date.strftime("%Y-%m-%d"))
                Previous_Date = datetime.datetime.strptime(Dates_List[max(0, IDX_Current - 1)], "%Y-%m-%d").date()

                Lower_GMSR = GMSR_Frame_STANDARD.select(["GMSR_Emerging_Lower", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]
                Upper_GMSR = GMSR_Frame_STANDARD.select(["GMSR_Emerging_Upper", "Date"]).filter(pl.col("Date") == date).to_numpy()[0][0]

                # Check if there is already a previous Index creation for the current country
                if len(Output_Count_Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date))) > 0:
                    
                    # Full Review
                    if (date < datetime.date(2023, 3, 20) and (date.month == 3 or date.month == 9)) or (date >= datetime.date(2023, 3, 20)):

                        TopPercentage, temp_Country = Index_Rebalancing_Box_STANDARD(temp_Emerging_Aggregate, SW_ACALLCAP, Output_Count_Standard_Index, Lower_GMSR, Upper_GMSR, country, date, 
                                                    Right_Limit[1], Left_Limit[1], "Emerging", Pivot_TOR, Output_Standard_Index, Emerging, AllCap_Index_Shadow, Securities_Cutoff, FX_Cutoff,
                                                    GMSR_Frame_STANDARD)

                        # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                        TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country_STANDARD(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging, LIF_Stored,
                                Starting_Date, Output_Standard_Index, temp_Emerging, ETF, Pivot_TOR, Standard_Index, AllCap_Index_Shadow, Original_MCAP_Developed)
                        
                    # Lite Review for Jun and December
                    else:

                        # Step 1 - Take previous date Index Composition (including Shadow Companies) from the ALLCAP Index (Security Level)
                        Previous_Composition = AllCap_Index_Shadow.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).drop("Shadow_Company")

                        # Count the number of Chairs from the Previous Standard Index (Company Level)
                        PreviousChairsNumber = Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(pl.col("ENTITY_QID")).unique().height

                        # Add a column for current Standard Securities and for Previously Shadow (from Standard Index not All Cap one)
                        Previous_Composition = Previous_Composition.with_columns(
                            pl.col("Internal_Number").is_in(Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(pl.col("Internal_Number"))).
                            alias("Previous_STANDARD")
                        ).join(Standard_Index.filter((pl.col("Country") == country) & (pl.col("Date") == Previous_Date)).select(["Internal_Number", "Shadow_Company"]),
                               on=["Internal_Number"], how="left")
                        
                        # Fill NULL Shadow_Company into TRUE
                        Previous_Composition = Previous_Composition.with_columns(
                            pl.col("Shadow_Company").fill_null(True)
                        )

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
                        
                        # Add FreeFloat and CapFactor as of Previous Review/Cutoff Date
                        Previous_Composition = Previous_Composition.join(
                            Emerging.filter(pl.col("Date") == Previous_Date).select(pl.col(["Internal_Number", "Free_Float", "Capfactor"])),
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

                        ######################################################################
                        # Update all components for Free/Full MCAP as of current Review Date #
                        ######################################################################
                        Previous_Composition = Previous_Composition.select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", "Previous_STANDARD"]))

                        # Add back the information needed
                        Previous_Composition = Previous_Composition.join(
                            Securities_Cutoff.select(pl.col(["Review", "stoxxId", "closePrice", "validDate", "currency", "shares"])),
                            left_on=["Date", "Internal_Number"],
                            right_on=["Review", "stoxxId"],
                            how="left"
                            ).rename({
                            "validDate": "Cutoff",
                            "currency": "Currency",
                            "closePrice": "Close_unadjusted_local_Cutoff",
                            "shares": "Number_of_Shares"
                            }).join(FX_Cutoff, on=["Cutoff", "Currency"], how="left")
                        
                        # Free Float and CapFactor
                        Previous_Composition = Previous_Composition.join(
                            Emerging.filter(pl.col("Date") == date).select(pl.col(["Internal_Number", "Free_Float", "Capfactor"])),
                            on=["Internal_Number"], how="left"
                        )

                        # Recalculate Free/Full MCAP with all updated components
                        Previous_Composition = Previous_Composition.with_columns(
                            (pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Close_unadjusted_local_Cutoff") * pl.col("Number_of_Shares")).
                            alias("Full_MCAP_USD_Cutoff"),
                            (pl.col("FX_local_to_Index_Currency_Cutoff") * pl.col("Close_unadjusted_local_Cutoff") * pl.col("Number_of_Shares") *
                             pl.col("Free_Float") * pl.col("Capfactor")).alias("Free_Float_MCAP_USD_Cutoff")
                            )

                        # Drop everything where Full_MCAP is NULL or 0
                        Previous_Composition = Previous_Composition.filter(
                            (pl.col("Full_MCAP_USD_Cutoff") > 0) & (pl.col("Free_Float_MCAP_USD_Cutoff") > 0)
                        )

                        ######################################################################
                        ######################################################################
                        ######################################################################

                        # Aggregate Securities by ENTITIY_QID
                        TopPercentage = Previous_Composition.select(pl.col(["Date", "Internal_Number", "Instrument_Name", "ENTITY_QID", "Country", 
                                                                            "Previous_STANDARD", "Free_Float_MCAP_USD_Cutoff", "Full_MCAP_USD_Cutoff"])).group_by(
                                                    ["Date", "ENTITY_QID"]).agg([
                                                        pl.col("Country").first().alias("Country"),
                                                        pl.col("Internal_Number").first().alias("Internal_Number"),
                                                        pl.col("Instrument_Name").first().alias("Instrument_Name"),
                                                        pl.col("Previous_STANDARD").first().alias("Previous_STANDARD"),
                                                        pl.col("Free_Float_MCAP_USD_Cutoff").sum().alias("Free_Float_MCAP_USD_Cutoff_Company"),
                                                        pl.col("Full_MCAP_USD_Cutoff").sum().alias("Full_MCAP_USD_Cutoff_Company")
                                                    ]).sort("Full_MCAP_USD_Cutoff_Company", descending = True)

                        # Adjust the columns and Calculate Weight and CumWeight
                        TopPercentage = TopPercentage.with_columns(
                                                    (pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).alias("Weight_Cutoff"),
                                                    (((pl.col("Free_Float_MCAP_USD_Cutoff_Company") / pl.col("Free_Float_MCAP_USD_Cutoff_Company").sum()).cum_sum())).alias("CumWeight_Cutoff")
                                                    ).sort("Full_MCAP_USD_Cutoff_Company", descending=True)
                        
                        # Update the PreviousChairsNumber
                        PreviousChairsNumber = TopPercentage.filter(pl.col("Previous_STANDARD") == True).height

                        # Create temp_Country for next function
                        temp_Country = TopPercentage.select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number", "Instrument_Name",
                            "Free_Float_MCAP_USD_Cutoff_Company", "Full_MCAP_USD_Cutoff_Company", "Weight_Cutoff", "CumWeight_Cutoff"]))
                        
                        # Add Size Column
                        TopPercentage = TopPercentage.with_columns(pl.lit("Standard").alias("Size"), pl.lit("Lite_Review").alias("Case"))

                        # Seize TopPercentage to keep only the Xth rows (where X is the number of PreviousChairsNumber)
                        TopPercentage = TopPercentage.head(PreviousChairsNumber).select(pl.col(["Date", "ENTITY_QID", "Country", "Internal_Number",
                            "Instrument_Name", "Free_Float_MCAP_USD_Cutoff_Company", "Full_MCAP_USD_Cutoff_Company", "Weight_Cutoff", "CumWeight_Cutoff", "Size", "Case"]))
                        
                        # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                        TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country_STANDARD(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging, LIF_Stored,
                                Starting_Date, Output_Standard_Index, temp_Emerging, ETF, Pivot_TOR, Standard_Index, AllCap_Index_Shadow, Original_MCAP_Developed)

                    # Stack to Output_Standard_Index
                    Output_Standard_Index = Output_Standard_Index.vstack(TopPercentage.select(Output_Standard_Index.columns))
                    
                    # Create the Output_Count_Standard_Index for future rebalacing
                    Output_Count_Standard_Index = Output_Count_Standard_Index.vstack(TopPercentage.group_by("Country").agg(
                        pl.len().alias("Count"),
                        pl.col("Date").first().alias("Date")
                    ).sort("Count", descending=True))
                
                # If there is no composition, a new Index will be created
                else:
                    TopPercentage, temp_Country = Index_Creation_Box_STANDARD(temp_Emerging_Aggregate, Lower_GMSR, Upper_GMSR, country, date, Percentage[1], Right_Limit[1], Left_Limit[1], "Emerging")

                    # Apply the check on Minimum_FreeFloat_MCAP_USD_Cutoff
                    TopPercentage, TopPercentage_Securities = Minimum_FreeFloat_Country_STANDARD(TopPercentage, temp_Country, Lower_GMSR, Upper_GMSR, date, country, "Emerging", Original_MCAP_Emerging, LIF_Stored,
                                Starting_Date, Output_Standard_Index, temp_Emerging, ETF, Pivot_TOR, Standard_Index, AllCap_Index_Shadow, Original_MCAP_Developed)

                    # Stack to Output_Standard_Index
                    Output_Standard_Index = Output_Standard_Index.vstack(TopPercentage.select(Output_Standard_Index.columns))
                    
                    # Create the Output_Count_Standard_Index for future rebalacing
                    Output_Count_Standard_Index = Output_Count_Standard_Index.vstack(TopPercentage.group_by("Country").agg(
                        pl.len().alias("Count"),
                        pl.col("Date").first().alias("Date")
                    ).sort("Count", descending=True))

                # Standard Index #
                Standard_Index = Standard_Index.vstack(TopPercentage_Securities.select(Standard_Index.columns))

            # Remove Shadow AllCap #
            Standard_Shadow = Standard_Index.filter(pl.col("Shadow_Company")==True)

            # Output ALlCap Frame
            Output_Frame_AllCap = pl.DataFrame(schema=AllCap_Index.schema)

            # Remove Shadow
            for date in AllCap_Index.select(pl.col("Date")).unique().to_series():
                temp_AllCap = AllCap_Index.filter(pl.col("Date")==date)
                temp_Standard = Standard_Shadow.filter(pl.col("Date")==date)

                temp_AllCap = temp_AllCap.join(temp_Standard.select(pl.col("Internal_Number", "Shadow_Company")), on="Internal_Number", how="left").with_columns(
                    pl.col("Shadow_Company_right").fill_null(pl.col("Shadow_Company").alias("Shadow_Company_right"))
                ).drop("Shadow_Company").rename({"Shadow_Company_right": "Shadow_Company"})

                Output_Frame_AllCap = Output_Frame_AllCap.vstack(temp_AllCap)

            # Remove Standard Shadow from main AllCap Frame
            AllCap_Index = Output_Frame_AllCap

            # Store AllCap_Index with Shadow
            AllCap_Index_Shadow = AllCap_Index

            # Remove All Shadow from main Frame
            AllCap_Index = AllCap_Index.filter(pl.col("Shadow_Company")==False)


