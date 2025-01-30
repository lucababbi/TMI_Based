import pandas as pd
import requests
from io import StringIO
from datetime import datetime

Full_Dates = pd.read_csv(r"C:\Users\et246\Desktop\V0_SAMCO\Dates\Review_Date-QUARTERLY.csv", parse_dates=True).tail(51)

# Base URL
base_url = "https://codfix2.bat.ci.dom/stoxxcalcservice/api/CalcValue/GetCalcValues"

# Parameters
params = {
    "vd": "20230228",  # Initial value of 'vd'
    "tokenListCSV": "TURNOVER_12M"
}

# Change 'vd' dynamically
params["vd"] = "20230131"  # Example: Updating to a new date

Output = pd.DataFrame()

for cutoff in Full_Dates["Cutoff"]:

    vd = datetime.strptime(cutoff, "%m/%d/%Y").strftime("%Y%m%d")
    token_list_csv = "TURNOVER_12M"
    full_url = f"{base_url}?vd={vd}&tokenListCSV={token_list_csv}"
    response = requests.get(full_url, verify=False)


    # Check if the request was successful
    if response.status_code == 200:
        # Parse the CSV response into a Pandas DataFrame
        csv_data = StringIO(response.text)  # Convert the response content to a file-like object
        df = pd.read_csv(csv_data)

        # Print or process the DataFrame
        print(df)

        Output = pd.concat([Output, df])
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(f"Response text: {response.text}")

Output.head()