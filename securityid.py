import pandas as pd
import requests
import time
import urllib3
from openpyxl import load_workbook

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_file(url, output_path):
    try:
        response = requests.get(url, stream=True, verify=False)  # Disable SSL verification
        response.raise_for_status()  # Check for HTTP errors

        with open(output_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    file.write(chunk)

        print(f"\nFile downloaded successfully and saved as {output_path}")
    except requests.exceptions.RequestException as e:
        print(f"\nError downloading file: {e}")
        
    time.sleep(1)
#Example usage
url = 'https://images.dhan.co/api-data/api-scrip-master.csv'
output_path = 'api-scrip-master.csv'
download_file(url, output_path)

# Load the Excel file
orderdata_file = 'orderdata.xlsx'
api_scrip_master_file = 'api-scrip-master.csv'

# Ensure the column names are as expected
securityid_column_symbol = 'symbol'
securityid_column_secid = 'secid'
api_column_symbol = 'SEM_CUSTOM_SYMBOL'
api_column_id = 'SEM_SMST_SECURITY_ID'

# Load the data from the 'securityid' sheet into a DataFrame
securityid_df = pd.read_excel(orderdata_file, sheet_name='securityid')

# Load the data from the CSV file into another DataFrame
api_scrip_master_df = pd.read_csv(api_scrip_master_file, low_memory=False)

# Create a dictionary for mapping from SEM_CUSTOM_SYMBOL to SEM_SMST_SECURITY_ID
mapping = pd.Series(api_scrip_master_df[api_column_id].values, index=api_scrip_master_df[api_column_symbol]).to_dict()


# Map the values from the securityid DataFrame
securityid_df[securityid_column_secid] = securityid_df[securityid_column_symbol].map(mapping)


# Write the updated securityid_df back to the 'securityid' sheet in the Excel file
with pd.ExcelWriter(orderdata_file, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
    securityid_df.to_excel(writer, sheet_name='securityid', index=False, columns=['secid'])

print("\nData from api-scrip-master.csv copied to the 'securityid' sheet successfully.")

# Print the updated securityid DataFrame
print(securityid_df[[securityid_column_symbol, securityid_column_secid]])