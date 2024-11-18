import pandas as pd
import time
from dhanhq import dhanhq
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
import ssl

# Define a custom adapter to bypass SSL verification
class SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        kwargs['ssl_context'] = context
        return super(SSLAdapter, self).init_poolmanager(*args, **kwargs)

# Load the Excel file
orderdata_file = 'orderdata.xlsx'

# Initialize the DhanHQ client
config_df = pd.read_excel(orderdata_file, sheet_name='config')
client_id = config_df.loc[config_df['parameter'] == 'client_id', 'value'].values[0]
access_token = config_df.loc[config_df['parameter'] == 'access_token', 'value'].values[0]

dhan = dhanhq(client_id, access_token)

# Create a custom requests session
session = requests.Session()
# Mount the custom SSLAdapter to handle 'https://'
session.mount('https://', SSLAdapter())

# Patch the session used by the DhanHQ client
dhan.session = session

# Load the data from the Excel file into a DataFrame
orderdata_df = pd.read_excel(orderdata_file, sheet_name='orderdata')

# Ensure the DataFrame has columns for orderId and orderStatus
#if 'orderId' not in orderdata_df.columns:
#    orderdata_df['orderId'] = None
#if 'orderStatus' not in orderdata_df.columns:
#    orderdata_df['orderStatus'] = None

# Convert the orderId column to str type to handle any type compatibility issues
#orderdata_df['orderId'] = orderdata_df['orderId'].astype(str)
#orderdata_df['orderStatus'] = orderdata_df['orderStatus'].astype(str)

# Define column names (ensure they match your actual column names in the Excel sheet)
orderdata_column_transaction_type = 'transaction_type'  # Adjust based on the actual column name
orderdata_column_secid = 'secid'  # Adjust based on the actual column name
orderdata_column_quantity = 'quantity'  # Adjust based on the actual column name


def place_individual_order(index, row, orderdata_df, orderdata_column_transaction_type, orderdata_column_secid, orderdata_column_quantity):
    # Extract order data from the row
    transaction_type = row[orderdata_column_transaction_type]
    security_id = row[orderdata_column_secid]
    quantity = row[orderdata_column_quantity]
    
    # Define the order data
    order_data = {
        'transaction_type': transaction_type,
        'exchange_segment': dhan.NSE_FNO,
        'product_type': dhan.MARGIN,
        'order_type': dhan.MARKET,
        'validity': dhan.DAY,
        'security_id': security_id,
        'quantity': quantity,
        'price': 0,
    }

    # Place the order
    try:
        response = dhan.place_order(
            exchange_segment=order_data["exchange_segment"],
            transaction_type=order_data["transaction_type"],
            quantity=order_data["quantity"],
            order_type=order_data["order_type"],
            product_type=order_data["product_type"],
            price=order_data["price"],
            validity=order_data["validity"],
            security_id=order_data["security_id"]
        )
        if response['status'] == 'success':
            order_id = response['data']['orderId']
            order_status = response['data']['orderStatus']
            print(f"Order placed successfully: Order ID - {order_id}, Order Status - {order_status}")
            # Update the DataFrame with the order ID and status
            #orderdata_df.at[index, 'orderId'] = str(order_id)
            #orderdata_df.at[index, 'orderStatus'] = str(order_status)
                
            # Wait until orderStatus becomes 'TRADED'
            while order_status != 'TRADED':
                time.sleep(1)  # Wait for 1 second before checking the status again
                order_status_response = dhan.get_order_by_id(order_id)  # Replace with actual method to get order status
                order_status = order_status_response['data']['orderStatus']
                print(f"\nChecking order status: Order ID - {order_id}, Order Status - {order_status}")
                # Update the DataFrame with the current order status
                #orderdata_df.at[index, 'orderStatus'] = order_status
        
        else:
            print(f"\nFailed to place order: {response}")
            # Update the DataFrame with the failure status
            #orderdata_df.at[index, 'orderStatus'] = response.get('message', 'Failed')
    except Exception as e:
        print(f"Error placing order: {e}")
        # Update the DataFrame with the error message
        #orderdata_df.at[index, 'orderStatus'] = str(e)

    # Wait for 1 second before placing the next order
    time.sleep(1)
    
    
# Iterate over each row in the DataFrame
for index, row in orderdata_df[orderdata_df[orderdata_column_transaction_type] == 'BUY'].iterrows():
    place_individual_order(index, row, orderdata_df, orderdata_column_transaction_type, orderdata_column_secid, orderdata_column_quantity)

    # Iterate over each row in the DataFrame and place SELL orders after BUY orders
for index, row in orderdata_df[orderdata_df[orderdata_column_transaction_type] == 'SELL'].iterrows():
    place_individual_order(index, row, orderdata_df, orderdata_column_transaction_type, orderdata_column_secid, orderdata_column_quantity)

print("\nOrders placed successfully.")