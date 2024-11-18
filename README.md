# Buy and Sell order placements using Python and Excel

This set of code along with the excel sheet can be used to place any type of future/option trading strategies that might involve multiple buy and sell legs such as IronFly, Batman etc.,.

This repo consists of 3 python code with the below functions :

- **securityid.py** To fetch security id from the broker
- **placeorder.py** To place buy and sell orders
- **exitorder.py** To monitor the open positions and place an exit order when the target is hit

## Features

**securityid.py**

- Downloads a CSV file from a given URL (from broker).
- Reads data from the downloaded CSV and an Excel file.
- Maps data from the CSV file to the Excel file based on specified columns.
- Updates the Excel file with the mapped data in an existing sheet.

**placeorder.py**

- Implements a custom SSL adapter to bypass SSL verification.
- Reads configuration and order data from an Excel file (`orderdata.xlsx`).
- Utilizes the Broker API to place buy and sell orders.
- Monitors and updates the status of each order until it is traded.
- Sequentially processes buy orders followed by sell orders with appropriate delays.

**exitorder.py**

- Periodically fetches positions and calculates total unrealized profit or loss.
- Automatically places exit orders when the total profit/loss reaches a predefined target.
- Places buy orders followed by sell orders, with delays to prevent API throttling and Margin issues.

## Prerequisites

Ensure you have the following installed on your system:

- **Python 3.7 or higher**
- All pre-requisites such as access token, client id, date and month of contract expiry are set in the orderdata.xlsx file which need to be filled before execution

### Python Libraries

The script relies on the following Python libraries:

- `pandas`
- `requests`
- `urllib3`
- `openpyxl`
- `danhhq` *(Since I used Dhan to place orders)*

You can install these dependencies using pip:

```bash
pip install pandas requests openpyxl urllib3 danhhq
```

## Usage

1. **Clone the repository** and navigate to the playbook directory.

    ```bash
    git clone https://github.com/moorthyoncloud/python_order-placement-and-pnl-monitoring.git
    cd python_order-placement-and-pnl-monitoring
    ```

2. **Specify details such as client id, access token, contract expiry, the buy and sell orders that needs to be placed in the Orderdata.xlsx after your analysis.
   **Fetch the security id of all the strikes specified. This updates the excel sheet with the required security id to place the orders.

    ```bash
    python securityid.py
    ```
	
3. **Open the Excel sheet, verify and save the results

4. **Place the orders using the placeorder.py

	```bash
    python placeorder.py
    ```

5. **Monitor and close the positions using exitorder.py

	```bash
    python exitorder.py
    ```

## Notes

- A known bug, where the excel sheet need to be opened and saved after fetching the security ids. Else, the placeorder scipt will throw a NaN error.

## License
This project is opensource and licensed under the MIT License.

## Author
- [Shenbagamoorthy](https://github.com/moorthyoncloud)
