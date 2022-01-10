# from dotenv import load_dotenv
# load_dotenv()
import os
import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import snowflake.connector
import datetime

# MAIN FUNCTION: QUERIES API.KRAKEN.COM/0/PUBLIC TO
# RETRIEVE RECENT TRADES DATA AND PASTE INTO SNOWFLAKE.

# Getting the env variables
user = os.environ.get('SNOWFLAKE-USER')
account = os.environ.get('SNOWFLAKE-ACCOUNT')
password = os.environ.get('SNOWFLAKE-PW')
# other pairs: pairs = ['ETHCHF','XETHZEUR', 'ADAEUR', 'DOTEUR', 'SOLEUR']
pairs = ['XXBTZEUR','XETHZEUR']

def create_url(pair):
    return f'https://api.kraken.com/0/public/Trades?pair={pair}'

def connect_to_endpoint(url):
    try:
        response = requests.get(url)
        json_data = response.json()
        print(json_data['error'])
    # Catch error if any
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
    return json_data

def save_to_tmp_file(json_data, pair):
    # Variables for tmp file
    filename = '/recent_trades'
    file_type = '.json'
    timestamp = str(int(round(datetime.datetime.now().timestamp())))
    filepath = '/tmp' + filename + '_' + pair + '_' +timestamp + file_type
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    str_json = str(json_data)
    # Writing to tmp file
    with open(filepath, 'a') as f:
        f.write(str_json)
        f.close()
    if os.path.exists(filepath):
        return filepath
    else:
        return "Saving Error"

def load_into_snowflake(filepaths):

    # Connecting to Snowflake
    con = snowflake.connector.connect(
        user=user,
        account=account,
        password=password
    )    
    try:
        # Running queries
        con.cursor().execute("USE WAREHOUSE SNOWFLAKE_WH")
        con.cursor().execute("USE DATABASE DAGOBERT_DB")
        con.cursor().execute("USE SCHEMA DAGOBERT_DB.KRAKEN_SCHEMA")
        for fp in filepaths:
            # Inserting json response into stage.
            con.cursor().execute(
                """PUT file://""" + fp + " @RECENT_TRADES")
        # Copying into raw table.
        con.cursor().execute("COPY INTO DEV_TRADES_RAW" +
        " FROM @RECENT_TRADES FILE_FORMAT = (FORMAT_NAME = 'JSON_FILE_FORMAT')")
    except snowflake.connector.errors.ProgrammingError as e:
        # Default error message
        print(e)
        # Customer error message
        print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
    finally:
        # Closing the connection
        con.close()
        # Remove tmp file
        for fp in filepaths:
            os.remove(fp)

def main(event, context):
# def main():
    print("Starting the update run.")
    filepaths = []
    for p in pairs:
        url = create_url(p)
        print(f'Getting data for {p}.')
        json_data = connect_to_endpoint(url)
        print('Writing to tmp file.')
        filepath = save_to_tmp_file(json_data, p)
        if filepath == "Saving Error":
            return print("Error with tmp file.")
        else: 
            filepaths.append(filepath)
            # with open('/tmp/recent_trades.json') as f:
            #     contents = f.read()
            #     print(contents)
    print('Loading data into Snowflake.')
    load_into_snowflake(filepaths)
    print("Data updated.")
    
if __name__ == '__main__':
    main()