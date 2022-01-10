from dotenv import load_dotenv
load_dotenv()
import os
import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import snowflake.connector

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

def save_to_tmp_file(json_data):
    # Variables for tmp file
    filename = '/recent_trades.json'
    filepath = '/tmp' + filename
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

def load_into_snowflake(filepath):
    if filepath == "Saving Error":
        return "Saving Error"
    else:
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
            # Inserting json response into stage.
            con.cursor().execute(
                """PUT file://""" + filepath + " @RECENT_TRADES_TEST")
        except snowflake.connector.errors.ProgrammingError as e:
            # Default error message
            print(e)
            # Customer error message
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        finally:
            # Closing the connection
            con.close()
            # Remove tmp file
            os.remove(filepath)

# def main(event, context):
def main():
    print("Starting the update run.")
    for p in pairs:
        print(f'Creating url for {p}.')
        url = create_url(p)
        print(f'Getting data for {p}.')
        json_data = connect_to_endpoint(url)
        print('Writing to tmp file.')
        filepath = save_to_tmp_file(json_data)
        # with open('/tmp/recent_trades.json') as f:
        #     contents = f.read()
        #     print(contents)
    print('Load data into Snowflake stage.')
    if load_into_snowflake(filepath) == "Saving Error":
        print("Error with tmp file.")
    else:
        print("Data updated.")
    
if __name__ == '__main__':
    main()