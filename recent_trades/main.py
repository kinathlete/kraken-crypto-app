from dotenv import load_dotenv
load_dotenv()
import os
import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import snowflake.connector
import datetime
import pytz

# MAIN FUNCTION: QUERIES API.KRAKEN.COM/0/PUBLIC TO
# RETRIEVE RECENT TRADES DATA AND PASTE INTO SNOWFLAKE.

# def main(event, context):
def main():

    print("Starting the update run.")

    # Getting the env variables
    user = os.environ.get('SNOWFLAKE-USER')
    account = os.environ.get('SNOWFLAKE-ACCOUNT')
    password = os.environ.get('SNOWFLAKE-PW')
    print(user, account, password)

    # pairs = ['ETHCHF','XETHZEUR', 'ADAEUR', 'DOTEUR', 'SOLEUR']
    pairs = ['XXBTZEUR']

    # Get data from Kraken
    try:
        for p in pairs:
            response = requests.get(f'https://api.kraken.com/0/public/Trades?pair={p}')
            json_data = json.loads(response.text)
            print(json_data['error'])     
    # Catch error if any
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)

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
        con.cursor().execute(
            "INSERT INTO STAGE_KRAKEN_RECENT_TRENDS(TIMESTAMP, RESPONSE) VALUES " +
            "('" + datetime.datetime.now(pytz.timezone('Europe/Zurich')).strftime("%x %X") + "', '"
            + response.text + "')"
        )
    except snowflake.connector.errors.ProgrammingError as e:
        # Default error message
        print(e)
        # Customer error message
        print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
    finally:
        # Closing the connection
        con.close()

    print("Data updated.")

if __name__ == '__main__':
    main()