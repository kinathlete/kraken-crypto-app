# from dotenv import load_dotenv
# load_dotenv()
import os
import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import snowflake.connector
import datetime
import pytz

# MAIN FUNCTION: QUERIES API.KRAKEN.COM/0/PUBLIC TO
# RETRIEVE RECENT TRADES DATA AND PASTE INTO SNOWFLAKE.

def main(event, context):

    print("Starting the update run.")

    # Getting the env variables
    user = os.environ.get('SNOWFLAKE-USER')
    account = os.environ.get('SNOWFLAKE-ACCOUNT')
    password = os.environ.get('SNOWFLAKE-PW')

    # other pairs: pairs = ['ETHCHF','XETHZEUR', 'ADAEUR', 'DOTEUR', 'SOLEUR']
    pairs = ['XXBTZEUR','XETHZEUR']
    responses = []

    # Get data from Kraken
    try:
        for p in pairs:
            response = requests.get(f'https://api.kraken.com/0/public/Trades?pair={p}')
            result = {
                "pair": p,
                "response": response.text
            }
            responses.append(result)
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
        # Preparing the insert statement for Snowflake
        insert_statement = ""
        for r, val in enumerate(responses):
            if (r+1) == len(responses):
                insert_values = "('" + datetime.datetime.now(pytz.timezone('Europe/Zurich')).strftime("%x %X") + "', '" + str(val['response']) + "')"
            else:
                insert_values = "('" + datetime.datetime.now(pytz.timezone('Europe/Zurich')).strftime("%x %X") + "', '" + str(val['response']) + "'),"
            insert_statement = insert_statement + insert_values
        # Execute insert statement
        con.cursor().execute(
            "INSERT INTO KRAKEN_TEST(TIMESTAMP, RESPONSE) VALUES "
            + insert_statement)

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