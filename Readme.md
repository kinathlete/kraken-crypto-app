# KRAKEN CRYPTO DATA CONNECTOR

This connector queries the public Kraken API (https://docs.kraken.com/rest).
Specifically it queries the 'Trades' endpoint to get the latest trades.
It then inserts the data into a Snowflake database using Snowflake Connector.