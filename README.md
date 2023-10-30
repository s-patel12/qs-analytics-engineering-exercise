# Analytics-Engineering Test

### Requirements

* Python 3.6+
* Libraries: pandas, requests, sqlalchemy, time

### Client

The client is the owner of a high-end chain of bars. The company's data infrastructure is old and many of the pipelines are slow or built using niche software. The owner wants to future proof the pipelines by converting them to Python.

We want to give the client confidence that we have the technical skills and the experience to refactor their entire data setup. To that end, we are building a PoC pipeline for the client to show off our abilities. They have chosen their transaction data as a key pipeline and have provided a sample dataset for their transactions and inventory, as well as an API endpoint for their menu. Your task is to build that PoC pipeline.

### Resources

- Transactions data for 3 bars in the form of CSVs.
- Glass stock data for those three bars as a CSV.
- They use the online cocktails database (`https://www.thecocktaildb.com/api.php`) for their menu and all bars serve a subset of these drinks according to the instructions in the database.

### Task

- You will provide production level Python code to read the data and process it, ready to be inserted into a Postgres RDS database.
- We want tables that will store data on the bars, transactions, glasses and which glasses are used for which cocktail. We do not need additional data on the cocktails themselves such as the ingredients.
- You will design the schema for the database and use sqlalchemy to define the necessary tables.

Considerations:
- The tables will be used for many downstream purposes, especially an array of dashboards.
- There are plans to change the source of the transactions data from CSV to API.
- The code may need to be run frequently so that the data in the database is fresh.
- The bar company owns many more than these three bars so the volume of data will increase.
- This pipeline will be run on a production server with many other important pipelines scheduled in.

### Project Structure
```bash
data/
|- bar_data.csv                    # glass stock data for each bar
|- budapest.csv.gz                 # transaction data for the Budapest bar
|- london_transactions.csv.gz      # transaction data for the London bar
|- ny.csv.gz                       # transaction data for the New York bar

bars_database.db                   # database to save clean data

process_data.py                    # ETL pipeline script

improvements.md                    # future improvements to be made to the pipeline

README.md                          # project description
```

### Instructions
To run ETL pipeline that loads, cleans, and stores the data in a SQL database:
```python
    python process_data.py
```
