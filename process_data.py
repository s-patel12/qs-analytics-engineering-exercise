# import libraries
import pandas as pd
import requests
import time
from sqlalchemy import create_engine
from sqlalchemy.sql import text


def create_sql_tables(conn):
    
    with open("create_tables.sql", 'r') as f:
        sql_statements = f.read()

    statements = sql_statements.split(';')

    try:
        # Execute each SQL statement to create tables in schema if they don't already exist
        for statement in statements:
            if statement.strip():
                conn.execute(text(statement))
                conn.commit()
        print("SQL statment executed successfully!")
    except Exception as e:
        print("Error: ", e)
        conn.rollback()


def load_glass_stock_data():

    # Importing glass stock data
    df = pd.read_csv(open("data\\bar_data.csv"), sep=',')

    # Data cleaning
    df['stock'] = df['stock'].str.extract('(\d+)')
    df['glass_type'] = df['glass_type'].str.lower()
    df.columns = ['glass_id', 'stock', 'bar_id']
    df = df[['glass_id', 'bar_id', 'stock']]

    # Retrieving list of unique bars in dataframe
    df_bars = df['bar_id'].unique().tolist()

    return df, df_bars


def load_transaction_bar(file, bar, compression='infer', header='infer', sep=',', date_format='%Y-%m-%d %H:%M:%S'):

    # Importing bar transaction data
    df = pd.read_csv(f'data\\{file}', compression=compression, header=header, sep=sep)

    # Data cleaning
    df.drop(columns=df.columns[0], axis=1, inplace=True)
    df.columns = ['timestamp', 'drink_id', 'amount']
    df['drink_id'] = df['drink_id'].astype(str).str.lower()
    df['amount'] = df['amount'].astype(float)
    df['bar_id'] = bar
    df['timestamp'] = pd.to_datetime(df['timestamp'], format=date_format)
    df = df[['drink_id', 'amount', 'bar_id', 'timestamp']]

    return df


def create_transactions_df(bars):

    # Concatenating all bar transaction data together
    df = pd.concat(bars, ignore_index=True)

    # Retrieving list of unique bars in dataframe
    df_bars = df['bar_id'].unique().tolist()

    return df, df_bars


def create_bars_df(list1, list2, conn):
    unique_bars = list(set(list1 + list2))
    bars = pd.read_sql('SELECT city FROM Bars_Dim', con = conn)
    flat_bar_list = [x[0] for x in bars.values.tolist()]

    return [item for item in unique_bars if item not in flat_bar_list]


def make_request(unique_drinks):
    id_list = []
    drinks_list = []
    glass_list = []

    base_url = "https://www.thecocktaildb.com/api/json/v1/1/search.php?s="
    request_delay = 0.5 # Adjust this value as needed (in seconds)

    for drink in unique_drinks:
        response = requests.get(f'{base_url}{drink}')

        # Check if the response status code is 200 (OK)
        if response.status_code == 200:
            content = response.json()
            
            # Check if 'drinks' is a key in the response JSON
            if 'drinks' in content:
                first_drink = content['drinks'][0]

                # Check if 'idDrink' and 'strGlass' exist in the first drink's data
                if 'idDrink' in first_drink and 'strGlass' in first_drink:
                    id_list.append(first_drink['idDrink'])
                    drinks_list.append(drink)
                    glass_list.append(first_drink['strGlass'].lower())
                    print(f"{drink} completed.")
                else:
                    print(f"Data for '{drink}' is incomplete.")
            else:
                print(f"No data found for '{drink}'.")
        else:
            print(f"Request for '{drink}' failed with status code {response.status_code}.")
            
        # Introduce a delay to respect rate limits
        time.sleep(request_delay)

        drink_dict = {'drink_id': id_list,
                      'drink_name': drinks_list,
                      'glass_id': glass_list}
    
    return drink_dict


def import_data_to_database(engine, bars_list, glass_stock_df, transactions_df, drinks_df):

    conn = engine.connect()

    # Appending data to Bars_Dim table
    if bars_list:
        print("     Updating Bar_Dim table in database...")
        bars_df = pd.DataFrame(bars_list, columns=['city'])
        bars_df.to_sql('Bars_Dim', engine, if_exists='append', index=False)
    else:
        print("     Bar_Dim table up-to-date in database.")

    # Replacing 'bar_id' in glass_stock_df and transactions_df with id number
    bars = pd.read_sql('SELECT bar_id, city FROM Bars_Dim', con = conn)
    bars_dict = bars.set_index('city').to_dict()['bar_id']
    glass_stock_df['bar_id'] = glass_stock_df['bar_id'].map(bars_dict)
    transactions_df['bar_id'] = transactions_df['bar_id'].map(bars_dict)

    # Checking if new glasses need to be added to Glass_Dim
    glasses = pd.read_sql('SELECT glass_name FROM Glass_Dim', con = conn)
    flat_glass_list = [x[0] for x in glasses.values.tolist()]

    glass_stock_glasses = glass_stock_df['glass_id'].unique().tolist()
    drinks_glasses = drinks_df['glass_id'].unique().tolist()
    unique_glasses = list(set(glass_stock_glasses + drinks_glasses))

    new_glasses = [item for item in unique_glasses if item not in flat_glass_list]

    # Appending data to Glass_Dim table
    if new_glasses:
        print("     Updating Glass_Dim table in database...")
        glass_df = pd.DataFrame(new_glasses, columns=['glass_name'])
        glass_df.to_sql('Glass_Dim', engine, if_exists='append', index=False)
    else:
        print("     Glass_Dim table up-to-date in database.")

    # Replacing 'glass_id' in glass_stock_df and transactions_df with id number
    glasses_sql = pd.read_sql('SELECT glass_id, glass_name FROM Glass_Dim', con = conn)
    glasses_dict = glasses_sql.set_index('glass_name').to_dict()['glass_id']
    glass_stock_df['glass_id'] = glass_stock_df['glass_id'].map(glasses_dict)
    drinks_df['glass_id'] = drinks_df['glass_id'].map(glasses_dict)

    # Replacing Glass_Stock table
    print("     Updating Glass_Stock table in database...")
    glass_stock_df.to_sql('Glass_Stock', engine, if_exists='replace', index=False)

    # Checking if new drinks need to be added to Drinks_Dim
    drinks = pd.read_sql('SELECT drink_id FROM Drinks_Dim', con = conn)
    flat_drinks_list = [x[0] for x in drinks.values.tolist()]
    new_drink_df = drinks_df[~drinks_df['drink_id'].isin(flat_drinks_list)]

    # Appending data to Drinks_Dim
    if new_drink_df.empty:
        print("     Drinks_Dim table up-to-date in database.")
    else:
        print("     Updating Drinks_Dim table in database...")
        new_drink_df.to_sql('Drinks_Dim', engine, if_exists='append', index=False)

    # Replacing 'drink_id' in transactions_df with id number
    drinks_sql = pd.read_sql('SELECT drink_id, drink_name FROM Drinks_Dim', con = conn)
    drinks_dict = drinks_sql.set_index('drink_name').to_dict()['drink_id']
    transactions_df['drink_id'] = transactions_df['drink_id'].map(drinks_dict)

    # Appending data to Transactions
    print("     Updating Transactions table in database...")
    transactions_df.to_sql('Transactions', engine, if_exists='append', index=False)


def main():

    # Connect to database
    database_url = "sqlite:///bars_database.db"  # "postgresql://username:password@host:port/database_name"
    engine = create_engine(database_url)                
    conn = engine.connect()

    print("Create database schema if doesn't already exist...")
    create_sql_tables(conn)
    
    print("Loading glass stock data...")
    glass_stock_df, unique_bars_glass_stock = load_glass_stock_data()

    print("Loading Budapest bar transaction data...")
    budapest = load_transaction_bar('budapest.csv.gz', 'budapest', compression='gzip')

    print("Loading London bar transaction data...")
    london = load_transaction_bar('london_transactions.csv.gz', 'london', compression='gzip', header=None, sep='\t')

    print("Loading New York bar transaction data...")
    new_york = load_transaction_bar('ny.csv.gz', 'new york', compression='gzip', date_format='%m-%d-%Y %H:%M')
    
    print("Creating Transactions table...")
    bars_df_list = [budapest, london, new_york]
    transactions_df, unique_bars_transactions = create_transactions_df(bars_df_list)

    print("Creating Bars table...")
    bars_list = create_bars_df(unique_bars_glass_stock, unique_bars_transactions, database_url)

    print("Getting drinks data from API...")
    unique_drinks = list(transactions_df['drink_id'].unique())
    drink_dict = make_request(unique_drinks)
    drinks_df = pd.DataFrame(drink_dict)
    drinks_df['drink_id'] = drinks_df['drink_id'].astype(int)

    print("Importing data to database...")
    import_data_to_database(engine, bars_list, glass_stock_df, transactions_df, drinks_df)

    conn.close()


if __name__ == '__main__':
    main()