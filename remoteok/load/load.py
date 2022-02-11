import pandas as pd
import datetime as dt
import psycopg2
from sqlalchemy import create_engine
# import remoteok.load.conection as pc
import conection as pc


pd.set_option('display.max_columns', 500)


def read_file(route):
    try:
        return pd.read_csv(route)
    except:
        print('Error en la lectura de la datra')


def find_duplicates(column, key, key_value):
    conn = pc.connection_elephant()

    query = f"""SELECT * FROM {column}
    WHERE {key} = '{key_value}';"""

    df = pd.read_sql(query,
                     con=conn)
    print(df)
    conn.close()


def insert_company(df):
    conn = pc.connection_elephant()
    df = pd.DataFrame({'name': df})
    sql_df = pd.read_sql(
        """SELECT * FROM company;
                    """,
        con=conn)

    currents = list(df['name'])
    registered = list(sql_df['name'].str.upper())
    news = pd.DataFrame({'name': list(set(currents) - set(registered))})

    if news.empty:
        print('No existen registros nuevos')
    else:
        df['ceo'] = 'unknown'
        df['company_premium'] = 'false'
        df['description'] = '--'
        df = pd.DataFrame(df)
        df.to_sql('company', con=conn,
                    if_exists='append',
                    index=False)
        print('se insertaron:', news)

    conn.close()
    return(sql_df[['id_company','name']])


def insert_category(df):
    conn = pc.connection_elephant()
    df = pd.DataFrame({'category': df})
    sql_df = pd.read_sql(
        """SELECT * FROM position_category;
                    """,
        con=conn)
    current_cats = list(df['category'])
    registered_cats = list(sql_df['category'].str.upper())
    new_cats = pd.DataFrame(
        {'category': list(set(current_cats) - set(registered_cats))})
    if new_cats.empty:
        print('No hay nada nuevo')
    else:
        new_cats.to_sql('position_category', con=conn,
                        if_exists='append',
                        index=False)
        print('Se registro lo sigiente', new_cats)
    conn.close()
    return(sql_df)


def get_data():
    conn = pc.connection_elephant()
    sql_df = pd.read_sql(
        """SELECT * FROM position_category;
                    """,
        con=conn)
    print(sql_df)
    conn.close()

def insert_data(df,categories, companies):
    conn = pc.connection_elephant()
    # categories = pd.read_sql("SELECT * FROM position_category;",
    #     con=conn)
    # categories = dict(categories)

    # companies = pd.read_sql(
    #     """SELECT name FROM company;
    #                 """,
    #     con=conn)

    # # companies = dict(companies)
    print(companies)
    print(categories)



def main():
    today = dt.date.today()
    data = read_file(
        f'./remoteok/clean_data/REMOTEOK_{today}_offers_CLEAN.csv')
    categories = insert_category(data["categories"].drop_duplicates())
    companies = insert_company(data['Company'].drop_duplicates())
    # find_duplicates('company','name','FIGMA')
    insert_data(data,categories, companies)
    # get_data()


if __name__ == '__main__':
    main()
