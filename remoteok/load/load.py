from xml.dom.domreg import registered
import pandas as pd
import datetime as dt
import time
from sqlalchemy import create_engine
# import remoteok.load.conection as pc
import conection as pc


pd.set_option('display.max_columns', 500)


class Load():
    def __init__(self) -> None:
        self.today = dt.date.today()
        self.route = f'./remoteok/clean_data/REMOTEOK_{self.today}_offers_CLEAN.csv'
        self.data = self.read_file(self.route)
        self.con = pc.connection_elephant()
        self.categories = self.insert_category(self.data["categories"].drop_duplicates(),
                                               self.con)
        self.companies = self.insert_company(self.data['Company'].drop_duplicates(),
                                             self.con)
        self.insert_data(self.data, self.categories, self.companies,
                         self.con)
        self.skills = self.insert_skills(self.data['SKILLS'],
                                         self.con)

    def read_file(self,route):
        try:
            return pd.read_csv(route)
        except:
            print('Error en la lectura de la datra')
            return None

    def find_duplicates(self,column, key, key_value, con):
        conn = con

        query = f"""SELECT * FROM {column}
        WHERE {key} = '{key_value}';"""

        df = pd.read_sql(query,
                         con=conn)
        print(df)
        # conn.close()

    def insert_company(self,df, con):
        conn = con
        df = pd.DataFrame({'name': df})
        sql_df = pd.read_sql(
            """SELECT * FROM company;
                        """,
            con=conn)

        currents = list(df['name'])
        registered = list(sql_df['name'].str.upper())
        news = pd.DataFrame({'name': list(set(currents) - set(registered))})

        if news.empty:
            print('No existen registros nuevos para company')
        else:
            news['ceo'] = 'unknown'
            news['company_premium'] = 'false'
            news['description'] = '--'
            news = pd.DataFrame(news)
            news.to_sql('company', con=conn,
                        if_exists='append',
                        index=False)
            print('se insertaron:', news)

        sql_df = pd.read_sql(
            """SELECT * FROM company;
                        """,
            con=conn)

        # conn.close()

        return(sql_df[['id_company', 'name']])

    def insert_category(self, df, con):
        conn = con
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
            print('No existen registros nuevos para position category')
        else:
            new_cats.to_sql('position_category', con=conn,
                            if_exists='append',
                            index=False)
            print('Se registro lo sigiente', new_cats)

        sql_df = pd.read_sql(
            """SELECT * FROM position_category;
                        """,
            con=conn)
        # conn.close()
        return(sql_df)

    def insert_location(self,df, con):
        df = pd.DataFrame({'country': df})
        currents = list(df['country'])
        conn = con
        sql_df = pd.read_sql(
            "SELECT * FROM location;",
            con=conn)

        registered = list(sql_df['country'])
        news = pd.DataFrame({'country': list(set(currents) - set(registered))})

        if news.empty:
            print('No existen registros nuevosoara location')
        else:
            news['continent'] = 'unknown'
            news = pd.DataFrame(news)
            news.to_sql('location', con=conn,
                        if_exists='append',
                        index=False)
            print('se insertaron: ' + (str)(len(news)),
                  news)

        sql_df = pd.read_sql(
            "SELECT * FROM location;",
            con=conn)
        # conn.close()
        return(sql_df[['id_location', 'country']])

    def get_data(self, table, con):
        conn = con
        sql_df = pd.read_sql(
            f"""SELECT * FROM {table};
                        """,
            con=conn)
        # conn.close()
        return sql_df

    def insert_data(self, df, categories, companies, con):
        comp_id = list(companies['id_company'])
        comp_nm = list(companies['name'])
        cat_id = list(categories['id_position_category'])
        cat_nm = list(categories['category'])

        for x in range(len(comp_id)):
            df['Company'] = df['Company'].replace(
                comp_nm[x], comp_id[x])

        for x in range(len(cat_id)):
            df['categories'] = df['categories'].replace(
                cat_nm[x], cat_id[x])

        df['CURRENCY'] = df['CURRENCY'].replace('USD', 2)
        df['seniority'] = 4
        df['modality'] = 'unknown'
        df[['activate', 'english', 'remote']] = 'True'
        df['english_level'] = 'conversational'
        df[['num_offers']] = 0
        locations = insert_location(df['Location'].drop_duplicates(), con)
        # time.sleep(6)
        loc_id = list(locations['id_location'])
        loc_nm = list(locations['country'])

        for x in range(len(loc_id)):
            df['Location'] = df['Location'].replace(
                loc_nm[x], loc_id[x])

        df = df[['Position', 'categories', 'seniority',
                'Description', 'modality', 'Date_published',
                 'activate', 'num_offers', 'MIN_SALARY',
                 'MAX_SALARY', 'MIDPOINT_SALARY', 'CURRENCY',
                 'remote', 'Location', 'english', 'english_level',
                 'URL', 'Company']]

        names = ['position_title', 'position_category_id',
                 'seniority_id', 'description', 'modality',
                 'date_position', 'activate', 'num_offers',
                 'salary_min', 'salary_max', 'salary',
                 'currency_id', 'remote', 'location_id',
                 'english', 'english_level', 'position_url',
                 'company_id']
        df.columns = names
        registered = get_data('position', con).drop(
            ['id_position', 'uid'], axis=1)
        # print(registered.columns)
        # print(df.columns)
        result = pd.concat([registered.drop_duplicates(), df])
        news = result[result.duplicated()]

        if news.empty:
            print('No existen registros nuevos para position')
        else:
            conn = con
            df.to_sql('position', con=conn,
                      if_exists='append',
                      index=False)
            print(f'Se cargaron : {len(news)} registrps')

    def insert_skills(self, df, con):
        df = list(df)
        df = [x.replace('[', '').replace(']', '').replace(
            "'", '').strip().split(', ') for x in df]
        df = [item for sublist in df for item in sublist]
        df = pd.DataFrame({'skill': df}).drop_duplicates()
        currents = list(df['skill'])
        conn = con
        sql_df = pd.read_sql(
            "SELECT * FROM skill;",
            con=conn)
        registered = list(sql_df['skill'])
        news = pd.DataFrame({'skill': list(set(currents) - set(registered))})

        if news.empty:
            print('No existen registros nuevos en skill')
        else:
            news.to_sql('skill', con=conn,
                        if_exists='append',
                        index=False)
            print('se insertaron: ' + (str)(len(news)),
                  news)

        sql_df = pd.read_sql(
            "SELECT * FROM skill;",
            con=conn)

        return sql_df

    def insert_position_skills(self):
        pass


if __name__ == '__main__':
    a = Load()
