import pandas as pd
import numpy as np
import datetime as dt


class Clean():
    def __init__(self):
        today = dt.date.today()
        route = f'./remoteok/raw_data/REMOTEOK_{today}_offers.csv'
        self.raw_data = self.read_raw(route)
        self.clean_data = self.clean_data(self.raw_data)

    def read_raw(self, route):
        try:
            return pd.read_csv(route)
        except:
            print('Andala Osa el archivo no existe')
            return None

    def clean_data(self, data):
        if not data.empty:
            df = data
            df['Salario'] = df['Salario'].str.replace('💰 ', ''
                                        ).str.replace('$', ''
                                        ).str.replace('*', ''
                                        ).str.replace('k', '000'
                                        )

            df[['MIN Salario', 'MAX Salario']] = df['Salario'].str.split(
                ' - ', 1, expand=True)
            df['Salario']
            df['MAX Salario'] = pd.to_numeric(df['MAX Salario'])
            df['MIN Salario'] = pd.to_numeric(df['MIN Salario'])
            df['AVG Salario'] = (df['MAX Salario'] + df['MIN Salario']) / 2
            df = df.drop(['Salario'], axis=1)
            df['currency'] = 'USD'
            df['Sallary period'] = 'Year'
            df['Ubicacion'] = df['Ubicacion'].str.replace('🌏 ', '')
            # print(df['Descripción'])
            df['Descripción'] = df['Descripción'].str.replace('[', ''
                                   ).str.replace(r'\\n', ' '
                                   ).str.replace(']', ''
                                   ).str.replace('🔎', ''
                                   ).str.replace("\\", ' '
                                   ).str.replace('✅', ''
                                   ).str.replace('"', ''
                                   ).str.replace('🌏 ', ''
                                   ).str.replace(' 👋', ''
                                   ).str.replace("'", '´'
                                   ).str.strip()

            a = []
            b = list(df['Descripción'])
            for text in b:
                a.append(' '.join(text.split()))

            df['Descripción'] = a
            today = dt.date.today()
            print(df.columns)

            df.columns = ['Position', 'Company', 'Location', 'Date_published',
                          'URL', 'Description', 'SKILLS','Home_URL',
                          'Site_Name', 'MIN_SALARY', 'MAX_SALARY', 
                          'MIDPOINT_SALARY', 'CURRENCY', 'SALLARY_PERIOD'
                          ]

            df['Date_published'] = df['Date_published'].str.replace('T', ' ')
            df['Company'] = df['Company'].str.upper()
            df['Position'] = df['Position'].str.upper()

            front = ['JAVASCRIPT','FRONTEND', 'FRONT END', 'REACT', ' UI',' UX']
            back = ['RUBY ON RAILS', 'PYTHON', 'BACKEND', 'BACK END', 'PHP',
                       'GO DEVELOPER', 'GO', 'API ']
            mng = ['MANAGER','MANAGMENT']
            full = ['FULL STACK','FULLSTACK']
            aly = ['ANALYST','ANALYTICS', 'BUSSINES INTELLIGENCE']
            unk = ['DEVELOPER','SOFTWARE','WEB','APPLICATION', 'SOLUTIONS']

            conditions = [
                df['Position'].isin(front),
                df['Position'].isin(back),
                df['Position'].isin(mng),
                df['Position'].isin(full),
                df['Position'].isin(aly),
                df['Position'].str.contains('DEVOPS'),
                df['Position'].str.contains('EDITOR'),
                df['Position'].str.contains('ECONOMIST'),
                df['Position'].str.contains('BLOCKCHAIN'),
                df['Position'].str.contains('WRAITER'),
                df['Position'].str.contains('EDITOR'),
                df['Position'].str.contains('CONSULTANT'),
                df['Position'].str.contains('SUPPORT'),
                df['Position'].str.contains('DATA'),
                df['Position'].str.contains('ECONOMIST'),
                df['Position'].str.contains('RECRUITER'),
                df['Position'].str.contains('HR'),
                df['Position'].isin(unk),
            ]

            choices = [
                'FRONTEND',
                'BACKEND',
                'MANAGER',
                'FULLSTACK',
                'ANALYST',
                'DEVOPS',
                'EDITOR',
                'ECONOMIST',
                'BLOCKCHAIN',
                'WRAITER',
                'EDITOR',
                'CONSULTANT',
                'CUSTOMER SUPPORT',
                'DATA',
                'ECONOMIST',
                'RECRUITER',
                'HUMAN RESOURCES',
                'DEVELOPER',
            ]

            df['categories'] = np.select(conditions, choices,'UNKNOWN')

            df.to_csv(f'./remoteok/clean_data/REMOTEOK_{today}_offers_CLEAN.csv',
                      encoding='utf-8-sig', index=False)
        else:
            print('No existen datos que limpiar')


if __name__ == '__main__':
    a = Clean()
