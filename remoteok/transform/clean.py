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
            df['Salario'] = df['Salario'].str.replace('💰 ', '').str.replace(
                '$', '').str.replace('*', '').str.replace('k', '000')
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
            df['Descripción'] = df['Descripción'].str.replace('[', '').str.replace(r'\\n', ' ').str.replace(']', '').str.replace(
                '🔎', '').str.replace("\\", ' ').str.replace('✅', '').str.replace('"', '').str.replace('🌏 ', '').str.replace(
                ' 👋', '').str.replace("'", '´').str.strip()
            a = []
            b = list(df['Descripción'])
            for text in b:
                a.append(' '.join(text.split()))

            df['Descripción'] = a
            today = dt.date.today()
            df.columns = ['Position','Company','Location','Date_published','URL','Description','SKILLS','Home_URL','Site_Name','MIN_SALARY','MAX_SALARY','MIDPOINT_SALARY','CURRENCY','SALLARY_PERIOD']
            df['Date_published'] = df['Date_published'].str.replace('T',' ')
            df['Company'] = df['Company'].str.upper()
            df['Position'] = df['Position'].str.upper()
            df.to_csv(f'./remoteok/clean_data/REMOTEOK_{today}_offers_CLEAN.csv',
                      encoding='utf-8-sig', index=False)
        else:
            print('No existen datos que limpiar')


if __name__ == '__main__':
    a = Clean()
