import pandas as pd
import geopandas as gpd
from rapidfuzz import utils, process
import os
from joblib import Parallel, delayed
import time
import re

class DataLoading():
    def __init__(self, data_source, geojson_source):
        self.data_source = data_source
        self.geojson_source = geojson_source

    def loading_from_xlsx(self):
        if os.path.exists(self.data_source):
            df2013 = pd.read_csv(os.path.join(self.data_source, "data2013.csv"), header=0, sep=',', chunksize=1000)
            df2014 = pd.read_csv(os.path.join(self.data_source, "data2014.csv"), header=0, sep=',', chunksize=1000)
            df2015 = pd.read_csv(os.path.join(self.data_source, "data2015.csv"), header=0, sep=',', chunksize=1000)
            df2016 = pd.read_csv(os.path.join(self.data_source, "data2016.csv"), header=0, sep=',', chunksize=1000)
            df2019 = pd.read_csv(os.path.join(self.data_source, "data2019.csv"), header=0, sep=',', chunksize=1000)
            return df2013, df2014, df2015, df2016, df2019
        else:
            return f"File from {self.data_source} doesn't exist"

    def loading_from_geojson(self):
        if os.path.exists(self.geojson_source):
            geojson = gpd.read_file(self.geojson_source)
            return geojson
        else:
            return f"File from {self.geojson_source} doesn't exist"

class DataProcessing():

    def __init__(self, geojson):
        self.geojson = geojson
    
    def search_corres(self, chunk, val):
        geojson_list = self.geojson['nom_comm'].str.replace('-', ' ').str.replace('/', 'sur').str.replace('st', 'saint')
        processed_list = [utils.default_process(name) for name in geojson_list]
        
        processed_chunks = []
        
        for (i, value) in enumerate(chunk['Ville']):
            i = i + val
            value = str(value)            
            value = value.replace('-', '').replace('/', 'sur').replace('st', 'saint').replace('-', ' ').replace('cedex', '')
            value = ''.join([l for l in value if not l.isdigit()])
            value = re.sub(r"\(.*?\)", "", value)
            
            match = process.extractOne(value, processed_list, processor=utils.default_process, score_cutoff=90)
            
            if match:
                chunk.loc[i, 'VILLE_2'] = self.geojson.loc[match[2], 'nom_comm']
                chunk.loc[i, 'CODE POSTAL'] = self.geojson.loc[match[2], 'postal_code']
                chunk.loc[i, 'CODE INSEE'] = self.geojson.loc[match[2], 'insee_com']
                chunk.loc[i, 'LON'] = self.geojson.loc[match[2], 'geo_point_2d']['lon']
                chunk.loc[i, 'LAT'] = self.geojson.loc[match[2], 'geo_point_2d']['lat']
                chunk.loc[i, 'COORDONNEES'] = f'[{self.geojson.loc[match[2], "geo_point_2d"]["lon"]}, {self.geojson.loc[match[2], "geo_point_2d"]["lat"]}]'
            
        processed_chunks.append(chunk)
        
        return processed_chunks

    def group(self, verified_data):
        verified_data = verified_data[verified_data['VILLE_2'] != '']
        pivoted = pd.pivot_table(verified_data, values='Population', index=['VILLE_2'], columns=['Espece'], aggfunc='sum')
        pivoted = pivoted.reset_index()
        pivoted = pivoted.fillna(0)

        add_ville_2 = verified_data.groupby('VILLE_2')['CODE INSEE'].first().reset_index()
        add_coordonnees = verified_data.groupby('VILLE_2')['COORDONNEES'].first().reset_index()
        add_code_postal = verified_data.groupby('VILLE_2')['CODE POSTAL'].first().reset_index()
        grouped_data = pd.merge(pivoted, add_ville_2, on='VILLE_2')
        grouped_data = pd.merge(grouped_data, add_coordonnees, on='VILLE_2')
        grouped_data = pd.merge(grouped_data, add_code_postal, on='VILLE_2')
        
        # Mise à jour du champs LAT et LON
        grouped_data['LON'] = grouped_data['COORDONNEES'].str.split().str[0].str.replace('[', '').str.replace(',', '')
        grouped_data['LAT'] = grouped_data['COORDONNEES'].str.split().str[1].str.replace(']', '').str.replace(',', '')

        return grouped_data

class DataExporting():

    def __init__(self, output_sheet, final_data):
        self.output_sheet = output_sheet
        self.final_data = final_data

    def export_csv(self):
        if not os.path.exists('./result/'):
            os.mkdir('./result/')

        if os.path.isfile(f'./result/{self.output_sheet}.xlsx'):
            with pd.ExcelWriter(f'./result/{self.output_sheet}.xlsx', engine='openpyxl', mode="a") as writer:
                self.final_data.to_excel(writer, sheet_name=str(self.output_sheet), header=True, index=True)
        else:
            with pd.ExcelWriter(f'./result/{self.output_sheet}.xlsx', engine='openpyxl', mode="w") as writer:
                self.final_data.to_excel(writer, sheet_name=str(self.output_sheet), header=True, index=True)

class DataPipeline(DataLoading, DataProcessing, DataExporting):
    def __init__(self):
        super().__init__(data_source, geojson_source)

    def run_process(self, data_load, df, year):
        geojson = data_load.loading_from_geojson()

        processed_data = []
        val = 0
        data_process = DataProcessing(geojson)
        print(f"=========== VERIFICATION DATA for {year} ===========")
        for _, chunk in enumerate(df):  
            verified_chunk = data_process.search_corres(chunk, val)
            processed_data.extend(verified_chunk)
            val += 1000

        verified_data = pd.concat(processed_data, ignore_index=True)
        print(f"=========== GROUP DATA for {year} ===========")
        grouped_data = data_process.group(verified_data)
                
        data_export = DataExporting(year, grouped_data)
        print(f"=========== EXPORT DATA for {year} ===========")
        data_export.export_csv()

    def pipeline_running(self, data_source, geojson_source):
        data_load = DataLoading(data_source, geojson_source)
        df2013, df2014, df2015, df2016, df2019 = data_load.loading_from_xlsx()
        df_list = [df2013, df2014, df2015, df2016, df2019]
        years = [2013, 2014, 2015, 2016, 2019]

        start = time.time()
        Parallel(n_jobs=5, prefer="threads")(delayed(DataPipeline().run_process)(data_load, df, years[year]) for year, df in enumerate(df_list))
        end = time.time()
        print('{:.4f} s'.format(end - start))

if __name__ == "__main__":
    data_source = "./data/"
    geojson_source = "./data/communes.geojson"

    pipeline = DataPipeline()
    pipeline.pipeline_running(data_source, geojson_source)
