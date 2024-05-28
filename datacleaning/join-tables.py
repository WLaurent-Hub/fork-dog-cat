import pandas as pd
import geopandas as gpd
from rapidfuzz import process, utils
from unidecode import unidecode
import os
import rapidfuzz
from joblib import Parallel, delayed

# Chargement des données
class DataLaoding():
    def __init__(self, data_source, geojson_source):
        self.data_source = data_source
        self.geojson_source = geojson_source

    def loading_from_xlsx(self):
        if os.path.exists(self.data_source) == True:
            df2017 = pd.read_csv(os.path.join(self.data_source, "2017.csv"), header=0, sep=',', nrows=100, chunksize = 20)
            # df2018 = pd.read_csv(os.path.join(self.data_source, "2018.csv"), header=0, sep=',', nrows=300, chunksize = 50)
            # df2019 = pd.read_csv(os.path.join(self.data_source, "2019.csv"), header=0, sep=',', nrows=300, chunksize = 50)
            # df2020 = pd.read_csv(os.path.join(self.data_source, "2020.csv"), header=0, sep=',', nrows=300, chunksize = 50)
            return df2017
        else:
            return f"File from {self.data_source} doesn't exist"

    def loading_from_geojson(self):
        if os.path.exists(self.geojson_source) == True:
            geojson = gpd.read_file(self.geojson_source)
            return geojson
        else:
            return f"File from {self.geojson_source} doesn't exist"

# Traitement de nettoyage
class DataProcessing():
    def __init__(self, data, geojson):
        self.data = data
        self.geojson = geojson

    def group_data(self):
        chunks = pd.DataFrame()

        for chunk in self.data:
            grouped = chunk.groupby(['VILLE', 'ESPECE'])['POPULATION'].sum().reset_index()
            chunks = pd.concat([chunks, grouped], ignore_index=True)

        grouped_data = chunks.groupby(['VILLE', 'ESPECE'])['POPULATION'].sum().unstack(fill_value=0).reset_index()
        return grouped_data

    def format_data(self):
        self.geojson['new_com'] = self.geojson['nom_de_la_commune'].str.lower()
        self.geojson['new_com'] = self.geojson['new_com'].apply(unidecode)
        self.geojson['new_com'] = self.geojson['new_com'].str.replace(' ', '')

    def search_corres(self, grouped_data):
        geojson_list = self.geojson['new_com']
        processed_list = [utils.default_process(name) for name in geojson_list]

        for (i, value) in enumerate(grouped_data['VILLE']):
            value = value.lower()
            value = unidecode(value)
            value = value.replace(' ', '')
            match = process.extractOne(value, processed_list, processor=None, score_cutoff=93)

            if match:
                grouped_data.loc[i, 'NEW_VILLE'] = self.geojson.loc[match[2], 'nom_de_la_commune']
                grouped_data.loc[i, 'CODE POSTAL'] = self.geojson.loc[match[2], 'code_postal']
                grouped_data.loc[i, 'SCORE'] = match[1]
                grouped_data.loc[i, 'code_insee'] = self.geojson.loc[match[2], 'code_commune_insee']

        return grouped_data

    # def final_treatment(self, corres_data):


# Exportation des données
class DataExporting():
    def __init__(self, output_file, final_data):
        self.output_file = output_file
        self.final_data = final_data

    def export_csv(self):
        with pd.ExcelWriter(self.output_file, engine='openpyxl', mode='w') as writer:
            self.final_data.to_excel(writer, sheet_name='Sheet1', header=True, index=True)

# Héritage des processus de traitements
class DataPipeline(DataLaoding, DataProcessing, DataExporting):

    def __init__(self):
        super().__init__(data_source, geojson_source)

    # Run du script
    def pipeline_running(self, data_source, geojson_source):
        data_load = DataLaoding(data_source, geojson_source)
        df2017 = data_load.loading_from_xlsx()
        geojson = data_load.loading_from_geojson()

        data_process = DataProcessing(df2017, geojson)
        grouped_data = data_process.group_data()
        data_process.format_data()
        corres_data = data_process.search_corres(grouped_data)
        # final_data = data_process.final_treatment(corres_data)

        output_file = f'./result_2017.xlsx'
        data_export = DataExporting(output_file, corres_data)
        data_export.export_csv()

if __name__ == "__main__":
    data_source = "./data/"
    geojson_source = "./data/code-postal-insee.geojson"

    pipeline = DataPipeline()
    pipeline.pipeline_running(data_source, geojson_source)
