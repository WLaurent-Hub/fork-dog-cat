import geopandas as gpd
import os
import time

class DataLoading():
    def __init__(self, geojson_source):
        self.geojson_source = geojson_source

    def loading_from_geojson(self):
        if os.path.exists(self.geojson_source):
            geojson = gpd.read_file(self.geojson_source)
            return geojson
        else:
            return f"File from {self.geojson_source} doesn't exist"

class DataProcessing():
    def __init__(self, geojson):
        self.geojson = geojson
    
    def calculate_density(self):
        # Project the geometry to a suitable projected CRS (e.g., EPSG:3857)
        self.geojson = self.geojson.to_crs(epsg=3857)

        colonnes = ['CHAT_2017', 'CHAT_2018', 'CHAT_2019', 'CHAT_2020', 'CHIEN_2017', 'CHIEN_2018', 'CHIEN_2019', 'CHIEN_2020']

        for colonne in colonnes:
            self.geojson[f'{colonne}_DENSITE'] = self.geojson[colonne] / self.geojson['geometry'].area * 1e6

        return self.geojson

class DataExporting():

    def __init__(self, final_data):
        self.final_data = final_data

    def export_geojson(self, output_path):
        if not os.path.exists('./result/'):
            os.mkdir('./result/')

        final_data_export = self.final_data.copy()
        final_data_export['geometry'] = final_data_export['geometry'].to_crs(epsg=4326)
        for col in final_data_export.columns:
            if final_data_export[col].dtype == 'O' and isinstance(final_data_export[col].iloc[0], list):
                final_data_export[col] = final_data_export[col].astype(str)
        final_data_export.to_file(output_path, driver='GeoJSON')
        
class DataPipeline(DataLoading, DataProcessing, DataExporting):
    def __init__(self, data_source):
        super().__init__(data_source)

    def run_process(self, data, output_path):
        print(f"=========== CALCULATE DENSITY ===========")
        final_data = DataProcessing(data).calculate_density()        
        
        print(f"=========== EXPORT DATA ===========")
        DataExporting(final_data).export_geojson(output_path)

    def pipeline_running(self, data_source, output_path):
        data_load = DataLoading(data_source)
        data = data_load.loading_from_geojson()
        
        start = time.time()
        self.run_process(data, output_path)
        end = time.time()
        print('{:.4f} s'.format(end - start))

if __name__ == "__main__":
    data_source = "./data/data_join.geojson"
    output_path = "./result/final_data.geojson"

    pipeline = DataPipeline(data_source)
    pipeline.pipeline_running(data_source, output_path)
