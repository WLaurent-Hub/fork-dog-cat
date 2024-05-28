import pandas as pd
import geopandas as gpd
import os
import time

class DataLoading():
    """
    Class for loading data from various sources.
    """
    def __init__(self, data_source, geojson_source):
        """
        Initialize the DataLoading class.

        Parameters:
        - data_source (str): Path to the directory containing CSV files.
        - geojson_source (str): Path to the GeoJSON file.
        """
        self.data_source = data_source
        self.geojson_source = geojson_source

    def loading_from_xlsx(self):
        """
        Load data from CSV files into Pandas DataFrames.

        Returns:
        - Tuple of Pandas DataFrames: DataFrames loaded from CSV files.
        """
        if os.path.exists(self.data_source):
            df2017 = pd.read_csv(os.path.join(self.data_source, "2017-geocode.csv"), header=0, sep=';')
            df2018 = pd.read_csv(os.path.join(self.data_source, "2018-geocode.csv"), header=0, sep=';')
            df2019 = pd.read_csv(os.path.join(self.data_source, "2019-geocode.csv"), header=0, sep=';')
            df2020 = pd.read_csv(os.path.join(self.data_source, "2020-geocode.csv"), header=0, sep=';')
            return df2017, df2018, df2019, df2020
        else:
            return f"File from {self.data_source} doesn't exist"

    def loading_from_geojson(self):
        """
        Load GeoJSON data into a GeoDataFrame.

        Returns:
        - GeoDataFrame: GeoDataFrame loaded from the GeoJSON file.
        """
        if os.path.exists(self.geojson_source):
            geojson = gpd.read_file(self.geojson_source)
            return geojson
        else:
            return f"File from {self.geojson_source} doesn't exist"

class DataProcessing():
    """
    Class for processing data, including spatial operations.
    """
    def __init__(self, geojson):
        """
        Initialize the DataProcessing class.

        Parameters:
        - geojson (GeoDataFrame): GeoDataFrame containing geometries for spatial operations.
        """
        self.geojson = geojson
        
    def sum_with_geojson(self, list_df, list_year):
        """
        Perform spatial join and aggregation on GeoDataFrame.

        Parameters:
        - list_df (list of DataFrames): List of DataFrames to be spatially joined.
        - list_year (list): List of corresponding years for labeling columns.

        Returns:
        - GeoDataFrame: Resulting GeoDataFrame after spatial join and aggregation.
        """
        data_join = self.geojson.copy()
        
        for i, df in enumerate(list_df):    
            gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['LON'], df['LAT']), crs='EPSG:4326')
            gdf = gdf.rename(columns={'CHAT': f'CHAT_{list_year[i]}', 'CHIEN': f'CHIEN_{list_year[i]}'})
            data_join['geometry'] = data_join.geometry
            joined_data = gpd.sjoin(data_join, gdf, how='left', op='contains')

            grouped_data = joined_data.groupby('insee_com', as_index=False).agg({f'CHAT_{list_year[i]}': 'sum', f'CHIEN_{list_year[i]}': 'sum',
                                                                                 'insee_com':'first'})
            
            data_join = data_join.merge(grouped_data, how='left', on='insee_com')
            
        return data_join

class DataExporting():
    """
    Class for exporting data to different formats.
    """
    def __init__(self, final_data):
        """
        Initialize the DataExporting class.

        Parameters:
        - final_data (DataFrame): DataFrame to be exported.
        """
        self.final_data = final_data

    def export_csv(self):
        """
        Export data to CSV format.

        Creates a result directory if it doesn't exist and appends to an existing Excel file.

        If the Excel file doesn't exist, it creates a new one.

        Returns:
        - None
        """
        if not os.path.exists('./result/'):
            os.mkdir('./result/')

        if os.path.isfile(f'./result/data_join.xlsx'):
            with pd.ExcelWriter(f'./result/data_join.xlsx', engine='openpyxl', mode="a") as writer:
                self.final_data.to_excel(writer, header=True, index=True)
        else:
            with pd.ExcelWriter(f'./result/data_join.xlsx', engine='openpyxl', mode="w") as writer:
                self.final_data.to_excel(writer, header=True, index=True)

class DataPipeline(DataLoading, DataProcessing, DataExporting):
    """
    Class representing the entire data processing pipeline.
    """
    def __init__(self):
        super().__init__(data_source, geojson_source)

    def run_process(self, list_df, list_year, data_load):
        """
        Run the entire data processing pipeline.

        Parameters:
        - list_df (list of DataFrames): List of DataFrames to be used in the pipeline.
        - list_year (list): List of corresponding years for labeling columns.
        - data_load (DataLoading): DataLoading object for loading data.

        Returns:
        - None
        """
        geojson = data_load.loading_from_geojson()
        
        data_process = DataProcessing(geojson)
        print(f"=========== JOIN DATA FOR ALL YEARS ===========")
        data_join = data_process.sum_with_geojson(list_df, list_year)        
        
        data_export = DataExporting(data_join)
        print(f"=========== EXPORT DATA ===========")
        data_export.export_csv()

    def pipeline_running(self, data_source, geojson_source):
        """
        Run the entire data processing pipeline.

        Parameters:
        - data_source (str): Path to the directory containing CSV files.
        - geojson_source (str): Path to the GeoJSON file.

        Returns:
        - None
        """
        data_load = DataLoading(data_source, geojson_source)
        df2017, df2018, df2019, df2020 = data_load.loading_from_xlsx()
        df_list = [df2017, df2018, df2019, df2020]
        years = [2017, 2018, 2019, 2020]
        
        start = time.time()
        DataPipeline().run_process(df_list, years, data_load)
        end = time.time()
        print('{:.4f} s'.format(end - start))

if __name__ == "__main__":
    data_source = "./data/"
    geojson_source = "./data/communes.geojson"

    pipeline = DataPipeline()
    pipeline.pipeline_running(data_source, geojson_source)
