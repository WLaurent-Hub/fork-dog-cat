import pandas as pd
import geopandas as gpd
from rapidfuzz import utils, fuzz, process
import os
from joblib import Parallel, delayed
import time
import re

class DataLoading():
    """
    This class handles loading data from various sources.
    """

    def __init__(self, data_source, geojson_source):
        """
        Initializes the DataLoading instance.

        Args:
            data_source (str): The path to the data source directory.
            geojson_source (str): The path to the GeoJSON source file.
        """
        self.data_source = data_source
        self.geojson_source = geojson_source

    def loading_from_xlsx(self):
        """
        Load data from CSV files within the data source directory.

        Returns:
            tuple: DataFrames for different years (e.g., df2017, df2018, df2019).
        """
        if os.path.exists(self.data_source):
            df2017 = pd.read_csv(os.path.join(self.data_source, "2017.csv"), header=0, sep=',', chunksize=1000)
            df2018 = pd.read_csv(os.path.join(self.data_source, "2018.csv"), header=0, sep=',', chunksize=1000)
            df2019 = pd.read_csv(os.path.join(self.data_source, "2019.csv"), header=0, sep=',', chunksize=1000)
            df2020 = pd.read_csv(os.path.join(self.data_source, "2020.csv"), header=0, sep=',', chunksize=1000)
            return df2017, df2018, df2019, df2020
        else:
            return f"File from {self.data_source} doesn't exist"

    def loading_from_geojson(self):
        """
        Load GeoJSON data from the specified source file.

        Returns:
            geopandas.GeoDataFrame: GeoJSON data.
        """
        if os.path.exists(self.geojson_source):
            geojson = gpd.read_file(self.geojson_source)
            return geojson
        else:
            return f"File from {self.geojson_source} doesn't exist"

class DataProcessing():
    """
    This class handles data processing and cleaning.
    """

    def __init__(self, data, geojson):
        """
        Initializes the DataProcessing instance.

        Args:
            data (pd.DataFrame): The data to be processed.
            geojson (geopandas.GeoDataFrame): GeoJSON data for additional information.
        """
        self.data = data
        self.geojson = geojson

    def verify_corres(self, chunk, val):
        """
        Verify and clean the correspondence of data.

        Args:
            chunk (pd.DataFrame): Data chunk to be processed.
            val (int): Offset value for indexing.

        Returns:
            list: Processed data chunks.
        """
        processed_chunks = []
        for i, ville in enumerate(chunk['VILLE']):
            i = i + val
            original_ville = chunk.loc[i, 'VILLE_2']
            original_ville = original_ville.replace('-', ' ').replace('/', 'sur')
            ville = ville.replace('st', 'saint').replace('-', ' ')

            if len(ville.split()) != len(original_ville.split()):
                original_ville = ' '.join(original_ville.split())

            original_ville = ''.join([l for l in original_ville if not l.isdigit()])
            original_ville = original_ville.replace('st', 'saint')
            original_ville = original_ville.replace('cedex', '')
            original_ville = re.sub(r"\(.*?\)", "", original_ville)

            if fuzz.ratio(ville, original_ville, processor=utils.default_process) < 80:
                chunk.loc[i, 'VILLE'] = ''
                chunk.loc[i, 'COORDONNEES'] = ''

        # Append the processed chunk to the list of processed chunks
        processed_chunks.append(chunk)

        return processed_chunks
    
    def verify_with_geojson(self, verify_data):
        geojson_list = self.geojson['nom_comm'].str.replace('-', ' ').str.replace('/', 'sur').str.replace('st', 'saint')
        processed_list = [utils.default_process(name) for name in geojson_list]
        
        for (i, value) in enumerate(verify_data['VILLE_2']):
            value = value.replace('-', '').replace('/', 'sur').replace('st', 'saint').replace('-', ' ').replace('cedex', '')
            value = ''.join([l for l in value if not l.isdigit()])
            value = re.sub(r"\(.*?\)", "", value)
            
            match = process.extractOne(value, processed_list, processor=utils.default_process, score_cutoff=93)

            if match:
                verify_data.loc[i, 'VILLE_3'] = self.geojson.loc[match[2], 'nom_comm']
                verify_data.loc[i, 'CODE POSTAL'] = self.geojson.loc[match[2], 'postal_code']
                verify_data.loc[i, 'CODE INSEE'] = self.geojson.loc[match[2], 'insee_com']
                verify_data.loc[i, 'LON'] = self.geojson.loc[match[2], 'geo_point_2d']['lon']
                verify_data.loc[i, 'LAT'] = self.geojson.loc[match[2], 'geo_point_2d']['lat']
                verify_data.loc[i, 'COORDONNEES'] = f'[{self.geojson.loc[match[2], "geo_point_2d"]["lon"]}, {self.geojson.loc[match[2], "geo_point_2d"]["lat"]}]'

        return verify_data

    def group(self, verified_data):
        """
        Group and aggregate data based on 'VILLE' and 'ESPECE'.

        Args:
            verified_data (pd.DataFrame): Processed data with 'VILLE' cleaned.

        Returns:
            pd.DataFrame: Grouped data with population sums for different species.
        """
        verified_data = verified_data[verified_data['VILLE'] != '']
        pivoted = pd.pivot_table(verified_data, values='POPULATION', index=['VILLE'], columns=['ESPECE'], aggfunc='sum')
        pivoted = pivoted.reset_index()
        pivoted = pivoted.fillna(0)

        add_ville_2 = verified_data.groupby('VILLE')['VILLE_2'].first().reset_index()
        add_coordonnees = verified_data.groupby('VILLE')['COORDONNEES'].first().reset_index()
        grouped_data = pd.merge(pivoted, add_ville_2, on='VILLE')
        grouped_data = pd.merge(grouped_data, add_coordonnees, on='VILLE')
        
        # Mise à jour du champs LAT et LON
        grouped_data['LON'] = grouped_data['COORDONNEES'].str.split().str[0].str.replace('[', '').str.replace(',', '')
        grouped_data['LAT'] = grouped_data['COORDONNEES'].str.split().str[1].str.replace(']', '').str.replace(',', '')

        return grouped_data

    def final_grouping(self, data_concatenated):
        data_full = data_concatenated[data_concatenated['VILLE_3'] != '']
        data_empty = data_concatenated[data_concatenated['VILLE_3'].isnull()]
        
        data_ville3 = data_full.groupby(['VILLE_3'], as_index=False).agg({'CHAT': 'sum', 'CHIEN': 'sum', 
                                                                                 'VILLE': 'first', 'VILLE_2': 'first',
                                                                                 'COORDONNEES': 'first', 
                                                                                 'LON': 'first', 'LAT': 'first',
                                                                                 'CODE POSTAL': 'first', 'CODE INSEE': 'first'})

        return pd.concat([data_ville3, data_empty])
        
    def add_empty_values(self, verified_data):
        """
        Add empty values for rows with 'VILLE' as an empty string.

        Args:
            verified_data (pd.DataFrame): Processed data.

        Returns:
            pd.DataFrame: Data with empty values grouped by 'VILLE_2'.
        """
        verified_data = verified_data[verified_data['VILLE'] == '']
        pivoted = pd.pivot_table(verified_data, values='POPULATION', index=['VILLE_2'], columns=['ESPECE'], aggfunc='sum')
        pivoted = pivoted.reset_index()
        pivoted = pivoted.fillna(0)

        add_ville = verified_data.groupby('VILLE_2')['VILLE'].first().reset_index()
        add_coordonnees = verified_data.groupby('VILLE_2')['COORDONNEES'].first().reset_index()

        empty_values = pd.merge(pivoted, add_ville, on='VILLE_2')
        empty_values = pd.merge(empty_values, add_coordonnees, on='VILLE_2')

        return empty_values

class DataExporting():
    """
    This class handles exporting processed data.
    """

    def __init__(self, output_sheet, final_data):
        """
        Initializes the DataExporting instance.

        Args:
            output_sheet (str): Name of the output sheet.
            final_data (pd.DataFrame): Final processed data.
        """
        self.output_sheet = output_sheet
        self.final_data = final_data

    def export_csv(self):
        """
        Export the final data to an Excel file.

        Saves the data to an Excel file with optional appending.

        """
        if not os.path.exists('./result/'):
            os.mkdir('./result/')

        if os.path.isfile(f'./result/{self.output_sheet}.xlsx'):
            with pd.ExcelWriter(f'./result/{self.output_sheet}.xlsx', engine='openpyxl', mode="a") as writer:
                self.final_data.to_excel(writer, sheet_name=str(self.output_sheet), header=True, index=True)
        else:
            with pd.ExcelWriter(f'./result/{self.output_sheet}.xlsx', engine='openpyxl', mode="w") as writer:
                self.final_data.to_excel(writer, sheet_name=str(self.output_sheet), header=True, index=True)

class DataPipeline(DataLoading, DataProcessing, DataExporting):
    """
    This class defines the data processing pipeline.
    """

    def __init__(self):
        super().__init__(data_source, geojson_source)

    def run_process(self, data_load, df, year):
        geojson = data_load.loading_from_geojson()

        processed_data = []
        val = 0
        data_process = DataProcessing(df, geojson)
        print(f"=========== GROUP DATA for {year} ===========")
        for chunk in df:  # Iterate through chunks
            verified_chunk = data_process.verify_corres(chunk, val)
            processed_data.extend(verified_chunk)
            val += 100

        verified_data = pd.concat(processed_data, ignore_index=True)

        print(f"=========== GROUP DATA for {year} ===========")
        grouped_data = data_process.group(verified_data)
        
        print(f"=========== ADD EMPTY VALUES for {year} ===========")
        empty_values = data_process.add_empty_values(verified_data)
        
        print(f"=========== SEARCH CORRES WITH GEOJSON FOR {year} ===========")
        grouped_data2 = data_process.verify_with_geojson(grouped_data)
        empty_values2 = data_process.verify_with_geojson(empty_values)
        
        data_concatenated = pd.concat([grouped_data2, empty_values2])
        
        print(f"=========== FINAL GROUPING FOR {year} ===========")
        data_final = data_process.final_grouping(data_concatenated)
        
        data_export = DataExporting(year, data_final)
        print(f"=========== EXPORT DATA for {year} ===========")
        data_export.export_csv()

    def pipeline_running(self, data_source, geojson_source):
        """
        Run the data processing pipeline.

        Loads data, processes it, and exports the final data.

        Args:
            data_source (str): Path to the data source directory.
            geojson_source (str): Path to the GeoJSON source file.
        """
        data_load = DataLoading(data_source, geojson_source)
        df2017, df2018, df2019, df2020 = data_load.loading_from_xlsx()
        df_list = [df2017, df2018, df2019, df2020]
        years = [2017, 2018, 2019, 2020]

        start = time.time()
        Parallel(n_jobs=4, prefer="threads")(delayed(DataPipeline().run_process)(data_load, df, years[year]) for year, df in enumerate(df_list))
        end = time.time()
        print('{:.4f} s'.format(end - start))

if __name__ == "__main__":
    data_source = "./data-cleaned/"
    geojson_source = "./data-cleaned/communes/communes.geojson"

    pipeline = DataPipeline()
    pipeline.pipeline_running(data_source, geojson_source)
