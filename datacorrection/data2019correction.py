import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, explained_variance_score, median_absolute_error
import matplotlib.pyplot as plt
import os
import time
import pandas as pd
import geopandas as gpd

class DataLoading():
    """
    Class for loading data from various sources.
    """
    def __init__(self, data_source):
        self.data_source = data_source

    def loading_from_geojson(self):
        """
        Load GeoJSON data into a GeoDataFrame.

        Returns:
        - GeoDataFrame: GeoDataFrame loaded from the GeoJSON file.
        """
        if os.path.exists(self.data_source):
            geojson = gpd.read_file(self.data_source, rows=20)
            return geojson
        else:
            return f"File from {self.data_source} doesn't exist"
        
class DataProcessing():
    def __init__(self):
        super().__init__()
        
    def apply_regression(self, x, y):
        x = np.array(x, dtype=np.float64).reshape(-1, 1)
        y = np.array(y)

        model = LinearRegression()
        model.fit(x, y)

        coeff = model.coef_[0]
        origine = model.intercept_
        predict_2019 = int(model.predict([[2019]])[0])

        return coeff, origine, predict_2019
        
class DataPipeline(DataLoading, DataProcessing):
    """
    Class representing the entire data processing pipeline.
    """
    def __init__(self):
        super().__init__(data_source)

    def run_process(self, data_source):
        data_load = DataLoading(data_source)
        data = data_load.loading_from_geojson()
        
        data_process = DataProcessing()
        
        list_chat_coeff, list_chien_coeff = [], []
        list_chat_ord, list_chien_ord = [], []
        list_chat_2019, list_chien_2019 = [], []
        list_chat_mse, list_chien_mse = [], []
        list_chat_rmse, list_chien_rmse = [], []
        list_chat_mae, list_chien_mae = [], []
        list_chat_r2, list_chien_r2 = [], []
        list_chat_evs, list_chien_evs = [], []
        list_chat_medae, list_chien_medae = [], []
        
        for i in range(len(data)): 
            list_years = ['2017', '2018', '2019']
            list_chat = [data['CHAT_2017'][i], data['CHAT_2018'][i], data['CHAT_2020'][i]]
            list_chien = [data['CHIEN_2017'][i], data['CHIEN_2018'][i], data['CHIEN_2020'][i]]
            
            # Pour les chats :
            coeff, ordonnee, predict_chat = data_process.apply_regression(list_years, list_chat)
            
            ## Ajout aux listes
            list_chat_coeff.append(coeff), list_chat_ord.append(ordonnee), list_chat_2019.append(predict_chat)
               
            # Pour les chiens :
            coeff, ordonnee, predict_chien = data_process.apply_regression(list_years, list_chien)
                        
            ## Ajout aux listes
            list_chien_coeff.append(coeff), list_chien_ord.append(ordonnee), list_chien_2019.append(predict_chien)
              
        result = data.copy()
        
        result['coef_chat'] = list_chat_coeff
        result['coef_chien'] = list_chien_coeff
        
        result['ordonnees_chat'] = list_chat_ord
        result['ordonnees_chien'] = list_chien_ord  
          
        result['CHAT_2019_PREDICT'] = list_chat_2019
        result['CHIEN_2019_PREDICT'] = list_chien_2019
        
        with pd.ExcelWriter(f'./result/result.xlsx', engine='openpyxl', mode="w") as writer:
            result.to_excel(writer, header=True, index=True) 

    def pipeline_running(self, data_source):        
        start = time.time()
        DataPipeline().run_process(data_source)
        end = time.time()
        print('{:.4f} s'.format(end - start))
        
if __name__ == "__main__":
    data_source = "./data/data_join.geojson"

    pipeline = DataPipeline()
    pipeline.pipeline_running(data_source)