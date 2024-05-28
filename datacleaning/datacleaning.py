import pandas as pd
import httpx
import asyncio
import time
import concurrent.futures
import cachetools

cache = cachetools.LRUCache(maxsize=1000)

# Chargement des données
class DataLoading():
    
    """
    Classe pour charger les données à partir d'un fichier Excel
    """
    
    def __init__(self, excel_source):
        
        """
        Initialisation des attributs de l'instance DataLaoding
        
        Args : 
            excel_source (str) : Chemin relatif ou absolu qui pointe vers le fichier Excel
        """

        self.excel_source = excel_source
    
    def loading_from_xlsx(self):
        
        """
        Fonction qui charge les données du fichier Excel à partir de l'attribut de l'objet.
        
        Returns : objet Pandas Dataframe de la table du fichier Excel.
        
        Exemple : 
        
            Pour charger les données depuis un fichier Excel nommé "data.xlsx" dans le répertoire courant :
            
            loader = DataLoading("data.xlsx")
            data = loader.loading_from_xlsx()
        """
        data = pd.read_excel(self.excel_source, sheet_name="2017")
        return data
    
    
# Traitement de nettoyage
class DataProcessing():
    
    """
    Classe pour le traitement des données, le formatage et le géocodage
    """
    
    API_BAN = "http://localhost:7878/search?"
    
    def __init__(self, dataset):
        
        """
        Initialisation des attributs de l'instance DataProcessing
        
        Args : 
            dataset (pd.DataFrame) : Dataframe Pandas contenant les données à exploiter
        """
        
        self.dataset = dataset
    
    def data_format(self):
        
        """
        Fonction de formatage des données en regroupant par espèce (Chien/Chat), 
        code postal et ville, et calcule de la somme des populations.

        Returns:
            pd.DataFrame: Un DataFrame Pandas avec les données formatées.

        Example:
            process = DataProcessing(self.dataset)
            group_data = process.data_format()
        """
        
        df = self.dataset.copy()
        df['VILLE'] = df['VILLE'].str.lower().str.replace("-", " ").str.replace(",", "")
        df['CODE POSTAL'] = df['CODE POSTAL'].apply(lambda x: x[1:] + '0' if x.startswith('00') else x)
        df_group = df.groupby(['ESPECE', 'CODE POSTAL', 'VILLE'])['POPULATION'].sum().reset_index()
        return df_group

    @staticmethod
    async def geocoder(cls):
        
        """
        Géocodage d'une commune à partir de son nom et de son code postal avec l'API de la BAN (Base Adresse Nationale).
        Installation d'une instance docker de l'API sur un serveur local pour une meilleure optimisation.

        Args:
            cls (dict): Dictionnaire contenant 'VILLE' et 'CODE POSTAL' à géolocaliser.

        Returns:
            tuple: Tuple contenant les coordonnées, le nom de la commune corrigé et le nom de la commune incorrect.

        Example:
            coordinates, city, wrong_city = await DataProcessing.geocoder({'VILLE': 'Paris', 'CODE POSTAL': '75000'})
        """
        
        cache_key = (cls['VILLE'], cls['CODE POSTAL'])

        # Présence et gestion du cache
        cached_result = cache.get(cache_key)
        if cached_result:
            return cached_result

        # Paramétrage pour la requête du géocodage
        params = {
            "q": f"{cls['VILLE']}, {cls['CODE POSTAL']}",
            "type": "municipality",
        }

        # Fonction d'exécution du géocodage
        def fetch_coordinates():
            try:
                # Utilisation de la bibliothèque httpx pour envoyer une requête HTTP
                with httpx.Client() as client:
                    response = client.get(DataProcessing.API_BAN, params=params)
                    if response.status_code == 200:
                        data = response.json()
                        if data and 'features' in data:
                            first_result = data['features'][0]
                            coordinate = first_result["geometry"]['coordinates']
                            ville = first_result['properties']['city']
                            result = coordinate, ville, cls['VILLE']
                            # Mise en cache du résultat pour une utilisation ultérieure
                            cache[cache_key] = result
                            return result
            except (httpx.ConnectTimeout, Exception) as e:
                pass
            
            # Return None pour indiquer l'absence de résultat
            return None, None, None

        # Utilisation d'un ThreadPoolExecutor pour exécuter la fonction fetch_coordinates de manière asynchrone
        with concurrent.futures.ThreadPoolExecutor() as executor:
            result = await asyncio.get_event_loop().run_in_executor(executor, fetch_coordinates)

        return result

# Exportation des données
class DataExporting():
    
    """
    Classe pour l'exportation des données au format Excel'
    """
    
    def __init__(self, output_file):
        
        """
        Initialisation des attributs de l'instance DataExporting
        
        Args:
            output_file (str): Chemin relatif ou absolu qui pointe vers le fichier Excel.
        """
        
        self.output_file = output_file
    
    def export_xlsx(self, df):
        
        """
        Fonction d'exportation d'un DataFrame Pandas au format Excel.

        Args:
            df (pd.DataFrame): Le DataFrame à exporter.

        Returns:
            None

        Example:
            exporter = DataExporting("output.xlsx")
            exporter.export_xlsx(dataframe_to_export)
        """
        
        df.to_excel(self.output_file, engine='openpyxl', index=False)

# Héritage des processus de traitements 
class DataPipeline(DataLoading, DataProcessing, DataExporting):
    
    """
    Classe pour gérer un pipeline :
        - chargement des données
        - traitement et formatage des données
        - géocodage des données
        - exportation des données
    """
    
    def __init__(self, excel_source, output_file):
        
        """
        Initialise une instance de la classe DataPipeline.

        Args:
            excel_source (str) : Chemin relatif ou absolu qui pointe vers le fichier Excel
            output_file (str): Chemin relatif ou absolu qui pointe vers le fichier Excel.
        """
        
        super().__init__(excel_source)
        self.output_file = output_file

    # Run du script
    async def async_pipeline_running(self):
        
        """
        Fonction asynchrone qui exécute le pipeline de traitement de données.

        Returns:
            None

        Example:
            pipeline = DataPipeline("data.xlsx", "output.xlsx")
            asyncio.run(pipeline.async_pipeline_running())
        """
        
        self.dataset = self.loading_from_xlsx()[:50]
        process = DataProcessing(self.dataset)
        group_data = process.data_format()

        # Définition de la taille de lot pour le traitement asynchrone
        batch_size = 50

        coord_list = []
        ville_list = []
        ville_2_list = []

        # Utilisation d'un client HTTP asynchrone
        async with httpx.AsyncClient() as client:
            
            # Boucle sur le traitement par lot des données
            for i in range(0, len(group_data), batch_size):
                batch = group_data[i:i+batch_size]

                tasks = []

                for _, row in batch.iterrows():
                    tasks.append(process.geocoder(row))

                # Traitement en parallèle par lot
                batch_results = await asyncio.gather(*tasks)

                 # Collecte des résultats de gécodage par lot
                for result in batch_results:
                    if result:
                        coord, ville, ville_2 = result
                        coord_list.append(coord)
                        ville_list.append(ville)
                        ville_2_list.append(ville_2)

        group_data['COORDONNEES'] = coord_list
        group_data['VILLE'] = ville_list
        group_data['VILLE_2'] = ville_2_list
        
        group_data = group_data.dropna(subset=['VILLE', 'COORDONNEES', 'VILLE_2'])
        self.export_xlsx(group_data)
        
if __name__ == "__main__":
    
    start_time = time.time()

    excel_data = "./data/dataset.xlsx"
    output_file = "test4.xlsx"
    
    pipeline = DataPipeline(excel_data, output_file)
    asyncio.run(pipeline.async_pipeline_running())
    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Le script a été exécuté en {execution_time} secondes.")
