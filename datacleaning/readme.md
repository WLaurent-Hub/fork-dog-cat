# Géocodage Base Adresse Nationale Express

## Présentation

Ce projet porte sur un **traitement des données I-CAD** sur le nombre de chat et chien par commune.

Ce pipeline comprend un traitement de données :
- Chargement des données
- Nettoyage et formatage des données 
- Géocodage des communes
- Exportation depuis un fichier Excel

Le script d'exécution `datacleaning.py` s'articule sur différents processus d'optimisation pour le temps de traitement :
- Parallélisation des tâches
- Batch processing
- Exécution asynchrone
- Instance Docker d'un serveur local de la BAN

Le script d'exécution `datagrouping.py` s'articule sur différents processus d'optimisation pour le temps de traitement :
- Parallélisation des tâches avec du multithreading
- Batch processing
- Excécution séquentielle avec du chunk processing

## Dépendances

Le projet est basé sur les modules :
- Pandas **v2.1.1**
- Requests **v2.31.0**
- HTTPX **v0.25.0**
- Asyncio **v1.5.8**
- Cachetools **v5.3.1**
- Openpyxl **v3.1.2**
- Joblib **v1.3.2**
- Rapidfuzz **v3.4.0**

Disponible dans le fichier `requirements.txt`
```bash
pip install -r requirements.txt
```

## Python version

Le projet est basé sur la version python : **v3.12.0**

Disponible dans le fichier `runtime.txt`

## Installation (**IMPORTANT**)

Ce projet est basé sur une instance docker addok

### Pré-requis:
- [Docker CE 1.10+ / Docker Compose 1.10+](https://docs.docker.com/engine/install/)
- Programme de dézippage : `unzip`
- Programme de téléchargement de fichiers : `wget`
- Windows PowerShell (distribution Windows)

### Installation de l'instance docker

Dans un premier temps, se placer dans notre dossier de travail `datacleaning`
```bash
git clone https://github.com/WLaurent-Hub/dog-cat.git
cd dog-cat/datacleaning
```

#### Télécharger des données de la BAN
```bash
wget https://adresse.data.gouv.fr/data/ban/adresses/latest/addok/addok-france-bundle.zip
```

#### Dézippage des données (ligne de commande ou manuellement)
```bash
mkdir ban_data
unzip -d ban_data addok-france-bundle.zip
```

#### Placer le fichier docker-compose dans le dossier de travail
```bash
mv docker-compose.yml datacleaning/
```

#### Voici la structure attendue

<pre>
📦datacleaning
 ┣ 📜datacleaning.py
 ┣ 📜readme.md
 ┣ 📜requirements.txt
 ┣ 📜runtime.txt
 ┣ 📂data
 ┃ ┣ 📜dataset.xlsx
 ┣ 📂ban_data
 ┃ ┣ 📜addok.conf
 ┃ ┣ 📜addok.db
 ┃ ┗ 📜dump.rdb
 ┣ 📂logs
 ┃ ┣ 📜notfound.log
 ┃ ┣ 📜notfound.log.2023-10-21
 ┃ ┣ 📜queries.log
 ┃ ┗ 📜queries.log.2023-10-21
 ┗ 📜docker-compose.yml
</pre>

#### Démarer Docker desktop et l'instance
```bash
cd datacleaning
docker-compose up
```

**Attention** : ne pas oublier de mettre à jour WSL
```bash
wsl --install
```

#### Tester le serveur
```bash
curl "http://localhost:7878/search?q=1+rue+de+la+paix+paris"
```

source : [addok-docker](https://github.com/BaseAdresseNationale/addok-docker#pr%C3%A9-requis)

### Installation des modules Python

Avant d'exécuter le programme, installer les dépendances python en utilisant `pip` :
```bash
pip install -r requirements.txt
```
