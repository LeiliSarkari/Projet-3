**Enoncé Projet 3** : Voici une vision globale des étapes techniques pour un premier moyen de transport (par exemple les bornes de vélos en libre-service) : Récupération par fichier plat des données unitaires et affichage d’une carte Collecte via une API des données unitaires et mise à jour de la carte Automatisation toutes les 15 minutes de la collecte via API et stockage de cet historique dans une base de données

Suivant l’heure du jour, vous verrez une sorte de “respiration”, en fonction des lieux de sorties, des quartiers de bureaux ou des zones résidentielles.

Puis, vous pourrez ajouter un second moyen de transport, etc...

Suivant les villes il y a des bornes de comptage sur les voies cyclables, et de trafic sur les voies rapides Concernant les transports en commun, vous obtiendrez la fréquence de desserte, mais vous n’avez généralement pas accès aux données sur la fréquentation (nombre d’usagers)

Pour automatiser la collecte, votre formateur vous donnera accès à une quête nommée “schedule”. Dans un process réel en entreprise, vous pourriez payer pour un serveur cloud. Ici, un ordinateur d’un membre du groupe pourra jouer le rôle de serveur, il faudra le faire tourner pendant minimum 48h pour avoir des résultats représentatifs.

###**Objectifs**
1. Automatiser la collecte de données afin de se constituer un historique de données (ex: connaître toutes les heures de passage des bus de la ligne 141 sur une journée, ou une période donnée)
2. Le faire d'abord pour un moyen de transport donné, puis étendre notre dataset en retenant d'autres moyens de transport
3. Visualiser par heure du jour via une map la densité du trafic

NB : Collecte toutes les 15min et pendant 48h (via library Schedule)
###**Outils / liens**
- Sujet projet -> https://docs.google.com/presentation/d/13m9KBTcyNWn0Bv6fEPkDp-HQaLsto65_/edit#slide=id.p12
- Trello -> https://trello.com/b/kNibrPGH/wcs-projet-3-transports-publics?completedInviteSignup=1
- Dashboard -> https://docs.google.com/spreadsheets/d/15u4nUEHV6tNLWb_1wRapEoc3HafNCN4fytNcaT5PSZg/edit?gid=352383392#gid=352383392
- Présentation Canva -> https://www.canva.com/design/DAGQ6fyaRf0/SmQXtXBCCdvfe561mOtEOQ/edit

###**Ressources**

- https://cartovista.com/fr/real-time-data-cartovista-api-python/ -> exemple
- https://appsource.microsoft.com/en-us/product/power-bi-visuals/wa104380981?tab=overview -> lien cartes dynamiques (cf Aïssata)
- https://medium.com/search?q=carte+interactive+avec+python+et+folium -> autre source pour cartes dynamiques

----------
- https://help.opendatasoft.com/apis/ods-explore-v2/#section/Introduction/Base-URL -> méthodologie requêtage API
- https://www.data.gouv.fr/fr/organizations/rennes-metropole-en-acces-libre-1/#/datasets -> comptage vélos et piétons ++ état du trafic en temps réel
- https://www.data.gouv.fr/fr/datasets/prochains-passages-des-lignes-de-bus-du-reseau-star-en-temps-reel/ -> fichiers csv avec passages bus temps réel
- https://data.explore.star.fr/explore/dataset/tco-bus-circulation-passages-tr/api/ -> datasource API pour requêter directement les données en temps réel des prochains passages des bus
- https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-bus-circulation-passages-tr/records?limit=20&refine=idligne%3A%220001%22 -> API RENNES pour le "nomcourtligne": "C1"
- https://data.rennesmetropole.fr/explore/dataset/etat-du-trafic-en-temps-reel/api/?location=11,48.1001,-1.68285&basemap=0a029a -> datasource API pour requêter directement les données en temps réel du trafic routier tps réel
- https://data.rennesmetropole.fr/api/explore/v2.1/catalog/datasets/etat-du-trafic-en-temps-reel/records?limit=20 -> API RENNES trafic routier
- https://data.rennesmetropole.fr/explore/dataset/etat-du-trafic-en-temps-reel/information/ -> dictionnaire des données API trafic routier tps réel
# **PART.1 = COLLECTE DES DONNEES --> REQUÊTAGE API ET AUTOMATISATION + COMPILATION DES DONNEES HISTORIQUES**
#LES LINKS APIS
# https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-bus-circulation-passages-tr/records?limit=20&refine=idligne%3A%220001%22
# https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-bus-circulation-passages-tr/records?limit=20&refine=idligne%3A%220002%22
# https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-bus-circulation-passages-tr/records?limit=20&refine=idligne%3A%220003%22
# https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-bus-circulation-passages-tr/records?limit=20&refine=idligne%3A%220004%22
# https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-bus-circulation-passages-tr/records?limit=20&refine=idligne%3A%220005%22
### ETAPE 0.a => FONCTION D'ENCODAGE POUR AUTOMATISER LA GENERATION DES URLS DES DIFFERENTES SOURCES API TRAFIC ROUTIER

import urllib.parse

def encode_url_elements(liste):
    # Créer une nouvelle liste pour stocker les éléments encodés
    encoded_list = []

    # Parcourir chaque élément de la liste
    for element in liste:
        # Encodage de l'élément pour qu'il soit utilisable dans une URL
        encoded_element = urllib.parse.quote(element)
        encoded_element += "%22"
        # Ajouter l'élément encodé à la liste
        encoded_list.append(encoded_element)

    return encoded_list
### ETAPE 0.b => ON GENERE AUTOMATIQUEMENT LES URLS DES DIFFERENTES SOURCE API BUS ET METRO (TÂCHE A REALISER 1 FOIS)

# URL de base - data bus ---> rem. : Au lieu de limit=20 on change par limit=-1 pour récupérer l'exhaustivité des données disponibles dans l'API propre à chaque ligne (sinon 20 par défaut)
base_url_bus = "https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-bus-circulation-passages-tr/records?limit=-1&refine=idligne%3A%22"

# URL de base - data métro (API métro)
url_metro_a = "https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-metro-circulation-passages-tr/records?limit=-1&refine=nomcourtligne%3A%22a%22"
url_metro_b = "https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-metro-circulation-passages-tr/records?limit=-1&refine=nomcourtligne%3A%22b%22"

# URL de base - data trafic routier ---> rem. : Au lieu de limit=20 on change par limit=-1 pour récupérer l'exhaustivité des données disponibles dans l'API propre à chaque ligne (sinon 20 par défaut)
base_url_routier = "https://data.rennesmetropole.fr/api/explore/v2.1/catalog/datasets/etat-du-trafic-en-temps-reel/records?limit=-1&refine=denomination%3A%22"

# liste des dénominations possibles pour le réseau routier
list_denomination_routier = ["Rond-point","Route nationale 136","Route départementale 34","Route départementale 29","Rue de Rennes","Route nationale 24","Rue de Fougères","Avenue Général George S. Patton",
                     "Rue du Général de Gaulle","Boulevard des Alliés","Rue Jules Vallès","Rue de Vern","Avenue Henri Fréville","Rue de Saint-Malo","Rue de Châteaugiron","Boulevard de Verdun",
                     "Rue Nationale","Boulevard Villebois-Mareuil","Rue de Nantes","Rue de Montfort","Avenue Général Leclerc","Rue de Lorient","Route départementale 288","Route départementale 82",
                     "Boulevard de Vitré","Avenue de la Libération","Boulevard Eugène Pottier","Boulevard Jean Mermoz","Route départementale 837","Route départementale 463","Rue de Vezin","Rue des Loges",
                     "Avenue de Rochester","Route départementale 173","Avenue Professeur Charles Foulon","Boulevard Georges Clemenceau","Boulevard Léon Grimault","Boulevard de Metz","Rue de Saint-Brieuc",
                     "Rue du Clos Courtel","Avenue Charles et Raymonde Tillon","Avenue Roger Dodin","Boulevard Oscar Leroux","Porte d'Alma","Route départementale 175","Avenue des Pays-Bas",
                     "Boulevard Emile Combes","Boulevard Voltaire","Porte de Bréquigny","Rue Jean Guéhenno","Rue Louis Guilloux","Rue Vaneau","Rue André Léo","Boulevard Saint-Roch","Porte de Saint-Nazaire",
                     "Route de Fougères","Avenue des Buttes de Coësmes","Porte de Maurepas","Avenue Gros Malhon","Avenue des Préales","Porte de Brest","Route de Saint-Malo","Rue Claude Bernard",
                     "Rue d'Antrain","Boulevard Saint-Jean-Baptiste de la Salle","Porte de Lorient","Route nationale 137","Avenue de la Touraudais","Boulevard Bertrand d'Argentré","Boulevard de Strasbourg",
                     "Route départementale 177","Avenue François Château","Boulevard Léon Bourgeois","Boulevard Volney","Porte de Beaulieu","Porte de Villejean","Route nationale 12","Avenue Andrée Viollis",
                     "Avenue des Champs Bleus","Boulevard Pierre Mendès-France","Boulevard d'Armorique","Porte de Cleunay","Route nationale 1012","Rue de Brest","Avenue de Belle Fontaine","Franklin Roosevelt",
                     "Rue Malakoff","Voie communale 1","Boulevard de l'Yser","Inconnu Voie non dénommée","Porte d'Angers","Rond-point de Ker Lann","Rond-point de Vaux","Rond-point des Sorinais",
                     "Rond-point du Château des Marais","Rond-point du Pâtis Fraux","Route départementale 224","Mail François Mitterrand","Passage Henri Fréville","Porte de Tizé"]

# Encodage de la liste des dénominations du réseau routier grâce à fonction précédemment créée
encoded_list_denomination = encode_url_elements(list_denomination_routier)

# Liste pour stocker les URLs générées via l'API Metro
urls_metro = [url_metro_a, url_metro_b]

# Liste pour stocker les URLs générées via l'API BUS
urls_bus = []

# Liste pour stocker les URLs générées via l'API RESEAU ROUTIER
urls_routier = []

# Boucle pour générer les URLs BUS de 1 à 200   ////  PARAMETRE A MODIFIER SI BESOIN /!\
for i in range(1, 201):
    line_number = str(i).zfill(4)               # Formatage du numéro de ligne avec zéro(s) devant
    url = f"{base_url_bus}{line_number}%22"     # Construire l'URL complète
    urls_bus.append(url)                        # Ajouter l'URL à la liste des urls bus soit la liste 'urls_bus'

# Boucle pour générer les URLs du réseau routier //// PARAMETRE A MODIFIER SI BESOIN /!\
for elt in encoded_list_denomination:
    url = f"{base_url_routier}{elt}"            # Construire l'URL complète
    urls_routier.append(url)                    # Ajouter l'URL à la liste

# Affichage des URLs bus - métro - routier générées
for url in urls_bus:
    print(url)

for url in urls_metro:
  print(url)

for url in urls_routier:
    print(url)

print(f"nombre d'url total API BUS: {len(urls_bus)}")
print(f"nombre d'url total API METRO: {len(urls_metro)}")
print(f"nombre d'url total API réseau routier: {len(urls_routier)}")

### ETAPE 1 => FONCTION DE REQUÊTAGE POUR UN LIEN API (SOIT POUR UNE SEULE LIGNE DE BUS, METRO OU AUTRE MOYEN DE TRANSPORT)

import requests
import pandas as pd

# # List of API endpoints
# urls = [
#     "https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-bus-circulation-passages-tr/records?limit=20&refine=idligne%3A%220001%22",
#     "https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-bus-circulation-passages-tr/records?limit=20&refine=idligne%3A%220002%22",
#     "https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-bus-circulation-passages-tr/records?limit=20&refine=idligne%3A%220003%22",
#     "https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-bus-circulation-passages-tr/records?limit=20&refine=idligne%3A%220004%22",
#     "https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/tco-bus-circulation-passages-tr/records?limit=20&refine=idligne%3A%220005%22"
# ]

## Fonction qui récupère et traite les données API pour une ligne de bus, métro, et tout autre moyen de transport (/!\ format structure Json à éventuellement ajuster !!)
## => Démarche = requêtage données format json et implémentation dans une liste processed_data + définition de la structure générale du dictionnaire imbriqué + conversion de la liste finale en dataframe

def fetch_and_process_data(url):
    response = requests.get(url).json()
    data = response['results']
    # Process data (e.g., extract relevant fields)
    processed_data = []
    for entry in data:
        processed_entry = {
            'idligne': entry.get('idligne') if entry.get('idligne') else entry.get('gml_id'), ## l’identifiant du tronçon Rennes Métropole auquel l’information est rattachée. L’identifiant peut être suffixé par « _D » et « _G » dans le cas de tronçon à double sens de circulation. Le sens de numérisation du tronçon est alors le sens « D », le sens inverse le sens « G »
            'nomcourtligne': entry.get('nomcourtligne') if entry.get('nomcourtligne') else entry.get('denomination'),
            'sens': entry.get('sens'),
            'destination': entry.get('destination') if entry.get('destination') else entry.get('predefinedlocationreference'),
            'idarret': entry.get('idarret'),
            'nomarret': entry.get('nomarret'),
            'lon': entry['coordonnees'].get('lon') if 'coordonnees' in entry else entry['geo_point_2d'].get('lon') if 'geo_point_2d' in entry else None,
            'lat': entry['coordonnees'].get('lat') if 'coordonnees' in entry else entry['geo_point_2d'].get('lat') if 'geo_point_2d' in entry else None,
            'arriveetheorique': entry.get('arriveetheorique'),
            'departtheorique': entry.get('departtheorique'),
            'arrivee': entry.get('arrivee'),
            'depart': entry.get('depart'),
            'idcourse': entry.get('idcourse'),
            'precision': entry.get('precision'),
            'visibilite': entry.get('visibilite'),
            'heureextraction': entry.get('heureextraction') if entry.get('heureextraction') else entry.get('datetime'),
            'trafficstatus': entry.get('trafficstatus'),
            'averagevehiclespeed': entry.get('averagevehiclespeed'),
            'vitesse_maxi': entry.get('vitesse_maxi')
        }
        processed_data.append(processed_entry)
    return pd.DataFrame(processed_data)

### ETAPE 2 => FONCTION DE CONCATENATION DES DONNEES APRES REQUÊTAGE DES DIFFERENTES URLS (SOIT DE L'ENSEMBLE DES LIGNES DE BUS)
### Démarche => on appelle la fonction 'fetch_and_process_data' précédemment créée en étape 1 pour chaque urls disponible et générée en étape 0

def fetch_and_concat_data(urls):
    """
    Récupère les données à partir de plusieurs URLs, les concatène et retourne un DataFrame unique.

    :param urls: Liste d'URLs à requêter
    :return: DataFrame combiné
    """
    # Initialiser une liste pour stocker les DataFrames
    dataframes = []

    # Boucle sur les URLs pour récupérer et traiter les données
    for i, url in enumerate(urls):
        try:
            # Récupérer et traiter les données de chaque URL via la fonction fetch_and_process_data créée à l'étape 1
            df = fetch_and_process_data(url)
            dataframes.append(df)
        except Exception as e:
            print(f"Erreur lors de la requête pour l'URL {url}: {e}")

    # Concaténer tous les DataFrames dans un DataFrame unique
    combined_df = pd.concat(dataframes, ignore_index=True)

    return combined_df
### ETAPE 3 => FONCTION DE SAUVEGARDE DE TTES LES DONNEES PRECEDEMMENT REQUÊTEES DANS UN FICHIER .CSV OU .TXT POUR CONSTITUER UN HISTORIQUE DE DONNEES (obj : 48h de données requêtées toutes les 15min)
### Démarche => on génère un horodatage qu'on implémente dans le nom du fichier .csv de requêtage et stockage, en prévision du stockage répété pendant 48h pour distinguer l'ensemble des fichiers .csv
### /!\ FONCTIONNE EN LOCAL UNIQUEMENT AUTREMENT DIT SUR JUPYTER NOTEBOOK

from datetime import datetime

def save_dataframe_with_timestamp(df, file_format='csv', transport='bus'): ## où df est le dataframe à sauvegarder et file_format='csv' le format du fichier de sortie généré // format .txt possible

    # Générer un horodatage sous forme de string
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Format YYYYMMDD_HHMMSS

    # Nom du fichier avec horodatage
    file_name = f"{transport}_data_{timestamp}" ## où le moyen de transport est à la main de l'utilisateur (paramètre fonction) et intégré au nom du fichier avec la date

    if file_format == 'csv':
        df.to_csv(f"{file_name}.csv", index=False, sep=',', encoding='utf-8')
        print(f"DataFrame sauvegardé en fichier {file_name}.csv")

    elif file_format == 'txt':
        df.to_csv(f"{file_name}.txt", index=False, sep='\t', encoding='utf-8')
        print(f"DataFrame sauvegardé en fichier {file_name}.txt")

    else:
        print(f"Format non supporté : {file_format}. Utilisez 'csv' ou 'txt'.")

# ## Test bon fonctionnement fonction
# save_dataframe_with_timestamp(combined_df, 'csv', 'bus')  # Sauvegarder en CSV avec horodatage + flag 'bus' dans le nom du fichier

### ETAPE 4 => ON TESTE L'ENCHAÎNEMENT DES FONCTIONS DES ETAPES 0,1,2,3 AVANT DE POUVOIR METTRE EN OEUVRE SCHEDULE POUR LA REPETITION DES TÂCHES 1,2,3 A INSTANT DONNE (OBJ : TOUTES LES 15 MIN PDT 48H)

import os
print(os.getcwd()) ## permet d'afficher le répertoire de travail en local où seront stockés les fichiers de sortie (données requêtées)

# df_test = fetch_and_process_data(url) ## exécution ETAPE 1 à partir de url de départ
df_combined_bus_test = fetch_and_concat_data(urls_bus) ## exécution ETAPE 2 à partir de l'ensemble des urls générées en ETAPE 0
df_combined_metro_test = fetch_and_concat_data(urls_metro) ## exécution ETAPE 2 à partir de l'ensemble des urls générées en ETAPE 0
df_combined_routier_test = fetch_and_concat_data(urls_routier) ## exécution ETAPE 2 à partir de l'ensemble des urls générées en ETAPE 0
# print(f"taille df_test: {len(df_test)}") ## à supp car pas très utile puisque donne l'info pour le dernier df dispo (i.e. dernière ligne de bus requêtée)
print(f"taille df_combined_bus_test: {len(df_combined_bus_test)}") ## permet d'indiquer la taille du df consolidé
print(f"taille df_combined_metro_test: {len(df_combined_metro_test)}") ## permet d'indiquer la taille du df consolidé
print(f"taille df_combined_routier_test: {len(df_combined_metro_test)}") ## permet d'indiquer la taille du df consolidé
# display(df_combined_test.head())
# print(df_combined_test["nomcourtligne"].unique()) ## permet de lister toutes les lignes de bus requêtées

## créer un dictionnaire avec toutes les lignes de bus requêtées et le nombre de données collectées pour chaque :
rslt_bus_dict = dict(df_combined_bus_test["nomcourtligne"].value_counts())
print(f"Nombre de données collectées par ligne de bus : {rslt_bus_dict}")
print(f"Nombre de lignes de bus collectées : {len(rslt_bus_dict)}")
# display(df_combined_test["nomcourtligne"].value_counts()) ## permet d'indiquer le volume de données requêtées pour chaque ligne de bus
save_dataframe_with_timestamp(df_combined_bus_test, 'csv', 'bus') ## permet de sauvegarder les données précédemment requêtées

## créer un dictionnaire avec toutes les lignes de metro requêtées et le nombre de données collectées pour chaque :
rslt_metro_dict = dict(df_combined_metro_test["nomcourtligne"].value_counts())
print(f"\nNombre de données collectées par ligne de metro : {rslt_metro_dict}")
print(f"Nombre de lignes de metro collectées : {len(rslt_metro_dict)}")
# display(df_combined_test["nomcourtligne"].value_counts()) ## permet d'indiquer le volume de données requêtées pour chaque ligne de bus
save_dataframe_with_timestamp(df_combined_metro_test, 'csv', 'metro') ## permet de sauvegarder les données précédemment requêtées

## créer un dictionnaire avec tout le réseau routier requêté et le nombre de données collectées pour chaque :
rslt_routier_dict = dict(df_combined_routier_test["nomcourtligne"].value_counts())
print(f"\nNombre de données collectées par réseau routier : {rslt_routier_dict}")
print(f"Nombre de réseau routier collecté : {len(rslt_routier_dict)}")
# display(df_combined_test["nomcourtligne"].value_counts()) ## permet d'indiquer le volume de données requêtées pour chaque ligne de bus
save_dataframe_with_timestamp(df_combined_routier_test, 'csv', 'routier') ## permet de sauvegarder les données précédemment requêtées

### ETAPE 5 => IMPORT DE LA LIBRAIRIE SCHEDULE POUR AUTOMATISER LA REPETITION PERIODIQUE DES TÂCHES 1,2,3 (OBJ = TOUTES LES 15 MIN PDT 48H)
### Démarche => on créée une fonction 'job' qui enchaine les 3 fonctions précédemment créées en Etapes 1,2,3 et qu'on appelle via schedule toutes les 15 min pour répétition du requêtage et stockage en .csv

!pip install schedule
import schedule
import time

def job():
    """
    Fonction de tâche à exécuter toutes les 15 minutes.
    """
    # df = fetch_and_process_data(url)
    df_combined_bus = fetch_and_concat_data(urls_bus)
    df_combined_metro = fetch_and_concat_data(urls_metro)
    df_combined_routier = fetch_and_concat_data(urls_routier)
    save_dataframe_with_timestamp(df_combined_bus, 'csv', 'bus')
    save_dataframe_with_timestamp(df_combined_metro, 'csv', 'metro')
    save_dataframe_with_timestamp(df_combined_routier, 'csv', 'routier')

# Planifier la tâche toutes les 15 minutes
schedule.every(15).minutes.do(job)

# Boucle infinie pour exécuter le scheduler
while True:
    schedule.run_pending()
    time.sleep(1)  # Pause de 1 seconde avant de vérifier la prochaine tâche

### ETAPE 6 => COMPILATION DES DONNEES STOCKEES DANS FICHIERS .CSV GENERES AUX ETAPES PRECEDENTES (REQUETAGE API) DANS DATAFRAME df_combined + AJOUT COLONNE AVEC HEURE/DATE EXTRACTION API + SAUVEGARDE DF FORMAT .CSV
### /!\ Avant de compiler les données, penser à homogénéiser les noms des 3 fichiers csv metro-bus-routier qd il y a des différence de datetime (même date - heure)
### /!\ Penser à mettre à jour le path de stockage des fichiers .csv requêté précédemment dans dossier spécifique sur le drive perso
import os
import pandas as pd
from google.colab import drive

# Monter Google Drive
drive.mount('/content/drive')

# Chemin où se trouvent tous les fichiers .csv de requêtage de l'API ----- /!\ PATH A METTRE A JOUR !!!!!!!
directory_path = '/content/drive/My Drive/fichiers_metro_Bus_Routier/'

# Lister tous les fichiers dans le répertoire et filtrer uniquement sur les fichiers avec extension .csv
all_files = os.listdir(directory_path)
csv_files = [f for f in all_files if f.endswith('.csv')]

# Initialiser un dictionnaire pour stocker les DataFrames par transport
transport_dataframes = {'bus': [], 'metro': [], 'routier': []}

# Lire chaque fichier .csv, ajouter les colonnes et l'ajouter à la liste des DataFrames du bon transport
for file in csv_files:
    file_path = os.path.join(directory_path, file)
    df = pd.read_csv(file_path)

    # Extraire la date, l'heure et le transport à partir du nom du fichier
    if len(file.split('_')) == 4:
        # Exemple de nom de fichier : bus_data_20240913_015206.csv
        timestamp = file.split('_')[3].replace('.csv', '')  # '015206'
        datestamp = file.split('_')[2]  # '20240913'
        transport = file.split('_')[0]  # 'bus', 'metro', 'routier'

        # Formater la date et l'heure
        date = f"{datestamp[:4]}-{datestamp[4:6]}-{datestamp[6:]}"
        time = f"{timestamp[:2]}:{timestamp[2:4]}:{timestamp[4:]}"
        date_time_str = f"{date} {time}"

        # Convertir en format datetime64[ns, UTC]
        date_time_api = pd.to_datetime(date_time_str, format='%Y-%m-%d %H:%M:%S', utc=True)

        # Ajouter les colonnes 'date_heure_requete_API' et 'transport_API'
        df['date_heure_requete_API'] = date_time_api
        df['transport_API'] = transport

        # Ajouter le DataFrame dans la liste correspondant au type de transport
        if transport in transport_dataframes:
            transport_dataframes[transport].append(df)

        # Afficher un message pour chaque fichier
        print(f"Fichier {file} chargé avec {len(df)} lignes ====> colonne date_heure_requete_API = {date_time_api}")

# Traiter chaque type de transport
for transport, df_list in transport_dataframes.items():
    if df_list:
        # Concaténer tous les fichiers du même type de transport
        df_combined = pd.concat(df_list, ignore_index=True)

        # Utiliser la date du dernier fichier pour le nom final
        date_file = df_combined['date_heure_requete_API'].max().strftime('%Y%m%d')

        # Enregistrer le DataFrame combiné au format CSV
        output_csv_path = f'/content/drive/My Drive/Projet3/df_combined_{transport}_{date_file}.csv'
        df_combined.to_csv(output_csv_path, index=False)

        # Afficher le nombre de fichiers concaténés et de lignes du DataFrame combiné
        print(f"\nNombre total de fichiers CSV chargés pour '{transport}' : {len(df_list)}")
        print(f"Nombre total de lignes du dataframe '{transport}' combiné : {len(df_combined)}")
        print(f"DataFrame '{transport}' combiné sauvegardé dans: {output_csv_path}")


# **PART.2 = TRAITEMENT DES DONNEES --> CHARGEMENT + EXPLORATION + NETTOYAGE**
### ETAPE 1 => CHARGEMENT DES DONNEES // ** To KEEP **
# Lire le fichier / import des données [suppose d'enrgistrer le/les fichiers .csv dans le drive perso de chacune directement à la racine]
### /!\ PATH A METTRE A JOUR !!

import pandas as pd
from google.colab import drive
drive.mount('/content/drive')
df_bus = pd.read_csv("/content/drive/My Drive/Projet3/df_combined_bus_20240925.csv") ################## /!\ à mettre à jour
df_metro = pd.read_csv("/content/drive/My Drive/Projet3/df_combined_metro_20240925.csv") ############## /!\ à mettre à jour
df_routier = pd.read_csv("/content/drive/My Drive/Projet3/df_combined_routier_20240925.csv") ########## /!\ à mettre à jour

print(f"taille df_bus:     {len(df_bus)}")
print(f"taille df_metro:   {len(df_metro)}")
print(f"taille df_routier: {len(df_routier)}")
### EXPLORATION DES DONNEES POUR CHECK et MIEUX COMPRENDRE LE CONTENU DU DATAFRAME ET LA FAISABILITE DES KPI
##  CHECK => Bonne exécution de la compilation + création + typage colonne -> df_bus
## concl = colonnes bien créées mais format 'date_heure_requete_API' non correcte donc à convertir avec les autres colonne date/heure (arrivee, depart, arriveeth, etc)

df_bus.info()
### CHECK => Bonne exécution de la compilation + création colonne -> df_metro
df_metro.info()
### CHECK => Bonne exécution de la compilation + création colonne -> df_routier
df_routier.info()
### CHECK BUS => unicité des valeurs des colonnes du df
print(f"Nombre de 'idligne' unique: {df_bus.idligne.nunique()}")
print(f"Nombre de 'nomcourtligne' unique: {df_bus.nomcourtligne.nunique()}")
print(f"Nombre de 'destination' unique: {df_bus.destination.nunique()}")
print(f"Nombre de 'idarret' unique: {df_bus.idarret.nunique()}")
print(f"Nombre de 'nomarret' unique: {df_bus.nomarret.nunique()}")
print(f"Nombre de coordonnées 'lon' unique: {df_bus.lon.nunique()}")
print(f"Nombre de coordonnées 'lat' unique: {df_bus.lat.nunique()}")
print(f"Nombre de 'idcourse' unique: {df_bus.idcourse.nunique()}")
print(f"Nombre de 'date_heure_requete_API' unique: {df_bus.date_heure_requete_API.nunique()}")
print(f"Nombre de 'heureextraction' unique: {df_bus.heureextraction.nunique()}")
print(f"Nombre de 'transport_API' unique: {df_bus.transport_API.nunique()} ---> {df_bus.transport_API.unique()}")
print(f"Depart min:            {df_bus.depart.min()} // Depart max:            {df_bus.depart.max()}")
print(f"Arrivee min:           {df_bus.arrivee.min()} // Arrivee max:           {df_bus.arrivee.max()}")
print(f"Arrivee theorique min: {df_bus.arriveetheorique.min()} // Arrivee theorique max: {df_bus.arriveetheorique.max()}")
### CHECK BUS => Bonne exécution de la compilation + création colonne
nb_occur_csv_bus = df_bus['date_heure_requete_API'].value_counts()
sorted_value_counts_bus = nb_occur_csv_bus.sort_index(ascending=True) # Convertir l'index de value_counts (les dates) en datetime si nécessaire, puis trier par ordre chronologique
display(sorted_value_counts_bus) # Afficher le résultat trié par ordre chronologique
### CHECK METRO => unicité des valeurs des colonnes du df
print(f"Nombre de 'idligne' unique: {df_metro.idligne.nunique()}")
print(f"Nombre de 'nomcourtligne' unique: {df_metro.nomcourtligne.nunique()}")
print(f"Nombre de 'destination' unique: {df_metro.destination.nunique()}")
print(f"Nombre de 'idarret' unique: {df_metro.idarret.nunique()}")
print(f"Nombre de 'nomarret' unique: {df_metro.nomarret.nunique()}")
print(f"Nombre de coordonnées 'lon' unique: {df_metro.lon.nunique()}")
print(f"Nombre de coordonnées 'lat' unique: {df_metro.lat.nunique()}")
print(f"Nombre de 'idcourse' unique: {df_metro.idcourse.nunique()}")
print(f"Nombre de 'date_heure_requete_API' unique: {df_metro.date_heure_requete_API.nunique()}")
print(f"Nombre de 'heureextraction' unique: {df_metro.heureextraction.nunique()}")
print(f"Nombre de 'transport_API' unique: {df_metro.transport_API.nunique()} ---> {df_metro.transport_API.unique()}")

### CHECK METRO => Bonne exécution de la compilation + création colonne
nb_occur_csv_metro = df_metro['date_heure_requete_API'].value_counts()
sorted_value_counts_metro = nb_occur_csv_metro.sort_index(ascending=True) # Convertir l'index de value_counts (les dates) en datetime si nécessaire, puis trier par ordre chronologique
display(sorted_value_counts_metro) # Afficher le résultat trié par ordre chronologique
### CHECK ROUTIER => unicité des valeurs des colonnes du df
print(f"Nombre de 'idligne' unique: {df_routier.idligne.nunique()}")
print(f"Nombre de 'nomcourtligne' unique: {df_routier.nomcourtligne.nunique()}")
print(f"Nombre de 'destination' unique: {df_routier.destination.nunique()}")
print(f"Nombre de coordonnées 'lon' unique: {df_routier.lon.nunique()}")
print(f"Nombre de coordonnées 'lat' unique: {df_routier.lat.nunique()}")
print(f"Nombre de 'idcourse' unique: {df_routier.idcourse.nunique()}")
print(f"Nombre de 'date_heure_requete_API' unique: {df_routier.date_heure_requete_API.nunique()}")
print(f"Nombre de 'heureextraction' unique: {df_routier.heureextraction.nunique()}")
print(f"Nombre de 'trafficstatus' unique: {df_routier.trafficstatus.nunique()} ---> {list(df_routier.trafficstatus.unique())}")
print(f"Nombre de 'averagevehiclespeed' unique: {df_routier.averagevehiclespeed.nunique()} ---> min:{df_routier.averagevehiclespeed.min()} // max:{df_routier.averagevehiclespeed.max()}")
print(f"Nombre de 'vitesse_maxi' unique: {df_routier.vitesse_maxi.nunique()} ---> min:{df_routier.vitesse_maxi.min()} // max:{df_routier.vitesse_maxi.max()}")
print(f"Nombre de 'transport_API' unique: {df_routier.transport_API.nunique()} ---> {df_routier.transport_API.unique()}")
### Zoom densité traffic =>
print(df_routier.trafficstatus.value_counts())
df_routier.groupby(by='date_heure_requete_API').trafficstatus.value_counts()
# Histogramme en barre pour afficher l'état du traffic routier au fil des heures // pourrait être intéressant de faire des zoom en fonction du lieu

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Filtrer le DataFrame pour exclure la catégorie 'unknown' dans 'trafficstatus'
df_filtered = df_routier[df_routier['trafficstatus'] != 'unknown']

# Regrouper par 'date_heure_requete_API' et 'trafficstatus', puis compter les occurrences
df_count = df_filtered.groupby(['date_heure_requete_API', 'trafficstatus']).size().reset_index(name='count')

# Créer un dictionnaire pour les couleurs personnalisées
palette = {'freeFlow': 'green', 'heavy': 'orange', 'congested': 'red'}

# Créer le graphique en barres avec Seaborn
plt.figure(figsize=(15, 8))
sns.barplot(x='date_heure_requete_API', y='count', hue='trafficstatus', data=df_count, palette=palette)

# Ajuster l'affichage
plt.title('Etat du traffic routier au fil du temps')
plt.xlabel('Date et Heure de Requête API')
plt.ylabel('Nombre')
plt.xticks(rotation=90)  # Rotation des étiquettes en abscisse pour améliorer la lisibilité
plt.legend(title='Etat du traffic (trafficstatus hors unknown)')

# Afficher le graphique
plt.tight_layout()
plt.show()

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Filtrer le DataFrame pour exclure la catégorie 'unknown' dans 'trafficstatus'
df_filtered = df_routier[df_routier['trafficstatus'] != 'unknown']

# Regrouper par 'date_heure_requete_API' et 'trafficstatus', puis compter les occurrences
df_count = df_filtered.groupby(['date_heure_requete_API', 'trafficstatus']).size().reset_index(name='count')

# Créer un dictionnaire pour les couleurs personnalisées
palette = {'freeFlow': 'green', 'heavy': 'orange', 'congested': 'red'}

# Créer le graphique en courbe avec Seaborn
plt.figure(figsize=(15, 8))
sns.lineplot(x='date_heure_requete_API', y='count', hue='trafficstatus', data=df_count, palette=palette, marker='o')

# Identifier et annoter les pics pour chaque état du trafic
for status in df_count['trafficstatus'].unique():
    df_status = df_count[df_count['trafficstatus'] == status]
    max_idx = df_status['count'].idxmax()  # Trouver l'index de la valeur maximale
    max_time = df_status.loc[max_idx, 'date_heure_requete_API']
    max_count = df_status.loc[max_idx, 'count']

    # Extraire uniquement l'heure du champ 'date_heure_requete_API'
    max_time_str = pd.to_datetime(max_time).strftime('%H:%M')

    # Annoter le pic avec l'heure correspondante
    plt.text(max_time, max_count, f'{max_time_str}', color=palette[status], ha='center', va='bottom')

# Ajuster l'affichage
plt.title('État du trafic routier au fil du temps')
plt.xlabel('Date et Heure de Requête API')
plt.ylabel('Nombre')
plt.xticks(rotation=90)  # Rotation des étiquettes en abscisse pour améliorer la lisibilité
plt.legend(title='État du trafic (trafficstatus hors unknown)')

# Afficher le graphique
plt.tight_layout()
plt.show()


df_routier.info()
df_bus.info()
df_metro.info()
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

# Supprimer les colonnes inutiles
df_metro_clean = df_metro.drop(columns=['arriveetheorique', 'departtheorique', 'idcourse', 'visibilite', 'heureextraction', 'trafficstatus', 'averagevehiclespeed', 'vitesse_maxi'])

# Convertir les colonnes d'horaires en datetime
df_metro_clean['arrivee'] = pd.to_datetime(df_metro_clean['arrivee'], errors='coerce')
df_metro_clean['depart'] = pd.to_datetime(df_metro_clean['depart'], errors='coerce')

df_metro.head()
# Utiliser plotly pour une carte interactive des arrêts
fig = px.scatter_mapbox(df_metro_clean, lat="lat", lon="lon", hover_name="nomarret",
                        color="nomcourtligne", zoom=10, height=600)
fig.update_layout(mapbox_style="open-street-map")
fig.show()
# Compter les stations uniques par ligne
stations_par_ligne = df_metro_clean.groupby('nomcourtligne')['nomarret'].nunique().reset_index(name='nombre_stations')

# Trier les lignes par le nombre de stations
stations_par_ligne = stations_par_ligne.sort_values(by='nombre_stations', ascending=False)

# Visualisation du nombre de stations par ligne
plt.figure(figsize=(10, 6))
plt.bar(stations_par_ligne['nomcourtligne'], stations_par_ligne['nombre_stations'], color='teal')
plt.title('Nombre de Stations par Ligne de Métro')
plt.xlabel('Ligne de Métro')
plt.ylabel('Nombre de Stations')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Assurez-vous que la colonne 'arrivee' est au format datetime
df_metro_clean['arrivee'] = pd.to_datetime(df_metro_clean['arrivee'], errors='coerce')

# Extraire l'heure d'arrivée
df_metro_clean['heure_arrivee'] = df_metro_clean['arrivee'].dt.hour

# Compter les trajets uniques par ligne et par heure
trajets_par_ligne_et_heure = df_metro_clean.groupby(['nomcourtligne', 'heure_arrivee', 'nomarret', 'destination']).size().reset_index(name='nombre_trajets')

# Compter le nombre total de trajets par ligne et par heure
nombre_trajets_par_ligne_et_heure = trajets_par_ligne_et_heure.groupby(['nomcourtligne', 'heure_arrivee'])['nombre_trajets'].sum().reset_index()

# Configurer le style de Seaborn
sns.set(style="whitegrid")

# Créer une figure
plt.figure(figsize=(14, 8))

# Définir les paramètres pour les barres côte à côte
bar_width = 0.15  # Largeur des barres
x = range(len(nombre_trajets_par_ligne_et_heure['heure_arrivee'].unique()))  # Positions sur l'axe x

# Créer des barres pour chaque ligne
for i, ligne in enumerate(nombre_trajets_par_ligne_et_heure['nomcourtligne'].unique()):
    subset = nombre_trajets_par_ligne_et_heure[nombre_trajets_par_ligne_et_heure['nomcourtligne'] == ligne]
    plt.bar([h + i * bar_width for h in subset['heure_arrivee']], subset['nombre_trajets'], width=bar_width, label=ligne)

# Ajouter des titres et des labels
plt.title('Nombre de Trajets Uniques par Ligne de Métro (Par Heure d\'Arrivée)', fontsize=16)
plt.xlabel('Heure d\'Arrivée', fontsize=14)
plt.ylabel('Nombre de Trajets', fontsize=14)

# Afficher toutes les heures de 0 à 23 sur l'axe x
plt.xticks(range(0, 24), [f'{hour}:00' for hour in range(24)], rotation=45)
plt.legend(title='Ligne de Métro', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)

# Afficher le graphique
plt.tight_layout()
plt.show()

import folium
import pandas as pd

# Créer une carte centrée sur une localisation moyenne
map_center = [df_metro_clean['lat'].mean(), df_metro_clean['lon'].mean()]
metro_map = folium.Map(location=map_center, zoom_start=12)

# Ajouter les arrêts de métro sur la carte
for idx, row in df_metro_clean.iterrows():
    folium.Marker(
        location=[row['lat'], row['lon']],
        popup=f"{row['nomarret']} - Ligne {row['nomcourtligne']}",
        icon=folium.Icon(color='blue')
    ).add_to(metro_map)

# Afficher la carte
metro_map.save("map_metro_stops.html")
metro_map

df_routier.info()
df_bus[['depart','arrivee','arriveetheorique','date_heure_requete_API']]
print(f"depart           => min: {df_bus.depart.min()} || max: {df_bus.depart.max()}")
print(f"arrivee          => min: {df_bus.arrivee.min()} || max: {df_bus.arrivee.max()}")
print(f"arriveetheorique => min: {df_bus.arriveetheorique.min()} || max: {df_bus.arriveetheorique.max()}")
print(f"date API         => min: {df_bus.date_heure_requete_API.min()} || max: {df_bus.date_heure_requete_API.max()}\n")

df_time_outlier = df_bus[['idligne','idcourse','depart','arrivee','arriveetheorique','date_heure_requete_API']].copy()

# Conversion date au format datetime
df_time_outlier['depart'] = pd.to_datetime(df_time_outlier['depart'], errors='coerce')
df_time_outlier['arrivee'] = pd.to_datetime(df_time_outlier['arrivee'], errors='coerce')
df_time_outlier['arriveetheorique'] = pd.to_datetime(df_time_outlier['arriveetheorique'], errors='coerce')
df_time_outlier['date_heure_requete_API'] = pd.to_datetime(df_time_outlier['date_heure_requete_API'], errors='coerce')

mask_time_outlier = (df_time_outlier.depart < df_time_outlier.date_heure_requete_API.min()) | (df_time_outlier.arrivee < df_time_outlier.date_heure_requete_API.min()) | (df_time_outlier.arriveetheorique < df_time_outlier.date_heure_requete_API.min()) | (df_time_outlier.depart > df_time_outlier.date_heure_requete_API.max()) | (df_time_outlier.arrivee > df_time_outlier.date_heure_requete_API.max()) | (df_time_outlier.arriveetheorique > df_time_outlier.date_heure_requete_API.max())
df_time_outlier = df_time_outlier.loc[mask_time_outlier]

print(f"nombre outlier: {len(df_time_outlier)} versus nombre total: {len(df_bus)}")
print(f"nombre idligne unique dans df_outlier: {df_time_outlier.idligne.nunique()} vs {df_bus.idligne.nunique()}")
print(f"nombre idcourse unique dans df_outlier: {df_time_outlier.idcourse.nunique()} vs {df_bus.idcourse.nunique()}")

### ETAPE 2 => CONCATENATION DES DATAFRAME POUR LES DIFFERENTS MOYENS DE TRANSPORT REQUETES (BUS, METRO, ROUTIER) /// ** TO KEEP **
##  Rem: Possible car même format de colonne (homogénéisation des intitulés) --> facilite le traitement des données (nettoyage, ajout colonne, analyses exploratoires)

df_all_transport = pd.concat([df_bus, df_metro, df_routier], ignore_index=True)
df_all_transport.info()
### Identification des doublons => En supprimant la colonne 'heureextraction' de l'analyse on trouve 20229 doublons soit quasi 50% du dataframe initial
### A priori pertinent de supprimer les doublons pour ne pas double compter les trajet dans une vision globale --> pour KPI par ex
### En revanche, pour des analyse par tranche horaire, il faut faire attention de supprimer les doublons de la tranche horaire dite uniquement --> pour KPI par ex
print(f"Nombre de doublons dans le dataframe original (toutes colonnes prises en compte): {df_all_transport.duplicated().sum()}")
print(f"Nombre de doublons dans le dataframe hors colonne 'heureextraction': {df_all_transport.drop(columns=['heureextraction','date_heure_requete_API']).duplicated().sum()}")

### Comparaison des colonnes 'arriveetheorique' / 'departtheorique' et 'arrivee' / 'depart'
# mask_theorique = (df_combined['arriveetheorique'] != df_combined['arrivee']) | (df_combined['departtheorique'] != df_combined['depart'])
print(f"Rappel nombre total de lignes du dataframe: {len(df_all_transport)}")
mask_theorique = (df_all_transport.arriveetheorique == df_all_transport.departtheorique)
print(f"Nombre de lignes dont les valeurs des colonnes 'departtheorique' et 'arriveetheorique' sont identiques: {len(df_all_transport.loc[mask_theorique])}")
mask_reel = (df_all_transport.arrivee == df_all_transport.depart)
print(f"Nombre de lignes dont les valeurs des colonnes 'depart' et 'arrivee' sont identiques: {len(df_all_transport.loc[mask_reel])}")
# display(df_combined.loc[~mask_theorique])
mask_retard = (df_all_transport.arrivee == df_all_transport.arriveetheorique)
print(f"Nombre de lignes du df dont les valeurs des colonnes 'arrivee' et 'arriveetheorique' sont identiques / pas de retard: {len(df_all_transport.loc[mask_retard])}")
print(f"Autrement dit, nombre de lignes du df pour lesquelles un retard est identifié: {len(df_all_transport) - len(df_all_transport.loc[mask_retard])}")
### ETAPE 3 => NETTOYAGE DES DONNEES --> 1. REMPLACEMENT DES VALEURS MANQUANTES PAR 0 /// ** TO KEEP **
df_all_transport.fillna(0, inplace=True)
df_all_transport.info()
### ETAPE 3 => NETTOYAGE DES DONNEES --> 2. CONVERSION DES COLONNES DATE AU FORMAT DATETIME /// ** TO KEEP **
### Rem : Gestion des erreurs via errors='coerce' (ex. 0 sur les lignes flagguées 'metro' et 'routier' car info manquante)

## Conversion des colonnes 'depart','arrivee','departtheorique','arriveetheorique', 'date_heure_requete_API' en format datetime avec pandas
df_all_transport['depart'] = pd.to_datetime(df_all_transport['depart'], errors='coerce')
df_all_transport['arrivee'] = pd.to_datetime(df_all_transport['arrivee'], errors='coerce')
df_all_transport['departtheorique'] = pd.to_datetime(df_all_transport['departtheorique'], errors='coerce')
df_all_transport['arriveetheorique'] = pd.to_datetime(df_all_transport['arriveetheorique'], errors='coerce')
df_all_transport['date_heure_requete_API'] = pd.to_datetime(df_all_transport['date_heure_requete_API'], errors='coerce')
df_all_transport.info()
df_all_transport.date_heure_requete_API.unique()
### ETAPE 3 => NETTOYAGE DES DONNEES --> 3. VERIFICATION QUE LES COORDONNEES GEOGRAPHIQUES SONT BIEN UNIQUES PAR NOM D'ARRET

# Groupby sur 'idarret' et 'nomarret', puis compter le nombre de combinaisons uniques de 'lon' et 'lat'
df_check_unique_coords = df_all_transport.groupby(['idarret', 'nomarret'])[['lon', 'lat']].nunique().reset_index()

# Filtrer les arrêts qui ont plus d'une combinaison unique de coordonnées
df_non_unique_coords = df_check_unique_coords[df_check_unique_coords[['lon', 'lat']].max(axis=1) > 1]

# Afficher le résultat
display(df_non_unique_coords)

### ETAPE 3 => NETTOYAGE DES DONNEES --> 4. DETERMINATION DES OUTLIERS // fonction

def detect_outliers_iqr(df, column):
    # Calcul des quartiles
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1

    # Définition des bornes pour les valeurs aberrantes
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    print(f"Borne inférieure : {lower_bound}, Borne supérieure : {upper_bound}")

    # # Filtrer les outliers
    # outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]

    return lower_bound, upper_bound
### ETAPE 3 => NETTOYAGE DES DONNEES --> Zoom variables 'arrivee' et 'arriveetheorique' /// cohérence des valeurs en delta (retard)
## Subset retard qui représente la différence entre l'heure réelle d'arrivée 'arrivee' et l'heure théorique 'arriveetheorique'
### + étude de la distribution de la variable 'retard' pour vérifier si valeurs aberrantes

df_all_retard = df_all_transport.copy()
df_all_retard['retard'] = (df_all_retard['arrivee'] - df_all_retard['arriveetheorique']).dt.total_seconds() / 60  # en minutes
print(f"retard min en minutes: {df_all_retard['retard'].min()}")
print(f"retard Q1: {df_all_retard['retard'].quantile(0.25)}")
print(f"retard Q2 ou mediane: {df_all_retard['retard'].quantile(0.5)}")
print(f"retard Q3: {df_all_retard['retard'].quantile(0.75)}")
print(f"retard max en minutes: {df_all_retard['retard'].max()}")
print(f"retard moyen en minutes: {df_all_retard['retard'].mean()}")

## Zoom retard > 70 ou retard < 0
print(f"\nNombre de retard >= 70 minutes: {len(df_all_retard[df_all_retard['retard'] >= 70])}")
print(f"Nombre de retard > 0 et < 70: {len(df_all_retard[(df_all_retard['retard'] > 0) & (df_all_retard['retard'] < 70)])}")
print(f"Nombre de non retard, ie = 0: {len(df_all_retard[df_all_retard['retard'] == 0])}")
print(f"Nombre d'arrivées anticipées < 0: {len(df_all_retard[df_all_retard['retard'] < 0])}")

## Utilisation fonction détection outliers pour déterminer les bornes
print(f"\nBornes outliers: {detect_outliers_iqr(df_all_retard, 'retard')}")

## Affichage des lignes min et max
print(df_all_retard.retard.idxmin())
print(df_all_retard.retard.idxmax())
pd.set_option('display.max_columns', None)
display(df_all_retard.iloc[[df_all_retard.retard.idxmin(),df_all_retard.retard.idxmax()]])

## boxplot retard pour observer la distribution
import seaborn as sns
import matplotlib.pyplot as plt
sns.boxplot(x=df_all_retard['retard'])
plt.show()

mask_check = (df_all_transport.idcourse == 268440518.0) # & (df_all_transport.destination == "Betton | Chevaigné") & (df_all_transport.nomarret == "Les Gayeulles")
df_all_transport.loc[mask_check,['idcourse','destination','nomarret','depart','arrivee','arriveetheorique','date_heure_requete_API','precision']].sort_values(by=['date_heure_requete_API','depart'])

### ETAPE 4 => CREATION SUBSET AGGREGE PAR IDCOURSE -> OBJECTIF = CALCULER NOMBRE ARRETS, DUREE TRAJET, RETARD OBSERVES AU GLOBAL POUR CHAQUE TRAJET
## Obj : Déterminer la durée des trajets => duree_trajet = différence entre l'heure d'arrivée max 'arrivee' et l'heure de départ min 'depart' par idcourse
## Obj : Déterminer les retard par trajet => retard = différence entre l'heure d'arrivée max 'arrivee' et l'heure d'arrivée théorique max 'arriveetheorique' par idcourse
## + étude de la distribution de la variable 'duree_trajet' pour vérifier si valeurs aberrantes

# Calculer le départ minimum, l'arrivée maximale et le nombre d'arrêts par idcourse x date-heure de requetage API
df_trajet = df_all_transport.groupby(['idcourse','date_heure_requete_API']).agg(
    depart_min=pd.NamedAgg(column='depart', aggfunc='min'),
    arrivee_max=pd.NamedAgg(column='arrivee', aggfunc='max'),
    arrivee_theo_max=pd.NamedAgg(column='arriveetheorique', aggfunc='max'),
    nb_arret=pd.NamedAgg(column='arriveetheorique', aggfunc='count')
).reset_index()

# Ajout colonne 'duree_trajet' pour calculer la durée du trajet (arrivee_max - depart_min) en minutes
df_trajet['duree_trajet'] = (df_trajet['arrivee_max'] - df_trajet['depart_min']).dt.total_seconds() / 60 ## si supp ".dt.total_seconds() / 60" -> format day, hh:mm:ss -- mais pb affichage qd delta negatif car il considère day=-1

# Ajout colonne 'retard' pour calculer le retard total du trajet (arrivee_max - arrivee_theo_max) en minutes
df_trajet['retard'] = (df_trajet['arrivee_max'] - df_trajet['arrivee_theo_max']).dt.total_seconds() / 60 ## si supp ".dt.total_seconds() / 60" -> format day, hh:mm:ss

# Fusionner avec les colonnes idligne et nomcourtligne en évitant les doublons
df_trajet = pd.merge(
    df_trajet,
    df_all_transport[['idcourse', 'idligne', 'nomcourtligne', 'destination']].drop_duplicates('idcourse'),
    on='idcourse',
    how='left'
)

# ENREGISTREMENT DU SUBSET AGGREGE AU FORMAT CSV POUR VISUALISATION DANS POWER BI ==>
output_csv_path = f'/content/drive/My Drive/Projet3/df_trajet_agg.csv'
df_trajet.to_csv(output_csv_path, index=False)


# # Suppression lignes index=83, 911, 761, 645, 8, 52, 170 identifiées comme valeur aberrante -> durée min = -1 days +23:51:00
# mask_duree_trajet_nulle = df_trajet.loc[df_trajet['duree_trajet']<='0 days 00:00:00']
# print(len(mask_duree_trajet_nulle))
# df_trajet = df_trajet(~mask_duree_trajet_nulle)

### Affichage des résultats
# df_trajet.info()
# display(df_trajet[['idcourse', 'idligne', 'nomcourtligne', 'destination', 'depart_min', 'arrivee_max', 'duree_trajet', 'retard']].sort_values(by=['idligne','depart_min'], ascending=True))
# display(df_trajet.groupby(by='idligne').duree_trajet.mean())
print(f"taille dataframe: {len(df_trajet)}\n")
print(f"nombre idcourse uniques: {df_trajet.idcourse.nunique()}")
# print(f"nombre date/heure requete API uniques: {df_trajet.date_heure_requete_API.nunique()}\n")
print(f"duree moyenne de l'ensemble des trajets: {df_trajet.duree_trajet.mean().round()}")
print(f"duree max observée sur l'ensemble des trajets: {df_trajet.duree_trajet.max().round()}")
print(f"duree min observée sur l'ensemble des trajets: {df_trajet.duree_trajet.min().round()}\n")
# display(df_trajet.groupby(by='idligne').retard.mean())
print(f"retard moyen de l'ensemble des trajets: {df_trajet.retard.mean().round()}")
print(f"retard max observé sur l'ensemble des trajets: {df_trajet.retard.max().round()}")
print(f"retard min observé sur l'ensemble des trajets: {df_trajet.retard.min().round()}\n")
# display(df_trajet.groupby(by='idligne').retard.mean())
print(f"nombre moyen d'arrêts sur l'ensemble des trajets: {df_trajet.nb_arret.mean().round()}")
print(f"nombre max de stations observé sur l'ensemble des trajets: {df_trajet.nb_arret.max().round()}")
print(f"nombre min de stations observé sur l'ensemble des trajets: {df_trajet.nb_arret.min().round()}\n")

## zoom valeurs aberrantes -> durée min = -1 days +23:51:00 identifiée pour index = 83 par ex mais pb. résolu en introduisant ".dt.total_seconds() / 60" dans calcul
print(df_trajet.retard.idxmin())
print(df_trajet.duree_trajet.idxmax())
display(df_trajet.iloc[df_trajet.duree_trajet.idxmax()])
print()

## durée et retard moyen observé sur le idligne souhaitée (qui est la ligne de bus pour laquelle on observe la + petite durée et + petit retard)
print(df_trajet.idligne.unique())
input_idligne = 200
mask_ligne = (df_trajet.idligne == input_idligne)
print(f"\nretard moyen ligne {input_idligne}: {df_trajet[mask_ligne].retard.mean().round()}")
print(f"duree trajet moyen ligne {input_idligne}: {df_trajet[mask_ligne].duree_trajet.mean().round()}")
print(f"nb arrêts moyen ligne {input_idligne}: {df_trajet[mask_ligne].nb_arret.mean().round()}")

df_trajet.info()
mask_check = (df_all_transport.idcourse == 268440438.0) # & (df_all_transport.destination == "Betton | Chevaigné") & (df_all_transport.nomarret == "Les Gayeulles")
df_all_transport.loc[mask_check,['idcourse','idligne','destination','nomarret','depart','arrivee','arriveetheorique','date_heure_requete_API','precision']].sort_values(by=['date_heure_requete_API','depart'])
# pd.set_option('display.max_columns', None)
# df_all_transport.head(25)
### ETAPE 5 => AJOUT COLONNES DANS DATAFRAME NON AGREGE AVEC LES HORAIRES DES PROCHAINS PASSAGES ET CELUI DU DERNIER PASSAGE ///  **TO KEEP**

df_timetable_all = df_all_transport.copy()
print(f"taille df avant drop: {len(df_timetable_all)}\n")
print(f"Avant drop: {list(df_timetable_all.columns)}")

# Suppression des doublons
df_timetable_all = df_timetable_all.drop(columns=['idcourse','heureextraction','date_heure_requete_API','depart','arriveetheorique',
                                                  'departtheorique','precision','visibilite','trafficstatus','averagevehiclespeed',
                                                  'vitesse_maxi']).drop_duplicates()
print(f"Après drop: {list(df_timetable_all.columns)}\n")

# Trier le DataFrame par 'idligne', 'nomarret' et 'arrivee' pour assurer l'ordre chronologique
df_timetable_all = df_timetable_all.sort_values(by=['idligne', 'nomarret', 'arrivee'])

# Créer les colonnes 'next_depart_1', 'next_depart_2' et 'next_depart_3' pour les 3 prochains passages au même arrêt
df_timetable_all['next_depart_1'] = df_timetable_all.groupby(['idligne', 'nomarret'])['arrivee'].shift(-1)
df_timetable_all['next_depart_2'] = df_timetable_all.groupby(['idligne', 'nomarret'])['arrivee'].shift(-2)
df_timetable_all['next_depart_3'] = df_timetable_all.groupby(['idligne', 'nomarret'])['arrivee'].shift(-3)

# Créer la colonne 'previous_depart' pour le précédent passage au même arrêt
df_timetable_all['previous_depart'] = df_timetable_all.groupby(['idligne', 'nomarret'])['arrivee'].shift(1)

# Créer la colonne 'waiting_time' pour calculer le temps d'attente avant le prochain passage en minutes
df_timetable_all['waiting_time'] = (df_timetable_all.next_depart_1 - df_timetable_all.arrivee).dt.total_seconds() / 60

# Afficher le DataFrame avec les nouvelles colonnes
print(f"taille df après drop: {len(df_timetable_all)}\n")
display(df_timetable_all[['idligne','nomarret','arrivee','next_depart_1','next_depart_2','next_depart_3','previous_depart','waiting_time']].head(20))

# ENREGISTREMENT DU CSV POUR VISUALISATION DANS POWER BI ==>
output_csv_path = f'/content/drive/My Drive/Projet3/df_timetable_all.csv'
df_timetable_all.to_csv(output_csv_path, index=False)

### ETAPE 6 => CREATION SUBSET AGGREGE PAR IDLIGNE AVEC LISTE DES ARRETS
### Obj : Obtenir la liste des arrêts par ligne de bus avec le nombre d'occurrences pour chaque arrêt sur la période de requêtage des données

# Groupby par 'idligne', 'idarret', 'nomarret', 'lon', 'lat' et compter les occurrences /// à voir si on garde 'destination'
df_agg_idligne = df_all_transport.groupby(['idligne','idarret', 'nomarret', 'lon', 'lat','transport_API']).size().reset_index(name='nb_occur')

# On ne garde que les bus et les métro car pas d'utilité pour le réseau routier
df_agg_idligne = df_agg_idligne[df_agg_idligne.transport_API.isin(['bus','metro'])]

# Afficher le DataFrame agrégé
display(df_agg_idligne)
print(f"nombre d'idligne distinct: {df_agg_idligne.idligne.nunique()}")
print(f"nombre d'idarret distinct: {df_agg_idligne.idarret.nunique()}")
print(f"nombre de nomarret distinct: {df_agg_idligne.nomarret.nunique()}")
print(f"nombre de lon/lat distinct: {df_agg_idligne.lon.nunique()} et {df_agg_idligne.lat.nunique()}")
## Check les nomarret qui ont plusieurs idarret associés =>

df_nomarret_idarret = df_agg_idligne.groupby('nomarret')['idarret'].nunique().reset_index()
nomarret_with_multiple_idarret = df_nomarret_idarret[df_nomarret_idarret['idarret'] > 1]
print(len(nomarret_with_multiple_idarret))
display(nomarret_with_multiple_idarret)
# **PART.3 = QUELQUES VISUELS --> ANALYSE DES DONNEES + KPI**
### KPI 1 = DUREE MOYENNE DES TRAJETS POUR CHAQUE LIGNE DE BUS

# Étape 1 : Identification du premier et le dernier arrêt de chaque 'idcourse'
# Par 'idcourse', on récupère l'heure de départ la plus tôt et l'heure d'arrivée la plus tard
df_trajet_2 = df_all_transport.groupby('idcourse').agg(
    depart_min=('depart', 'min'),  # Le premier départ du trajet = heure min de colonne 'depart'
    arrivee_max=('arrivee', 'max'),  # La dernière arrivée du trajet = heure max de colonne 'arrivee'
    nomcourtligne=('nomcourtligne', 'first')  # Le nom court de la ligne
).reset_index()

# Étape 2 : Calculer la durée du trajet en secondes
df_trajet_2['duree_trajet_secondes'] = (df_trajet_2['arrivee_max'] - df_trajet_2['depart_min']).dt.total_seconds()

# Étape 3 : Calculer la durée moyenne des trajets pour chaque ligne
# Regroupement par 'idligne' ou 'nomcourtligne' pour calculer la moyenne
# Exemple avec 'idligne'
duree_moyenne_par_ligne = df_trajet_2.groupby('nomcourtligne')['duree_trajet_secondes'].mean().reset_index()

# Afficher les résultats --> durée en minutes
duree_moyenne_par_ligne['duree_trajet_minutes'] = round(duree_moyenne_par_ligne['duree_trajet_secondes'] / 60,0)
display(duree_moyenne_par_ligne.sort_values(by='duree_trajet_minutes', ascending=False))
# Si vous souhaitez utiliser 'nomcourtligne' au lieu de 'idligne', remplacez simplement 'idligne' par 'nomcourtligne'
# duree_moyenne_par_ligne = df_trajet.groupby('nomcourtligne')['duree_trajet'].mean().reset_index()

df_trajet_2.sort_values(by=['duree_trajet_secondes'],ascending=False)
print(f"min: {df_trajet_2.duree_trajet_secondes.min()}")
print(f"Q1: {df_trajet_2.duree_trajet_secondes.quantile(0.25)}")
print(f"Q2: {df_trajet_2.duree_trajet_secondes.quantile(0.5)}")
print(f"Q3: {df_trajet_2.duree_trajet_secondes.quantile(0.75)}")
print(f"max: {df_trajet_2.duree_trajet_secondes.max()}")
print(f"moyenne: {df_trajet_2.duree_trajet_secondes.mean()}")
print(f"\nrappel nombre de trajet totaux: {len(df_trajet)}")
print(f"nombre de trajet donc la durée est supérieure à 2h30 (valeurs aberrantes): {len(df_trajet_2[df_trajet_2.duree_trajet_secondes>=9000])}")
print(f"nombre de trajet donc la durée est comprise entre 1h et 2h30: {len(df_trajet_2[(df_trajet_2.duree_trajet_secondes<9000) & (df_trajet_2.duree_trajet_secondes>=3600)])}")
print(f"nombre de trajet donc la durée est comprise entre 30min et 1h: {len(df_trajet_2[(df_trajet_2.duree_trajet_secondes<3600) & (df_trajet_2.duree_trajet_secondes>=1800)])}")
print(f"nombre de trajet donc la durée est comprise entre 0 et 30min: {len(df_trajet_2[(df_trajet_2.duree_trajet_secondes<1800) & (df_trajet_2.duree_trajet_secondes>=0)])}")
print(f"nombre de trajet donc la durée est négative (valeurs aberrantes): {len(df_trajet_2[df_trajet_2.duree_trajet_secondes<0])}")
### KPI 2 = NOMBRE DE TRAJETS PAR LIGNE DE BUS

# Compter le nombre de trajets (idcourse) réalisés par ligne
nombre_trajets_par_ligne = df_all_transport.groupby('nomcourtligne')['idcourse'].nunique().reset_index()
nombre_trajets_par_ligne.rename(columns={'idcourse': 'nombre_trajets'}, inplace=True)

# Fusionner les deux DataFrames (duree_moyenne_par_ligne et nombre_trajets_par_ligne)
df_resultat = pd.merge(duree_moyenne_par_ligne, nombre_trajets_par_ligne, on='nomcourtligne')

# Afficher le DataFrame final avec la durée moyenne et le nombre de trajets
display(df_resultat.sort_values(by='duree_trajet_minutes', ascending=False))
### VISUELS ASSOCIES AUX KPIS 1 ET 2 // garder le top 10

import matplotlib.pyplot as plt
import seaborn as sns

# Créer une figure
fig, ax1 = plt.subplots(figsize=(12, 7))

# Style des graphiques
sns.set(style="whitegrid")

# 1. Premier axe (axe gauche) : Durée moyenne des trajets en minutes (Bar Chart)
color = 'tab:blue'
ax1.set_xlabel('Ligne de bus (idligne)', fontsize=12)
ax1.set_ylabel('Durée moyenne des trajets (minutes)', color=color)
sns.barplot(x='nomcourtligne', y='duree_trajet_minutes', data=df_resultat, palette='Blues_d', ax=ax1)
ax1.tick_params(axis='y', labelcolor=color)
ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45, ha='right', fontsize=8)  # Rotation de 60° et alignement à droite pour lisibilité

# 2. Deuxième axe (axe droit) : Nombre de trajets uniques (Scatter plot / Point Chart)
ax2 = ax1.twinx()  # Créer un deuxième axe qui partage le même axe x
color = 'tab:green'
ax2.set_ylabel('Nombre de trajets uniques', color=color)  # On ajoute un label pour l'axe y
ax2.plot(df_resultat['nomcourtligne'], df_resultat['nombre_trajets'], color=color, marker='o', linestyle='None', markersize=5)  # Diminuer la taille des points à 5
ax2.tick_params(axis='y', labelcolor=color)

# Ajouter un titre
# plt.title('Durée moyenne des trajets et nombre de trajets par ligne de bus')

# Ajuster les marges pour éviter le chevauchement
fig.tight_layout()

# Afficher le graphique
plt.show()

### VISUEL KPI - SUR L'ENSEMBLE DE L'HISTORIQUE, REPARTITION DES TRAJETS PAR DUREE
import matplotlib.pyplot as plt

# Calcul des catégories de trajets
categories_duree = [
    'Trajets > 2h30 (valeurs aberrantes)',
    'Trajets entre 1h et 2h30',
    'Trajets entre 30min et 1h',
    'Trajets entre 0 et 30min',
    'Trajets avec durée négative (valeurs aberrantes)'
]

# Nombre de trajets par catégorie
nombre_trajets = [
    len(df_trajet_2[df_trajet_2.duree_trajet_secondes >= 9000]),
    len(df_trajet_2[(df_trajet_2.duree_trajet_secondes < 9000) & (df_trajet_2.duree_trajet_secondes >= 3600)]),
    len(df_trajet_2[(df_trajet_2.duree_trajet_secondes < 3600) & (df_trajet_2.duree_trajet_secondes >= 1800)]),
    len(df_trajet_2[(df_trajet_2.duree_trajet_secondes < 1800) & (df_trajet_2.duree_trajet_secondes >= 0)]),
    len(df_trajet_2[df_trajet_2.duree_trajet_secondes < 0])
]

# Création du diagramme en barres
plt.figure(figsize=(10, 6))
plt.barh(categories_duree, nombre_trajets, color='skyblue')

# Titre et labels
plt.title('Répartition des trajets selon la durée (37005 trajets)', fontsize=14)
plt.xlabel('Nombre de trajets', fontsize=12)
# plt.ylabel('Catégories de durée de trajet', fontsize=12)

# Affichage des valeurs sur les barres
for i, v in enumerate(nombre_trajets):
    plt.text(v + 5, i, str(v), color='black', va='center')

# Afficher le graphique
plt.tight_layout()
plt.show()


### KPI 3 = NOMBRE DE TRAJETS EN COURS PAR TRANCHE HORAIRE DE REQUETAGE API // densité trafic - respiration - heures de pointes
### Démarche = Grouper par 'date_heure_requete_API' et compter le nombre d'idcourse unique
trips_per_hour = df_all_transport.groupby(pd.Grouper(key='date_heure_requete_API', freq='H'))['idcourse'].nunique() ## par heure pleine
trips_per_time_API = df_all_transport.groupby('date_heure_requete_API').agg({'idcourse': 'nunique'}).reset_index() ## par heure d'extraction de l'API et donc de génération des fichiers .csv
trips_per_time_API.rename(columns={'idcourse': 'nombre_trajets'}, inplace=True)

# # Afficher les résultats
# print(trips_per_hour)
# print(trips_per_time_API)

# Check : on vérifie qu'on retrouve bien le total de idcourse unique ==> /!\ non pas possible car on regarde idcourse.nunique() par tranche horaire et non au global
print(f"Vérification somme trips_per_hour = {trips_per_hour.sum()} versus {df_all_transport.idcourse.nunique()}")
print(f"Vérification somme trips_per_time_API = {trips_per_time_API.nombre_trajets.sum()} versus {df_all_transport.idcourse.nunique()}")

### VISUEL KPI 3 EN COMPARANT LE NOMBRE DE TRAJETS PAR HEURE PLEINE ET PAR HEURE DE REQUETAGE =>

import matplotlib.dates as mdates
import seaborn as sns
import matplotlib.pyplot as plt

# Calculer la limite max pour l'axe y en prenant le maximum entre les deux jeux de données
y_max = max(trips_per_hour.max(), trips_per_time_API['nombre_trajets'].max()) + 50

# Créer une figure avec deux sous-graphes côte à côte
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# --- Graphique 1 : Nombre de trajets par heure pleine (trips_per_hour) ---
ax1.plot(trips_per_hour.index, trips_per_hour.values, marker='o', color='b')
ax1.set_title('Nombre de trajets par heure pleine', fontsize=14)
# ax1.set_xlabel('Heure (HH:00)', fontsize=12)
# ax1.set_ylabel('Nombre de trajets (idcourse unique)', fontsize=12)
ax1.grid(True)

# Ajustement de l'échelle de l'axe x pour couvrir toute la plage de `trips_per_hour`
ax1.set_xlim([trips_per_hour.index.min(), trips_per_hour.index.max()])

# Formater les étiquettes de l'axe x pour n'afficher que les heures
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax1.xaxis.set_major_locator(mdates.HourLocator(interval=1))  # Afficher les heures avec un intervalle d'1h

# Rotation des étiquettes de l'axe x pour lisibilité
plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')

# Définir les mêmes limites pour l'axe y
ax1.set_ylim(0, y_max)

# --- Graphique 2 : Nombre de trajets par requête API (trips_per_time_API) ---
sns.lineplot(x='date_heure_requete_API', y='nombre_trajets', data=trips_per_time_API, marker='o', ax=ax2, color='r')
ax2.set_title('Nombre de trajets par requête API', fontsize=14)
# ax2.set_xlabel('Horodatage des requêtes API', fontsize=12)
# ax2.set_ylabel('Nombre de trajets (idcourse unique)', fontsize=12)
ax2.grid(True)

# Ajustement de l'échelle de l'axe x en utilisant les horodatages réels
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax2.xaxis.set_major_locator(mdates.HourLocator(interval=1))  # Ajuster pour afficher les heures avec un intervalle d'1h

# Rotation des labels pour plus de lisibilité
plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')

# Définir les mêmes limites pour l'axe y
ax2.set_ylim(0, y_max)

# Ajuster l'affichage pour éviter que les sous-graphes ne se chevauchent
plt.tight_layout()

# Afficher les graphiques
plt.show()
### KPI4 = IDENTIFICATION DES RETARDS

# --- Nombre de retards et temps de retard total par heure d'extraction API ---


# --- Nombre de retards et temps de retard total par heure pleine ---


# Afficher les résultats

**Nombre de courses par ligne**
df_all_transport.info()
df_all_transport['transport_API'].value_counts()
import pandas as pd
import matplotlib.pyplot as plt

courses_par_ligne = df_all_transport[['idligne','sens','nomcourtligne','idcourse','transport_API']].copy()
courses_par_ligne = courses_par_ligne.drop_duplicates()
# Calculer le nombre de courses par ligne et par type de transport
courses_par_ligne = courses_par_ligne.groupby(['nomcourtligne', 'transport_API']).size().unstack(fill_value=0)

filtres = ['bus', 'metro']
# courses_par_ligne was unstacked, moving 'transport_API' to the column index


courses_par_ligne = courses_par_ligne[courses_par_ligne['transport_API'].isin(filtres)]

# Sélectionner les 10 lignes avec le plus de courses
top_10_courses = courses_par_ligne.sum(axis=1).nlargest(10)

# Filtrer les données pour les lignes sélectionnées
top_10_data = courses_par_ligne.loc[top_10_courses.index]

# Visualisation
plt.figure(figsize=(14, 7))  # Ajustez la taille de la figure
top_10_data.plot(kind='bar', stacked=True, color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'], edgecolor='black', width=0.8)

# Ajout des titres et labels avec des polices plus petites
plt.title("Top 10 Lignes de Transport par Nombre de Courses", fontsize=16, fontweight='bold')
plt.xlabel("Ligne de transport", fontsize=14)
plt.ylabel("Nombre de courses", fontsize=14)

# Ajuster l'orientation et la taille des étiquettes
plt.xticks(rotation=30, ha='right', fontsize=12)  # Rotation des étiquettes
plt.yticks(fontsize=12)
plt.legend(title="Type de transport", fontsize=12, title_fontsize='14')
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Ajuster le layout pour faire de la place pour les labels
plt.tight_layout()

# Afficher les annotations sur les barres
for index, value in enumerate(top_10_data.sum(axis=1)):
    plt.text(index, value + 1, int(value), ha='center', fontsize=10, color='black')

# Afficher le graphique
plt.show()
# 0. Faire une copie de df_all_transport
df_transport_copy = df_all_transport.copy()

# 1. Supprimer les doublons en fonction de 'heureextraction' et 'idcourse'
df_transport_copy = df_transport_copy.drop_duplicates(subset=['heureextraction', 'idcourse'])

# 2. Convertir 'arrivee' et 'arriveetheorique' en format datetime
df_transport_copy['arrivee'] = pd.to_datetime(df_transport_copy['arrivee'], errors='coerce')
df_transport_copy['arriveetheorique'] = pd.to_datetime(df_transport_copy['arriveetheorique'], errors='coerce')

# Vérification des valeurs manquantes après conversion
print("Valeurs manquantes après conversion :")
print(df_transport_copy[['arrivee', 'arriveetheorique']].isna().sum())

# 3. Calculer l'écart d'arrivée en minutes
df_transport_copy['ecart_arrivee'] = (df_transport_copy['arrivee'] - df_transport_copy['arriveetheorique']).dt.total_seconds() / 60

# 4. Définir le statut de ponctualité : à l'heure, en retard, en avance
def punctuality_status(row):
    if row['ecart_arrivee'] > 5:  # Plus de 5 minutes de retard
        return 'Late'
    elif row['ecart_arrivee'] < -5:  # Plus de 5 minutes d'avance
        return 'Early'
    else:
        return 'On Time'

df_transport_copy['punctuality_status'] = df_transport_copy.apply(punctuality_status, axis=1)

# 5. Visualiser le statut de ponctualité
plt.figure(figsize=(10, 6))
df_transport_copy['punctuality_status'].value_counts().plot(kind='bar', color=['green', 'red', 'blue'])
plt.title('Statut de Ponctualité des Transports', fontsize=16)
plt.xlabel('Statut', fontsize=14)
plt.ylabel('Nombre de Transports', fontsize=14)
plt.xticks(rotation=0)  # Rotation des étiquettes de l'axe x
plt.grid(axis='y', linestyle='--', alpha=0.7)  # Ajouter une grille pour la lisibilité
plt.tight_layout()  # Ajuster le layout pour une meilleure présentation
plt.show()
 Temps moyen d’attente à un arrêt
les 10 arrêts de bus avec le temps d'attente moyen le plus élevé
import pandas as pd
import matplotlib.pyplot as plt

# Chargement du DataFrame 'df_all_transport'
# df_all_transport = pd.read_csv('your_file.csv')  # Décommenter pour charger depuis un fichier CSV

# 0. Faire une copie de df_all_transport
df_transport_copy1 = df_all_transport.copy()

# 1. Calcul du temps d'attente moyen par arrêt et par type de transport
df_transport_copy1['temps_attente'] = df_transport_copy1['arriveetheorique'].diff().dt.total_seconds() / 60

# 2. Filtrer uniquement pour les arrêts de bus
bus_data_copy = df_transport_copy1[df_transport_copy1['transport_API'] == 'bus']

# 3. Calculer le temps d'attente moyen par arrêt
temps_moyen_attente_bus = bus_data_copy.groupby(['nomarret'])['temps_attente'].mean()

# 4. Sélectionner les 10 arrêts de bus avec le temps d'attente moyen le plus élevé
top_10_arrêts_bus = temps_moyen_attente_bus.nlargest(10)

# 5. Visualisation
plt.figure(figsize=(14, 8))
top_10_arrêts_bus.plot(kind='barh', color='#1f77b4', edgecolor='black')

# Ajout des titres et labels
plt.title("Top 10 Arrêts de Bus par Temps Moyen d'Attente", fontsize=16)
plt.xlabel("Temps d'attente moyen (minutes)", fontsize=14)
plt.ylabel("Arrêt", fontsize=14)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.grid(axis='x', linestyle='--', alpha=0.7)

# Ajuster le layout pour faire de la place pour les labels
plt.tight_layout()

# Afficher le graphique
plt.show()
Fréquence des bus par ligne
# Filtrer les données pour les bus
bus_data = df_all_transport[df_all_transport['transport_API'] == 'bus']

# Calculer le nombre de courses par ligne
courses_par_ligne_bus = bus_data['nomcourtligne'].value_counts()

# Sélectionner les 10 lignes avec le plus de courses
top_10_lignes_bus = courses_par_ligne_bus.nlargest(10)

# Créer le graphique à barres
plt.figure(figsize=(10, 6))
top_10_lignes_bus.plot(kind='bar', color='#4B9CD3', edgecolor='black')

# Titre et étiquettes
plt.title('Top 10 Lignes de Bus', fontsize=16)
plt.xlabel("Ligne de transport", fontsize=12)
plt.ylabel("Nombre de courses", fontsize=12)
plt.xticks(rotation=45)

# Ajouter les valeurs sur les barres
for j in range(len(top_10_lignes_bus)):
    plt.text(j, top_10_lignes_bus.values[j] + 5, str(top_10_lignes_bus.values[j]),
             ha='center', va='bottom', fontsize=10)

plt.grid(axis='y', linestyle='--', alpha=0.7)  # Ajouter une grille pour la lisibilité
plt.tight_layout()  # Ajuster le layout
plt.show()
# Filtrer les données pour le routier
routier_data = df_all_transport[df_all_transport['transport_API'] == 'routier']

# Calculer le nombre de courses par ligne
courses_par_ligne_routier = routier_data['nomcourtligne'].value_counts()

# Sélectionner les 10 lignes avec le plus de courses
top_10_lignes_routier = courses_par_ligne_routier.nlargest(10)

# Créer le graphique à barres
plt.figure(figsize=(10, 6))
top_10_lignes_routier.plot(kind='bar', color='#9B59B6', edgecolor='black')

# Titre et étiquettes
plt.title('Top 10 Lignes Routières', fontsize=16)
plt.xlabel("Ligne de transport", fontsize=12)
plt.ylabel("Nombre de courses", fontsize=12)
plt.xticks(rotation=45)

# Ajouter les valeurs sur les barres
for j in range(len(top_10_lignes_routier)):
    plt.text(j, top_10_lignes_routier.values[j] + 5, str(top_10_lignes_routier.values[j]),
             ha='center', va='bottom', fontsize=10)

plt.grid(axis='y', linestyle='--', alpha=0.7)  # Ajouter une grille pour la lisibilité
plt.tight_layout()  # Ajuster le layout
plt.show()
import pandas as pd
import matplotlib.pyplot as plt

# Supposons que df_all_transport est déjà défini
# Filtrer les données pour le métro
metro_data = df_all_transport[df_all_transport['transport_API'] == 'metro']

# Calculer le nombre de courses par ligne
courses_par_ligne_metro = metro_data['nomcourtligne'].value_counts()

# Sélectionner les 10 lignes avec le plus de courses
top_10_lignes_metro = courses_par_ligne_metro.nlargest(10)

# Créer le graphique à barres
plt.figure(figsize=(10, 6))
top_10_lignes_metro.plot(kind='bar', color='#3498DB', edgecolor='black')  # Couleur bleu

# Titre et étiquettes
plt.title(' Lignes de Métro', fontsize=16)
plt.xlabel("Ligne de transport", fontsize=12)
plt.ylabel("Nombre de courses", fontsize=12)
plt.xticks(rotation=45)

# Ajouter les valeurs sur les barres
for j in range(len(top_10_lignes_metro)):
    plt.text(j, top_10_lignes_metro.values[j] + 5, str(top_10_lignes_metro.values[j]),
             ha='center', va='bottom', fontsize=10)

plt.grid(axis='y', linestyle='--', alpha=0.7)  # Ajouter une grille pour la lisibilité
plt.tight_layout()  # Ajuster le layout
plt.show()
 comparer la fréquence des métros
 analyser le nombre de transports en service par période de la journée (Nuit, Matin, Après-midi, Soir)


# Suppression des doublons sur les colonnes autres que 'heureextraction', 'date_heure_requete_API', 'date_depart'
df_retards_unique = df_retards.drop_duplicates(subset=[col for col in df_retards.columns if col not in ['heureextraction', 'date_heure_requete_API', 'date_depart']])

# Calculer le retard maximal par 'idcourse'
df_retards_max = df_retards_unique.groupby('idcourse').agg(
    retard_max=('retard', 'max'),  # Retard maximum par trajet
    date_heure_requete_API=('date_heure_requete_API', 'first')  # Garder la première heure de requête pour ce trajet
).reset_index()

# --- Nombre de retards et temps de retard total par heure d'extraction API ---
retards_par_API = df_retards_max.groupby('date_heure_requete_API').agg(
    nombre_retards=('idcourse', 'count'),  # Nombre de trajets avec retard
    temps_retard_total=('retard_max', 'sum')  # Somme des retards maximaux par trajet
).reset_index()

# --- Nombre de retards et temps de retard total par heure pleine ---
retards_par_heure = df_retards_max.groupby(pd.Grouper(key='date_heure_requete_API', freq='H')).agg(
    nombre_retards=('idcourse', 'count'),  # Nombre de trajets avec retard
    temps_retard_total=('retard_max', 'sum')  # Somme des retards maximaux par trajet
).reset_index()

# Afficher les résultats corrigés
print(retards_par_API)
print(retards_par_heure)
#display(df_retards_max)
print(df_retards_max.idcourse.nunique())
df_retards_max.sort_values(by=['retard_max'], ascending=False)
# df_combined.loc[df_combined['idcourse'==268437029]]

# Filtre du df_combined sur le critère idcourse == 268437029
mask_idcourse = df_retards['idcourse']==268437029
df_retards[mask_idcourse].sort_values(by=['retard'],ascending=False)
#  DataFrame nommé `df_all_transport`
# Initialiser un dictionnaire pour stocker les value_counts
value_counts_dict = {}

# Obtenir les value_counts pour chaque colonne
for column in df_all_transport.columns:
    value_counts_dict[column] = df_all_transport[column].value_counts(dropna=False)

# Afficher les value_counts pour chaque colonne
for column, value_counts in value_counts_dict.items():
    print(f"Value counts for column '{column}':")
    print(value_counts)
    print("\n")
# **PART.4 = A REVOIR / COMPLETER**
# Créer une palette de couleurs pour chaque type de transport
colors = {'bus': 'blue', 'routier': 'green', 'metro': 'red'}

# Créer une figure
plt.figure(figsize=(10, 6))

# Tracer les points pour chaque type de transport avec des couleurs différentes
for transport_type in df_all_transport['transport_API'].unique():
    subset = df_all_transport[df_all_transport['transport_API'] == transport_type]
    plt.scatter(subset['lon'], subset['lat'],
                alpha=0.5,
                s=10,
                label=transport_type,
                color=colors[transport_type])

# Ajouter le titre et les labels
plt.title('Geographical Distribution of Stops by Transport Type')
plt.xlabel('Longitude')
plt.ylabel('Latitude')

# Ajouter une légende pour différencier les types de transports
plt.legend(title='Type de transport')

# Afficher la carte
plt.show()
import pandas as pd
import matplotlib.pyplot as plt

# Supposons que df_all_transport est déjà défini

# Créer une palette de couleurs et de marqueurs pour chaque type de transport
colors = {'bus': 'blue', 'routier': 'green', 'metro': 'red'}
markers = {'bus': 'o', 'routier': 's', 'metro': '^'}  # Cercles pour bus, carrés pour routier, triangles pour métro

# Créer une figure
plt.figure(figsize=(10, 6))

# Tracer les points pour chaque type de transport avec des couleurs et marqueurs différents
for transport_type in df_all_transport['transport_API'].unique():
    subset = df_all_transport[df_all_transport['transport_API'] == transport_type]
    plt.scatter(subset['lon'], subset['lat'],
                alpha=0.5,
                s=50,  # Taille des marqueurs
                label=transport_type,
                color=colors[transport_type],
                marker=markers[transport_type])  # Marqueur spécifique pour chaque type

# Ajouter le titre et les labels
plt.title('Distribution Géographique des Arrêts par Type de Transport')
plt.xlabel('Longitude')
plt.ylabel('Latitude')

# Ajouter une légende pour différencier les types de transports
plt.legend(title='Type de transport')

# Afficher la carte
plt.show()
# Calculate the frequency per stop (number of trips) from df_all_transport
df_frequentation = df_all_transport.drop_duplicates().groupby(['idarret', 'nomarret', 'lat', 'lon']).size().reset_index(name='frequentation')

# Creating the map centered on Rennes
m = folium.Map(location=[48.1173, -1.6778], zoom_start=13)

# Define the function to choose color based on frequentation
def get_color(frequentation):
    if frequentation > 20:
        return 'red'
    elif frequentation > 10:
        return 'orange'
    else:
        return 'green'

# Add markers to the map
for _, row in df_frequentation.iterrows():
    folium.CircleMarker(
        location=(row['lat'], row['lon']),
        radius=8,  # Size of the marker
        color=get_color(row['frequentation']),
        fill=True,
        fill_color=get_color(row['frequentation']),
        fill_opacity=0.6,
        popup=f"{row['nomarret']}: {row['frequentation']} trajets",
    ).add_to(m)

# Display the map in a Jupyter Notebook or Google Colab
m
import pandas as pd
import folium
from folium.plugins import MarkerCluster

# Filtrage des colonnes importantes et suppression des doublons
df_unique_arrets = df_all_transport[['lat', 'lon', 'nomarret']].drop_duplicates()

# Créer une carte centrée sur la moyenne des coordonnées
m = folium.Map(location=[df_unique_arrets['lat'].mean(), df_unique_arrets['lon'].mean()], zoom_start=12)

# Ajouter un cluster de marqueurs pour améliorer l'affichage
marker_cluster = MarkerCluster().add_to(m)

# Ajouter des marqueurs dans le cluster
for i, row in df_unique_arrets.iterrows():
    folium.Marker(
        location=[row['lat'], row['lon']],
        popup=f"Arrêt: {row['nomarret']}"
    ).add_to(marker_cluster)

# Sauvegarder la carte dans un fichier HTML
m.save("carte_interactive_arrets_bus.html")
m
import pandas as pd
import folium

# Exemple : chargement du DataFrame 'df_all_transport'
# df_all_transport = pd.read_csv('your_file.csv') # Charger le DataFrame s'il est dans un fichier CSV

# Filtrer par type de transport
bus_data1 = df_all_transport[df_all_transport['transport_API'] == 'bus'].sample(1000)  # Échantillon de 100 arrêts de bus
routier_data1 = df_all_transport[df_all_transport['transport_API'] == 'routier'].sample(1000)  # Échantillon de 100 arrêts routiers
metro_data1 = df_all_transport[df_all_transport['transport_API'] == 'metro'].sample(1000)  # Échantillon de 100 arrêts de métro

Créer une carte avec des arrêts de bus, routier, et métro avec des couleurs différentes
# Créer une carte centrée sur les coordonnées moyennes
map_center = [df_all_transport['lat'].mean(), df_all_transport['lon'].mean()]
transport_map = folium.Map(location=map_center, zoom_start=12)

# Couleurs pour chaque type de transport
colors = {
    'bus': 'blue',
    'routier': 'green',
    'metro': 'red'
}

# Ajouter les arrêts de bus
for _, row in bus_data.iterrows():
    folium.Marker(
        location=[row['lat'], row['lon']],
        popup=row['nomarret'],
        icon=folium.Icon(color=colors['bus'], icon='bus', prefix='fa')  # icône pour le bus
    ).add_to(transport_map)

# Ajouter les arrêts routiers
for _, row in routier_data.iterrows():
    folium.Marker(
        location=[row['lat'], row['lon']],
        popup=row['nomarret'],
        icon=folium.Icon(color=colors['routier'], icon='truck', prefix='fa')  # icône pour routier
    ).add_to(transport_map)

# Ajouter les arrêts de métro
for _, row in metro_data.iterrows():
    folium.Marker(
        location=[row['lat'], row['lon']],
        popup=row['nomarret'],
        icon=folium.Icon(color=colors['metro'], icon='subway', prefix='fa')  # icône pour le métro
    ).add_to(transport_map)

# Sauvegarder la carte
transport_map
2. Carte de la vitesse moyenne des véhicules
from folium.plugins import HeatMap

# Combiner les échantillons
sample_data = pd.concat([bus_data1, routier_data1, metro_data1])
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

# Créer une carte centrée sur la position moyenne des arrêts dans l'échantillon
speed_sample_map = folium.Map(location=[sample_data['lat'].mean(), sample_data['lon'].mean()], zoom_start=12)

# Normalisation des vitesses pour une échelle de couleurs dans l'échantillon
speed_max_sample = sample_data['averagevehiclespeed'].max()
speed_min_sample = sample_data['averagevehiclespeed'].min()
norm_sample = plt.Normalize(vmin=speed_min_sample, vmax=speed_max_sample)

# Ajouter les arrêts avec une couleur correspondant à la vitesse moyenne
for _, row in sample_data.iterrows():
    speed = row['averagevehiclespeed']
    color = plt.cm.viridis(norm_sample(speed))  # Palette de couleurs viridis
    color_hex = mcolors.rgb2hex(color)          # Conversion en hexadécimal

    folium.CircleMarker(
        location=[row['lat'], row['lon']],
        radius=5,
        color=color_hex,
        fill=True,
        fill_color=color_hex,
        fill_opacity=0.7,
        popup=f"Vitesse Moyenne: {speed:.2f} km/h"
    ).add_to(speed_sample_map)

# Sauvegarder la carte
speed_sample_map
import pandas as pd
import folium

# Filtrer les données pour Rennes (si votre DataFrame contient des données pour plusieurs villes, par exemple)
# Exemple : df_rennes_transport = df_all_transport[df_all_transport['city'] == 'Rennes']

# Compter le nombre d'arrêts par ligne à Rennes
arret_par_ligne = df_all_transport.groupby('nomcourtligne').size().reset_index(name='nombre_arrets')

# Créer une carte centrée sur Rennes (coordonnées de Rennes : latitude 48.1173, longitude -1.6778)
m = folium.Map(location=[48.1173, -1.6778], zoom_start=12)

# Ajouter des cercles proportionnels au nombre d'arrêts par ligne
for _, row in arret_par_ligne.iterrows():
    ligne_data = df_all_transport[df_all_transport['nomcourtligne'] == row['nomcourtligne']].iloc[0]
    folium.CircleMarker(
        location=[ligne_data['lat'], ligne_data['lon']],  # Positionner le cercle
        radius=row['nombre_arrets'] / 100,  # Taille proportionnelle au nombre d'arrêts
        popup=f"Ligne : {row['nomcourtligne']}, Nombre d'arrêts : {row['nombre_arrets']}",
        color='blue',
        fill=True,
        fill_color='blue'
    ).add_to(m)

# Si vous utilisez Jupyter Notebook
m
from folium.plugins import HeatMap

# Carte de chaleur (Heatmap) des arrêts de bus
map_heatmap = folium.Map(location=[df_all_transport['lat'].mean(), df_all_transport['lon'].mean()], zoom_start=12)

# Ajout de la heatmap
heat_data = [[row['lat'], row['lon']] for index, row in df_all_transport.iterrows()]
HeatMap(heat_data).add_to(map_heatmap)

map_heatmap
from folium.features import DivIcon

# Exemple de cluster pour les arrêts de bus
marker_cluster = MarkerCluster().add_to(m)

for index, row in df_metro.iterrows():
    # Créer un numéro pour chaque arrêt
    number = str(index + 1)  # Vous pouvez ajuster ce numéro comme vous le souhaitez

    # Ajouter un marqueur avec un numéro
    folium.Marker(
        location=[row['lat'], row['lon']],
        popup=f"Ligne : {row['nomcourtligne']}, Arrêt : {row['nomarret']}",
        icon=DivIcon(
            icon_size=(30, 30),
            icon_anchor=(15, 15),
            html=f'<div style="font-size: 12px; color: blue;"><b>{number}</b></div>'
        )
    ).add_to(marker_cluster)

# Afficher la carte
m

# **Exploration des données**

# Find duplicate rows
duplicate_rows = df_all_transport[df_all_transport.duplicated()]

# Count the number of duplicate rows
num_duplicates = duplicate_rows.shape[0]
print(f"Number of duplicate rows: {num_duplicates}")

# Optionally, you can view the duplicates themselves
print("Duplicate rows:")
display(duplicate_rows)
# **Traitement des données**
from folium.plugins import MarkerCluster

# Exemple de cluster pour les arrêts de bus
marker_cluster = MarkerCluster().add_to(m)

for index, row in df_bus.iterrows():
    folium.Marker(
        location=[row['lat'], row['lon']],
        popup=f"Ligne : {row['nomcourtligne']}, Arrêt : {row['nomarret']}",
        icon=folium.Icon(color="blue", icon="bus", prefix='fa')
    ).add_to(marker_cluster)

m
# Filtrage des colonnes importantes et suppression des doublons
df_unique_arrets = df_all_transport[['lat', 'lon', 'nomarret']].drop_duplicates()

# Vérification des premières lignes pour s'assurer que les données sont correctes
display(df_unique_arrets.head())

df_unique_arrets.info()
# **Map**
import pandas as pd
import folium
# Create a Folium map centered around the mean latitude and longitude of the combined DataFrame
map_center = [df_all_transport['lat'].mean(), df_all_transport['lon'].mean()]
m = folium.Map(location=map_center, zoom_start=12)

# Add markers to the map
for idx, row in df_all_transport.iterrows():
    if pd.notna(row['lat']) and pd.notna(row['lon']):
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=f"Line: {row['idligne']}<br>Stop: {row['nomarret']}",
            icon=folium.Icon(color='blue', icon='info-sign')
        ).add_to(m)

# Save the map to an HTML file
m.save('bus_stops_map.html')

# If running in a Jupyter notebook, you can display the map inline
m
df_unique_arrets = all_df[['lat', 'lon', 'nomarret']].drop_duplicates()

# Créer une carte centrée sur la moyenne des coordonnées
m = folium.Map(location=[df_unique_arrets['lat'].mean(), df_unique_arrets['lon'].mean()], zoom_start=12)

# Ajouter un marqueur pour chaque arrêt de bus unique
for i, row in df_unique_arrets.iterrows():
    folium.Marker(
        location=[row['lat'], row['lon']],
        popup=row['nomarret']
    ).add_to(m)

# Sauvegarder la carte dans un fichier HTML
m.save("carte_tous_arrets.html")

# Afficher la carte dans un notebook si nécessaire
m
from google.colab import files

# Save the map to an HTML file
map_path = 'carte_all.html'
m.save(map_path)

# Download the HTML file
files.download(map_path)
# Save the map to an HTML file
m.save('carte_all.html')
import pandas as pd
import folium
from folium.plugins import AntPath

# Assumons que all_df contient les colonnes 'idbus', 'lat', 'lon', et 'heureextraction'

# Créer une carte centrée sur la moyenne des coordonnées
m = folium.Map(location=[df_all_transport['lat'].mean(), df_all_transport['lon'].mean()], zoom_start=13)

# Ajouter les trajets des bus
for bus_id, group in df_all_transport.groupby('idbus'):
    # Trier les points par heure d'extraction pour chaque bus
    group = group.sort_values('heureextraction')
    # Convertir les points en liste de tuples pour AntPath
    points = list(zip(group['lat'], group['lon']))

    # Ajouter un trajet avec une ligne continue
    folium.PolyLine(locations=points, color='blue', weight=2.5, opacity=0.8).add_to(m)

    # Ajouter un AntPath pour les trajets animés (optionnel)
    folium.plugins.AntPath(
        locations=points,
        dash_array=[20, 20],
        delay=1000,
        color='blue',
        pulse_color='white'
    ).add_to(m)

# Ajouter une couche de tuiles OpenStreetMap
folium.TileLayer('openstreetmap').add_to(m)

# Ajouter un contrôle de calques
folium.LayerControl().add_to(m)

# Sauvegarder la carte dans un fichier HTML
m.save("carte_interactive_trajets_bus.html")

print("La carte avec les trajets des bus a été sauvegardée sous 'carte_interactive_trajets_bus.html'")
m







### VISUEL / CARTE AVEC LA REPRESENTATION DES ARRETS DE LA LIGNE C1 --->

# import folium
# # Créer une carte centrée sur le centre géographique des arrêts de bus C1
# moyenne_lat = df_C1['lat'].mean()
# moyenne_lon = df_C1['lon'].mean()
# carte_C1 = folium.Map(location=[moyenne_lat, moyenne_lon], zoom_start=13)

# # Ajouter des marqueurs pour chaque arrêt de bus C1
# for index, row in df_C1.iterrows():
#     folium.Marker(
#         location=[row['lat'], row['lon']],
#         popup=f"Arrêt: {row['nomarret']}<br>Destination: {row['destination']}<br>Arrivée Théorique: {row['arriveetheorique']}",
#         icon=folium.Icon(icon="bus", prefix='fa')
#     ).add_to(carte_C1)

# carte_C1
# ### VISUEL / COMPARAISON DES HORAIRES REELS VS THEORIQUES POUR IDENTIFICATION DES RETARDS POTENTIELS

# import matplotlib.pyplot as plt
# import seaborn as sns

# # Convertir les colonnes d'horaires en datetime
# df_C1['arriveetheorique'] = pd.to_datetime(df_C1['arriveetheorique'])
# df_C1['arrivee'] = pd.to_datetime(df_C1['arrivee'])

# # Calculer la différence entre l'arrivée théorique et l'arrivée réelle en minutes
# df_C1['difference'] = (df_C1['arrivee'] - df_C1['arriveetheorique']).dt.total_seconds() / 60

# # Tracer le graphique de comparaison
# plt.figure(figsize=(14, 6))
# sns.lineplot(data=df_C1, x='nomarret', y='arriveetheorique', marker='o', label='Arrivée Théorique')
# sns.lineplot(data=df_C1, x='nomarret', y='arrivee', marker='o', label='Arrivée Réelle')

# # Personnaliser le graphique
# plt.xticks(rotation=45, ha='right')
# plt.xlabel('Arrêt')
# plt.ylabel('Heure')
# plt.title('Comparaison entre les horaires prévus et réels d\'arrivée pour la ligne C1')
# plt.legend()
# plt.tight_layout()
# plt.show()
