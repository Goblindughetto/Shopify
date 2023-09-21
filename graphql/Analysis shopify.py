import os
from dotenv import load_dotenv, find_dotenv
import shopify
import json 
import time
import requests
import pandas as pd

valid_json_data = []

# Charger les variables d'environnement
load_dotenv(find_dotenv())
token = os.getenv('TOKEN')
merchant = os.getenv('MERCHANT')

# Configurer la session Shopify
api_version = '2023-04'
api_session = shopify.Session(merchant, api_version, token)
shopify.ShopifyResource.activate_session(api_session)

def start_bulk_query(query):
    bulk_operation_query = f"""
    mutation {{
        bulkOperationRunQuery(
            query: "{query}"
        ) {{
            bulkOperation {{
                id
                status
            }}
            userErrors {{
                field
                message
            }}
        }}
    }}
    """

    bulk_operation_response = shopify.GraphQL().execute(bulk_operation_query)
    bulk_operation_data = json.loads(bulk_operation_response)

    if 'data' in bulk_operation_data:
        bulk_operation = bulk_operation_data['data'].get('bulkOperationRunQuery', {}).get('bulkOperation')
        if bulk_operation:
            bulk_operation_id = bulk_operation.get('id')
            print(f"Opération bulk démarrée avec succès. ID de l'opération : {bulk_operation_id}")
            return bulk_operation_id
        else:
            print("Erreur lors du démarrage de l'opération bulk.")
    else:
        print("Erreur lors de l'exécution de la requête GraphQL :")
        print(bulk_operation_data)

    return None

def wait_for_bulk_completion(bulk_operation_id):
    while True:
        status = check_bulk_operation_status(bulk_operation_id)
        if status == 'COMPLETED':
            print("L'opération bulk est terminée.")
            break
        elif status == 'FAILED':
            print("L'opération bulk a échoué.")
            break
        else:
            print("En attente de la fin de l'opération bulk...")
            time.sleep(5)  # Attendre 5 secondes (ou ajustez selon vos besoins)

def check_bulk_operation_status(operation_id):
    graphql_query = f"""
    query CheckBulkOperationStatus {{
      node(id: "{operation_id}") {{
        ... on BulkOperation {{
          status
        }}
      }}
    }}
    """
    response = shopify.GraphQL().execute(graphql_query)
    response_data = json.loads(response)
    return response_data.get('data', {}).get('node', {}).get('status')

# Fonction pour récupérer les informations des produits de chaque commande
def get_line_item_info(line_items):
    line_item_data = []
    for item in line_items:
        line_item_info = {
            'Product Name': item.name,
            'Quantity': item.quantity,
            'Sku':item.sku,
            "OriginalUnitPrice":item.originalUnitPrice,
           

        }
        line_item_data.append(line_item_info)
    return line_item_data

# Exemple d'utilisation
query = """
{
  orders(first: 5) {
    edges {
      node {
        name
        clientIp
        createdAt
        id
        cancelReason
        confirmed
        currentTotalWeight
        totalPrice
        totalRefunded
        totalDiscounts
        customerAcceptsMarketing
        processedAt
        returnStatus
        shippingAddress {
          address1
          city
          longitude
          latitude
          id
          countryCode
          country
          coordinatesValidated
          company
          province
          provinceCode
          zip
          firstName
        }
        shippingLine {
          code
          carrierIdentifier
          price
          deliveryCategory
        }
        channel {
          name
          id
        }
        lineItems(first: 5) {
          edges {
            node {
              name
              quantity
              sku
              originalUnitPrice
            }
          }
        }
        customer {
          lifetimeDuration
          numberOfOrders
          amountSpent {
            amount
          }
          averageOrderAmount
          id
          firstName
        }
        customerJourneySummary {
          customerOrderIndex
          daysToConversion
          
      }
    }
  }
}
}

"""

bulk_operation_id = start_bulk_query(query)

# ...

if bulk_operation_id:
    wait_for_bulk_completion(bulk_operation_id)
    
    # Récupérer l'URL de téléchargement des données de l'opération bulk
    bulk_data_url_query = f"""
    query BulkDataUrl {{
      node(id: "{bulk_operation_id}") {{
        ... on BulkOperation {{
          url
        }}
      }}
    }}
    """
    bulk_data_url_response = shopify.GraphQL().execute(bulk_data_url_query)
    bulk_data_url_data = json.loads(bulk_data_url_response)

    # Vérifier si la réponse contient la clé 'data'
    if 'data' in bulk_data_url_data:
        bulk_data_url = bulk_data_url_data['data'].get('node', {}).get('url')
        # Télécharger les données depuis l'URL
        if bulk_data_url:
            response = requests.get(bulk_data_url)
            if response.status_code == 200:
                bulk_data = response.text.split("\n")  # Split data by lines
                valid_json_data = [json.loads(line) for line in bulk_data if line.strip()]
                print(valid_json_data)  # Create a list of valid JSON objects
                df = pd.DataFrame(valid_json_data)  # Create a DataFrame from the list
                df.to_csv('commandes_shopify.csv', index=False, encoding='utf-8')
                print("Export CSV terminé.")
                print(df)
            else:
                print("Erreur lors du téléchargement des données bulk.")
        else:
            print("Erreur lors de la récupération de l'URL des données bulk.")
    else:
        print("Erreur lors de la récupération de l'URL des données bulk :")
        print(bulk_data_url_data)
else:
    print("L'opération bulk n'a pas pu être démarrée.")




    
import pandas as pd
import re
import matplotlib.pyplot as plt



# Charger les données depuis un fichier CSV en spécifiant le type de données pour la colonne 19
df = pd.read_csv('commandes_shopify.csv', dtype={19: str}, encoding='utf-8')

# Créer des DataFrames vides pour les commandes et les produits
df_orders = pd.DataFrame(columns=['Order ID', 'Client IP', 'Order Date', 'Shipping Address', 'ZIP', 'Total Weight', 'TotalPrice', 'Total Discounts', 'Total Refunded', 'Customer Accepts Marketing', 'Province', 'Province Code', 'City', 'Longitude', 'Latitude', 'Country Code', 'Created At', 'Country', 'Company', 'Channel Name', 'Shipping Line Code', 'Shipping Line Carrier Identifier', 'Shipping Line Price', 'Shipping Line Delivery Category', 'Customer ID', 'Customer Number of Orders', 'Customer Amount Spent'])
df_products = pd.DataFrame(columns=['Order ID', 'Product Name', 'Quantity', 'Sku', 'OriginalUnitPrice'])

# Identifier les lignes représentant des commandes
is_order = df['name'].str.startswith('#')

# Extraire les commandes dans df_orders
df_orders['Order ID'] = df.loc[is_order, 'name'].str.slice(start=0)
df_orders['Client IP'] = df.loc[is_order, 'clientIp']
df_orders['Order Date'] = df.loc[is_order, 'createdAt']
df_orders['Total Weight'] = df.loc[is_order, 'currentTotalWeight']
df_orders['TotalPrice'] = df.loc[is_order, 'totalPrice']
df_orders['Total Refunded'] = df.loc[is_order, 'totalRefunded']
df_orders['Total Discounts'] = df.loc[is_order, 'totalDiscounts']
df_orders['Customer Accepts Marketing'] = df.loc[is_order, 'customerAcceptsMarketing']
df_orders['Created At'] = df.loc[is_order, 'createdAt']

def extract_customer_info(customer_data, info_key):
    if isinstance(customer_data, str):  # Vérifier si la valeur est une chaîne de caractères
        match = re.search(f"'{info_key}': '([^']*)'", customer_data)
        if match:
            return match.group(1)
    return None  # Retourner None si la valeur n'est pas conforme au format attendu

# Appliquer la fonction pour extraire l'ID du client et créer une nouvelle colonne "Customer ID"
df_orders['Customer ID'] = df.loc[is_order, 'customer'].apply(lambda x: extract_customer_info(x, 'id'))

# Appliquer la fonction pour extraire le nombre de commandes du client
df_orders['Customer Number of Orders'] = df.loc[is_order, 'customer'].apply(lambda x: extract_customer_info(x, 'numberOfOrders'))

# Appliquer la fonction pour extraire le montant dépensé par le client
df_orders['Customer Amount Spent'] = df.loc[is_order, 'customer'].apply(lambda x: extract_customer_info(x, 'amountSpent.amount'))



def extract_customer_id(customer_data):
    if isinstance(customer_data, str):  # Vérifier si la valeur est une chaîne de caractères
        match = re.search(r"'id': '([^']*)'", customer_data)
        if match:
            return match.group(1)
    return None  # Retourner None si la valeur n'est pas conforme au format attendu

# Appliquer la fonction pour extraire l'ID du client et créer une nouvelle colonne "Customer ID"
df_orders['Customer ID'] = df.loc[is_order, 'customer'].apply(extract_customer_id)


# Extraire les informations de l'adresse d'expédition sans utiliser JSON
df_orders['Shipping Address'] = df.loc[is_order, 'shippingAddress'].str.extract(r"'address1': '(.*?)'")
df_orders['ZIP'] = df.loc[is_order, 'shippingAddress'].str.extract(r"'zip': '(.*?)'")
df_orders['Province'] = df.loc[is_order, 'shippingAddress'].str.extract(r"'province': '(.*?)'")
df_orders['Province Code'] = df.loc[is_order, 'shippingAddress'].str.extract(r"'provinceCode': '(.*?)'")
df_orders['City'] = df.loc[is_order, 'shippingAddress'].str.extract(r"'city': '(.*?)'")
df_orders['Longitude'] = df.loc[is_order, 'shippingAddress'].str.extract(r"'longitude': (.*?),")
df_orders['Latitude'] = df.loc[is_order, 'shippingAddress'].str.extract(r"'latitude': (.*?),")
df_orders['Country Code'] = df.loc[is_order, 'shippingAddress'].str.extract(r"'countryCode': '(.*?)'")
df_orders['Country'] = df.loc[is_order, 'shippingAddress'].str.extract(r"'country': '(.*?)'")
df_orders['Company'] = df.loc[is_order, 'shippingAddress'].str.extract(r"'company': (.*?),")

# Extraire les informations de 'shippingLine' sans utiliser JSON
shipping_line_data = df.loc[is_order, 'shippingLine'].str.extract(r"'code': '(.*?)', 'carrierIdentifier': (.*?), 'price': '(.*?)', 'deliveryCategory': (.*?)$")
df_orders['Shipping Line Code'] = shipping_line_data[0]
df_orders['Shipping Line Carrier Identifier'] = shipping_line_data[1]
df_orders['Shipping Line Price'] = shipping_line_data[2]
df_orders['Shipping Line Delivery Category'] = shipping_line_data[3]

# Extraire la colonne "name" de la colonne "channel" sans utiliser JSON
channel_data = df.loc[is_order, 'channel'].str.extract(r"'name': '(.*?)', 'id': (.*?)$")
df_orders['Channel Name'] = channel_data[0]

# Répéter l'ID de commande pour les produits associés
df['Order ID'] = df['name'].where(is_order).ffill()

# Extraire les produits dans df_products
df_products['Order ID'] = df.loc[~is_order, 'Order ID']
df_products['Product Name'] = df.loc[~is_order, 'name']
df_products['Quantity'] = df.loc[~is_order, 'quantity']
df_products['Sku'] = df.loc[~is_order, 'sku']
df_products['OriginalUnitPrice'] = df.loc[~is_order, 'originalUnitPrice']



# Convertir les colonnes Latitude et Longitude en float en gérant les valeurs non numériques
df_orders['Latitude'] = pd.to_numeric(df_orders['Latitude'], errors='coerce')
df_orders['Longitude'] = pd.to_numeric(df_orders['Longitude'], errors='coerce')
df_orders['Country Code'] = df_orders['Country Code'].astype(str)
df_orders['Country'] = df_orders['Country'].astype(str)
df_orders['Company'] = df_orders['Company'].astype(str)
df_orders['Channel Name'] = df_orders['Channel Name'].astype(str)
df_orders['Shipping Line Price'] = df_orders['Shipping Line Price'].astype(float)

df_orders['Created At'] = pd.to_datetime(df_orders['Created At'])
df_orders= df_orders.drop(['Shipping Line Carrier Identifier', 'Shipping Line Code', 'Shipping Line Delivery Category', 'Province Code', 'Province'], axis=1)

df_orders.to_csv('commandes_shopify_updated.csv', index=False, encoding='utf-8')

df_orders.info()
df_orders.shape
df_products 

    
beer_club_skus = ['Beer@home', 'Beer@home NOSEND', 'BBP BEER CLUB GIFT NOSEND', 'BEERGEEK1', 'BBP BEER CLUB NO SEND', 'Beer@home NO SEND']

mask = df_products['Sku'].isin(beer_club_skus)

df_beer_club = df_products[mask]

df_beer_club = df_beer_club[['Order ID', 'Product Name', 'Quantity', 'Sku', 'OriginalUnitPrice']]
df_beer_club

df_bongo = df_orders[(df_orders['Customer ID'] == 'gid://shopify/Customer/3834879082659') & (df_orders['TotalPrice'] <= 0)]


df_bongo = df_bongo[['Order ID', 'TotalPrice', 'City', 'ZIP', 'Customer ID']]


df_orders = df_orders[df_orders['Customer ID'] != 'gid://shopify/Customer/3834879082659']


customer_id_to_remove = 'gid://shopify/Customer/4639759958179'
df_orders = df_orders[df_orders['Customer ID'] != customer_id_to_remove]

df_orders.info()


beer_club_order_ids = df_beer_club['Order ID'].unique()

mask = df_orders['Order ID'].isin(beer_club_order_ids)

df_orders = df_orders[~mask]
df_orders

num_rows = df_beer_club.shape[0]

print("Nombre de lignes dans df_beer_club :", num_rows)


# Supprimer les commandes avec un Total Price égal à 0
df_orders = df_orders[df_orders['TotalPrice'] != 0]
df_orders.info()




import matplotlib.pyplot as plt

# Filtrer les lignes avec des valeurs non nulles dans 'Country Code' et inclure seulement les pays spécifiques
selected_countries = ['LU', 'GB', 'NL', 'BE', 'FR', 'DE']
filtered_orders = df_orders[df_orders['Country Code'].isin(selected_countries)]

# Nombre total de commandes par code de pays
orders_by_country_code = filtered_orders['Country Code'].value_counts()

# Créer un camembert
plt.figure(figsize=(8, 8))
plt.pie(orders_by_country_code, labels=orders_by_country_code.index, autopct='%1.1f%%', startangle=140)
plt.axis('equal')  # Pour que le camembert soit un cercle parfait
plt.title("Répartition des commandes par code de pays (LU, GB, NL, BE, FR, DE)")
plt.show()



# Filtrer les lignes avec des valeurs non nulles dans 'Country' et inclure seulement les pays spécifiques
selected_countries = ['Luxembourg', 'United Kingdom', 'Netherlands', 'Belgium', 'France', 'Germany']
filtered_orders = df_orders[df_orders['Country'].isin(selected_countries)]

# Nombre total de commandes par pays
orders_by_country = filtered_orders['Country'].value_counts()

# Créer un camembert
plt.figure(figsize=(8, 8))
plt.pie(orders_by_country, labels=orders_by_country.index, autopct='%1.1f%%', startangle=140)
plt.axis('equal')  # Pour que le camembert soit un cercle parfait
plt.title("Répartition des commandes par pays (LU, GB, NL, BE, FR, DE)")
plt.show()




# Chiffre d'affaires par code postal
revenue_by_zip = df_orders.groupby('ZIP')['TotalPrice'].sum()
print("Chiffre d'affaires par code postal :\n", revenue_by_zip)

# Moyenne des prix par code postal
average_price_by_zip = df_orders.groupby('ZIP')['TotalPrice'].mean()
print("Moyenne des prix par code postal :\n", average_price_by_zip)

import matplotlib.pyplot as plt

# Nombre total de commandes par ville
orders_by_city = df_orders['City'].value_counts()

# Afficher les 5 villes où vous envoyez le plus de commandes
top_cities = orders_by_city.head(5)

# Afficher les counts des 5 principales villes
print("Top 5 des villes où vous envoyez le plus de commandes :")
print(top_cities)

# Créer un graphique à barres pour les 5 principales villes
plt.figure(figsize=(10, 6))
top_cities.plot(kind='bar')
plt.xlabel('Ville')
plt.ylabel('Nombre de commandes')
plt.title('Top 5 des villes où vous envoyez le plus de commandes')
plt.xticks(rotation=45)
plt.show()


import matplotlib.pyplot as plt

# Exclure "Bruxelles" de la liste des villes de destination
cities_excluding_brussels = df_orders[df_orders['City'] != 'Bruxelles']

# Nombre total de commandes par ville (hors Bruxelles)
orders_by_city = cities_excluding_brussels['City'].value_counts()

# Afficher les 20 premières villes (hors Bruxelles) où vous envoyez le plus de commandes
top_cities = orders_by_city.head(20)

# Créer un graphique à barres pour les 20 premières villes (hors Bruxelles)
plt.figure(figsize=(12, 6))
top_cities.plot(kind='bar')
plt.xlabel('Ville')
plt.ylabel('Nombre de commandes')
plt.title('Top 20 des villes (hors Bruxelles) où vous envoyez le plus de commandes')
plt.xticks(rotation=45)
plt.show()


import matplotlib.pyplot as plt

# Exclure "Bruxelles" de la liste des villes de destination
cities_excluding_brussels = df_orders[df_orders['City'] != 'Bruxelles']

# Nombre total de commandes par ville (hors Bruxelles)
orders_by_city = cities_excluding_brussels['City'].value_counts()

# Sélectionner les 20 premières villes (hors Bruxelles)
top_cities = orders_by_city.head(20)

# Créer un camembert pour les 20 premières villes (hors Bruxelles)
plt.figure(figsize=(10, 10))
plt.pie(top_cities, labels=top_cities.index, autopct='%1.1f%%', startangle=140)
plt.axis('equal')  # Pour que le camembert soit un cercle parfait
plt.title('Répartition des commandes par ville (hors Bruxelles)')
plt.show()

import matplotlib.pyplot as plt

# Exclure "Bruxelles", "Brussels" et "Brussel" de la liste des villes de destination
cities_excluding_brussels = df_orders[~df_orders['City'].isin(['Bruxelles', 'Brussels', 'Brussel'])]

# Nombre total de commandes par ville (hors Bruxelles, Brussels et Brussel)
orders_by_city = cities_excluding_brussels['City'].value_counts()

# Sélectionner les 20 premières villes (hors Bruxelles, Brussels et Brussel)
top_cities = orders_by_city.head(20)

# Créer un camembert pour les 20 premières villes (hors Bruxelles, Brussels et Brussel)
plt.figure(figsize=(10, 10))
plt.pie(top_cities, labels=top_cities.index, autopct='%1.1f%%', startangle=140)
plt.axis('equal')  # Pour que le camembert soit un cercle parfait
plt.title('Répartition des commandes par ville (hors Bruxelles, Brussels et Brussel)')
plt.show()

import matplotlib.pyplot as plt

# Filtrer les commandes en France
cities_in_france = df_orders[df_orders['Country'] == 'France']

# Nombre total de commandes par ville en France
orders_by_city_in_france = cities_in_france['City'].value_counts()

# Sélectionner les 20 premières villes en France
top_cities_in_france = orders_by_city_in_france.head(20)

# Créer un graphique à barres pour les 20 premières villes en France
plt.figure(figsize=(12, 6))
top_cities_in_france.plot(kind='bar')
plt.xlabel('Ville')
plt.ylabel('Nombre de commandes')
plt.title('Top 20 des villes en France où vous envoyez le plus de commandes')
plt.xticks(rotation=45)
plt.show()




import matplotlib.pyplot as plt

# Filtrer les commandes en France
cities_in_france = df_orders[df_orders['Country'] == 'France']

# Normaliser la casse de la colonne 'City' pour éviter les doublons
cities_in_france['City'] = cities_in_france['City'].str.lower()

# Exclure Paris (en minuscules) des données
cities_in_france = cities_in_france[cities_in_france['City'] != 'paris']

# Nombre total de commandes par ville en France
orders_by_city_in_france = cities_in_france['City'].value_counts()

# Sélectionner les 20 premières villes en France
top_cities_in_france = orders_by_city_in_france.head(20)

# Créer un graphique à barres pour les 20 premières villes en France (en excluant Paris)
plt.figure(figsize=(12, 6))
top_cities_in_france.plot(kind='bar')
plt.xlabel('Ville')
plt.ylabel('Nombre de commandes')
plt.title('Top 20 des villes en France (hors Paris) où vous envoyez le plus de commandes')
plt.xticks(rotation=45)
plt.show()


import matplotlib.pyplot as plt

# Filtrer les commandes aux Pays-Bas
orders_in_netherlands = df_orders[df_orders['Country Code'] == 'NL']

# Nombre total de commandes par ville aux Pays-Bas
orders_by_city = orders_in_netherlands['City'].value_counts().head(20)

# Créer un histogramme pour les 20 premières villes avec le plus grand nombre de commandes aux Pays-Bas
plt.figure(figsize=(12, 6))
orders_by_city.plot(kind='bar')
plt.xlabel('Ville')
plt.ylabel('Nombre de commandes')
plt.title('20 premières villes aux Pays-Bas avec le plus grand nombre de commandes')
plt.xticks(rotation=45)
plt.show()


# Filtrer le DataFrame pour exclure les valeurs TotalPrice <= 10
df_filtered = df_orders[df_orders['TotalPrice'] > 10]

# Calculer les statistiques descriptives pour la colonne "TotalPrice"
stats = df_filtered['TotalPrice'].describe()

# Afficher les statistiques descriptives
print(stats)


# Compter combien de commandes ont un montant total égal à 0
nombre_de_commandes_avec_montant_zero = (df_orders['TotalPrice'] == 0).sum()

# Afficher le nombre de commandes avec un montant total égal à 0
print("Nombre de commandes avec montant total égal à 0 :", nombre_de_commandes_avec_montant_zero)

# Compter le nombre de lignes où TotalPrice > 10
count_gt_10 = (df_orders['TotalPrice'] <15).sum()

# Afficher le nombre de lignes
print(f"Nombre de lignes où TotalPrice <20 : {count_gt_10}")

import matplotlib.pyplot as plt

# Tracer un boxplot horizontal pour la colonne "TotalPrice"
plt.figure(figsize=(8, 4))
plt.boxplot(df_filtered['TotalPrice'], vert=False)
plt.xlabel('Prix')
plt.title('Boxplot horizontal de TotalPrice')
plt.show()

import matplotlib.pyplot as plt

# Créez un histogramme pour la distribution des prix des commandes avec plus de détails entre 0 et 300
plt.figure(figsize=(12, 6))
plt.hist(df_orders['TotalPrice'], bins=30, range=(0, 300), alpha=0.7, color='blue', edgecolor='black')
plt.xlabel('TotalPrice')
plt.ylabel('Nombre d\'observations')
plt.title('Répartition des Prix des Commandes (entre 0 et 300)')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()


# Compter le nombre d'occurrences de chaque nom de produit
product_name_counts = df_products['Product Name'].value_counts()

# Afficher les 30 premiers produits les plus comptés
top_30_product_names = product_name_counts.head(30)
print(top_30_product_names)

df
df_beer_club