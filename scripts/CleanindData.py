 

#PROJECT 1 (CLEANING DATA)
# #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
# #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
import pandas as pd
import numpy as np
import unicodedata
import os

# #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
# #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
#ORDER ITEMS TB :
#    NO NULL VALUE :
order_items=pd.read_csv("Project 1\olist_order_items_dataset.csv")
order_items = order_items.drop_duplicates()

print(order_items.info())
print("#######################################################################################################")
print(order_items.sample(50))
print("#######################################################################################################")
#FIX DATATYPE:

order_items['order_id'] = order_items['order_id'].astype('string')
order_items['product_id'] = order_items['product_id'].astype('string')
order_items['seller_id'] = order_items['seller_id'].astype('string')
order_items['shipping_limit_date'] =  pd.to_datetime(order_items['shipping_limit_date'], errors='coerce')
print("#######################################################################################################")
# #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
#SELLERS TB :
#    NO NULL VALUE :
sellers=pd.read_csv("Project 1\olist_sellers_dataset.csv")

sellers = sellers.drop_duplicates()
print(sellers.info())
print("#######################################################################################################")
print(sellers.sample(50))
print("#######################################################################################################")
#FIX DATATYPE:
sellers['seller_id'] = sellers['seller_id'].astype('string')
sellers['seller_city'] = sellers['seller_city'].astype('string')
sellers['seller_state'] = sellers['seller_state'].astype('string')


#CLEANING FUNCTION :
#Made for cleaning portuguese lenguage expecially :
def clean_column(text,underscore=False):
    if pd.isna(text): #-----NaN value option
        return text
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')#----solution for brazilian vocals
    text = text.lower()#----lower all 
    text = text.strip()
    text = ' '.join(text.split())#----solution double space 
    if underscore == True:
        text = text.replace('_', ' ').replace('-', ' ')

    return text

sellers['seller_city']=sellers['seller_city'].apply(clean_column)

print("#######################################################################################################")
# Mapping the variable :
city_replacement_map = {

    'sao pauo': 'sao paulo',
    'sao paluo': 'sao paulo',
    'sao paulop': 'sao paulo',
    'sao jose do rio pret': 'sao jose do rio preto',
    's jose do rio preto': 'sao jose do rio preto',
    'scao jose do rio pardo': 'sao jose do rio pardo',
    'ribeirao pretp': 'ribeirao preto',
    'riberao preto': 'ribeirao preto',
    'belo horizont': 'belo horizonte',
    'cascavael': 'cascavel',
    'floranopolis': 'florianopolis',
    'garulhos': 'guarulhos',
    'balenario camboriu': 'balneario camboriu',
    'sao bernardo do capo': 'sao bernardo do campo',
    'sao jose dos pinhas': 'sao jose dos pinhais',
    'sando andre': 'santo andre',
    'ao bernardo do campo': 'sao bernardo do campo',
    'tabao da serra': 'taboao da serra',
    'portoferreira': 'porto ferreira',
    'sao paulo / sao paulo': 'sao paulo',
    'rio de janeiro / rio de janeiro': 'rio de janeiro',
    'rio de janeiro \\rio de janeiro': 'rio de janeiro',
    'rio de janeiro, rio de janeiro, brasil': 'rio de janeiro',
    'novo hamburgo, rio grande do sul, brasil': 'novo hamburgo',
    'lages - sc': 'lages',
    'brasilia df': 'brasilia',
    'cariacica / es': 'cariacica',
    'sbc/sp': 'sao bernardo do campo',
    'angra dos reis rj': 'angra dos reis',
    'pinhais/pr': 'pinhais',
    'maua/sao paulo': 'maua',
    'mogi das cruzes / sp': 'mogi das cruzes',
    'barbacena/ minas gerais': 'barbacena',
    'sao paulo sp': 'sao paulo',
    'jacarei / sao paulo': 'jacarei',
    'ribeirao preto / sao paulo': 'ribeirao preto',
    'carapicuiba / sao paulo': 'carapicuiba',
    'sao sebastiao da grama/sp': 'sao sebastiao da grama',
    'aguas claras df': 'aguas claras',
    "arraial d'ajuda (porto seguro)": 'arraial d ajuda',
    "sao miguel d'oeste": 'sao miguel do oeste',
    '04482255': 'unknown', 
    'vendas@creditparts.com.br': 'unknown', 
    'centro': 'unknown',
    'bahia': 'unknown', 
    'minas gerais': 'unknown', 
    'parana': 'unknown',
    'santa catarina': 'unknown', 
    'tocantins': 'unknown', 
    'sp': 'sao paulo', 
    'sbc': 'sao bernardo do campo',
     'auriflama/sp': 'auriflama',
    'sao paulo - sp': 'sao paulo',
    'santo andre/sao paulo': 'santo andre',
    'sp / sp': 'sao paulo',
    'santa barbara d oeste': 'santa barbara do oeste',
    'andira-pr': 'andira',
    'mogi das cruses': 'mogi das cruzes', 
    'robeirao preto': 'ribeirao preto',   
    'santa barbara d oeste': 'santa barbara do oeste', 
    'california': 'unknown',
    'guarulhos-sp': 'guarulhos',
    'lambari d%26apos%3boeste': 'lambari do oeste',
    'lambari doeste': 'lambari do oeste',
    'parana d\'oeste': 'parana do oeste',
    'aparecida doeste': 'aparecida do oeste',
    'santa rita doeste': 'santa rita do oeste',
    'sao felipe d\'oeste': 'sao felipe do oeste',
    'mirassol d oeste': 'mirassol do oeste',
    'bandeirantes d\'oeste': 'bandeirantes do oeste',
    'olho d agua das cunhas': 'olho dagua das cunhas',
    'olho d\'agua': 'olho dagua',
    'fatimarmnte dutra': 'unknown',
    'pulinopolis': 'unknown',
    'deserto': 'unknown',
    'luar': 'unknown',
    'roberto': 'unknown',
    '4o. centenario': 'unknown'
    

}

sellers['seller_city'] = sellers['seller_city'].map(city_replacement_map).fillna(sellers['seller_city'])
sellers['seller_city'] = sellers['seller_city'].astype('string')#---change data once again after mapping !
# #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
#CUSTOMER TB :

# NO NULL VALUE :
customer=pd.read_csv("Project 1\olist_customers_dataset.csv")

customer = customer.drop_duplicates()
print(customer.info())
print("#######################################################################################################")
print(customer.sample(50))
print("#######################################################################################################")
#FIX DATATYPE:

customer['customer_id'] = customer['customer_id'].astype('string')
customer['customer_unique_id'] = customer['customer_unique_id'].astype('string')
customer['customer_city'] = customer['customer_city'].astype('string')
customer['customer_state'] = customer['customer_state'].astype('string')
#FUNCTION USE TO BE SURE THE STRING VARIABLE  IS  CLEAN :
customer['customer_city'] = customer['customer_city'].apply(clean_column)
# #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
#PRODUCTS TB :

#    NO NULL VALUE :
products=pd.read_csv("Project 1\olist_products_dataset.csv")

products = products.drop_duplicates()
print(products.info())
print("#######################################################################################################")
print(products.sample(50))
print("#######################################################################################################")
#FIX DATATYPE:

products['product_id'] = products['product_id'].astype('string')
products['product_category_name'] = products['product_category_name'].astype('string')

# FIX NaN  :
mask_correlated_nans = products['product_category_name'].isna()
columns_to_display = [
    'product_id',
    'product_category_name',
    'product_name_lenght',
    'product_description_lenght',
    'product_photos_qty',
    'product_weight_g' #We already see by the info that only these coloums have around 610 NaN value lets check if the Nan values are stick all togheter !
]

print(products.loc[mask_correlated_nans,columns_to_display].head(50))

#Yes They are !...So let's fill category name and the values with uknow for name 
# and numerical variable with median .
#usefull to keep values during query for  metrics calculation

# #N/A filling values:
products['product_category_name'].fillna('unknown', inplace=True)
products['product_name_lenght'].fillna(products['product_name_lenght'].median(), inplace=True)
products['product_description_lenght'].fillna(products['product_description_lenght'].median(), inplace=True)
products['product_photos_qty'].fillna(products['product_photos_qty'].median(), inplace=True)

# #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
# Mapping category products name with english names (for analisys):

category_translation_map = {
    'perfumaria': 'perfumery',
    'artes': 'art',
    'esporte lazer': 'sports_leisure',
    'bebes': 'baby',
    'utilidades domesticas': 'home_utilities',
    'instrumentos musicais': 'musical_instruments',
    'cool stuff': 'general_items', 
    'moveis decoracao': 'furniture_decor',
    'eletrodomesticos': 'home_appliances',
    'brinquedos': 'toys',
    'cama mesa banho': 'bed_bath_table',
    'construcao ferramentas seguranca': 'construction_tools_safety',
    'informatica acessorios': 'computers_accessories',
    'beleza saude': 'health_beauty',
    'malas acessorios': 'luggage_accessories',
    'ferramentas jardim': 'garden_tools',
    'moveis escritorio': 'office_furniture',
    'automotivo': 'automotive',
    'eletronicos': 'electronics',
    'fashion calcados': 'fashion_shoes',
    'telefonia': 'telephony',
    'papelaria': 'stationery',
    'fashion bolsas e acessorios': 'fashion_bags_accessories',
    'pcs': 'pcs',
    'casa construcao': 'home_construction',
    'relogios presentes': 'watches_gifts',
    'construcao ferramentas construcao': 'construction_tools_construction',
    'pet shop': 'pet_shop',
    'eletroportateis': 'portable_appliances',
    'agro industria e comercio': 'agro_industry_commerce',
    'unknown': 'unknown', 
    'moveis sala': 'living_room_furniture',
    'sinalizacao e seguranca': 'signaling_security',
    'climatizacao': 'air_conditioning',
    'consoles games': 'consoles_games',
    'livros interesse geral': 'books_general_interest',
    'construcao ferramentas ferramentas': 'construction_tools_tools',
    'fashion underwear e moda praia': 'fashion_underwear_beachwear',
    'fashion roupa masculina': 'fashion_male_clothing',
    'moveis cozinha area de servico jantar e jardim': 'kitchen_dining_garden_furniture',
    'industria comercio e negocios': 'industry_commerce_business',
    'telefonia fixa': 'fixed_telephony',
    'construcao ferramentas iluminacao': 'construction_tools_lighting',
    'livros tecnicos': 'technical_books',
    'eletrodomesticos 2': 'home_appliances', # Stick with the category home_appliances ,already existing
    'artigos de festas': 'party_articles',
    'bebidas': 'drinks',
    'market place': 'market_place',
    'la cuisine': 'home_appliances', # Stick with the category home_appliances ,already existing 
    'construcao ferramentas jardim': 'construction_tools_garden',
    'fashion roupa feminina': 'fashion_female_clothing',
    'casa conforto': 'home_comfort',
    'audio': 'audio',
    'alimentos bebidas': 'food_drinks',
    'musica': 'music',
    'alimentos': 'food',
    'tablets impressao imagem': 'tablets_printing_image',
    'livros importados': 'imported_books',
    'portateis casa forno e cafe': 'home_oven_coffee_portables',
    'fashion esporte': 'fashion_sport',
    'artigos de natal': 'christmas_articles',
    'fashion roupa infanto juvenil': 'fashion_childrens_clothing',
    'dvds blu ray': 'dvds_blu_ray',
    'artes e artesanato': 'art_craftsmanship',
    'pc gamer': 'pc_gamer',
    'moveis quarto': 'bedroom_furniture',
    'cine foto': 'cine_photo',
    'fraldas higiene': 'diapers_hygiene',
    'flores': 'flowers',
    'casa conforto 2': 'home_comfort_2', 
    'portateis cozinha e preparadores de alimentos': 'kitchen_food_prep_portables',
    'seguros e servicos': 'insurance_services',
    'moveis colchao e estofado': 'mattress_upholstery_furniture',
    'cds dvds musicais': 'cds_dvds_musical'
}

#IMPORTANT USE FUNCTON BEFORE MAPPING TO HAVE SAME NAMES INSIDE 'category_translation_map' :
products['product_category_name'] = products['product_category_name'].apply(lambda x: clean_column(x, underscore=True))

products['product_category_name'] = products['product_category_name'].map(category_translation_map)

# #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
#ORDERS TB :
#    NO NULL VALUE :
orders=pd.read_csv("Project 1\olist_orders_dataset.csv")

orders = orders.drop_duplicates()
print(orders.info())
print("#######################################################################################################")
print(orders.sample(50))
print("#######################################################################################################")
#FIX DATATYPE:

orders['order_id'] = orders['order_id'].astype('string')
orders['customer_id'] = orders['customer_id'].astype('string')
orders['order_status'] = orders['order_status'].astype('string')
orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'], errors='coerce')
orders['order_approved_at'] = pd.to_datetime(orders['order_approved_at'], errors='coerce')
orders['order_delivered_carrier_date'] = pd.to_datetime(orders['order_delivered_carrier_date'], errors='coerce')
orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'], errors='coerce')
orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'], errors='coerce')

#STILL Nan on temporal variable....keep it same to not create Bias(NaN when Status is "canceled"and "unavailable".
# #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
#TB REVIEW : NO NaN in the filtered one .
reviews=pd.read_csv("Project 1\olist_order_reviews_dataset.csv")
reviews = reviews.drop_duplicates()

print(reviews.info())
print("#######################################################################################################")
print(reviews.sample(50))
print("#######################################################################################################")

#ISOLATE THE VARIABLES I NEED :
columns_to_keep = ['review_id', 'order_id', 'review_score', 'review_creation_date']
reviews_cleaned = reviews[columns_to_keep].copy() # .copy() to avoid problems with the original TB 

#FIX DATATYPE:
reviews_cleaned['review_id'] = reviews_cleaned['review_id'].astype('string')
reviews_cleaned['order_id'] = reviews_cleaned['order_id'].astype('string')
reviews_cleaned['review_creation_date'] = pd.to_datetime(reviews_cleaned['review_creation_date'], errors='coerce')
# #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
#GEOLOCATION TAB :
 # #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

geolocation=pd.read_csv("Project 1\olist_geolocation_dataset.csv")
geolocation = geolocation.drop_duplicates()

print(geolocation.info())
print("#######################################################################################################")
print(geolocation.sample(50))
print("#######################################################################################################")



#FIX DATATYPE:

geolocation['geolocation_zip_code_prefix'] = geolocation['geolocation_zip_code_prefix'].astype('int64')
geolocation['geolocation_city'] = geolocation['geolocation_city'].astype('string')
geolocation['geolocation_state'] = geolocation['geolocation_state'].astype('string')

#CLEAN WITH FUNCTION :
geolocation['geolocation_city'] = geolocation['geolocation_city'].apply(clean_column)
geolocation['geolocation_state'] = geolocation['geolocation_state'].apply(clean_column)
# Standardize city names in 'geolocation_city' using the unified replacement map
# to ensure consistency across all datasets. Names not in the map remain unchanged.
geolocation['geolocation_city'] = geolocation['geolocation_city'].map(city_replacement_map).fillna(geolocation['geolocation_city'])
geolocation['geolocation_city'] = geolocation['geolocation_city'].astype('string')
geolocation['geolocation_state'] = geolocation['geolocation_state'].astype('string')

#CREATE CODE TO DOWNLOAD CSV OF ALL THE TB CLEAN :


home_dir = os.path.expanduser('~')
output_dir = os.path.join(home_dir, 'Desktop', 'Braz_EcommerceCleanTab7')
os.makedirs(output_dir, exist_ok=True)

order_items.to_csv(os.path.join(output_dir, "olist_order_items_cleaned.csv"), index=False)
sellers.to_csv(os.path.join(output_dir, "olist_sellers_cleaned.csv"), index=False)
customer.to_csv(os.path.join(output_dir, "olist_customers_cleaned.csv"), index=False)
products.to_csv(os.path.join(output_dir, "olist_products_cleaned.csv"), index=False)
orders.to_csv(os.path.join(output_dir, "olist_orders_cleaned.csv"), index=False)
reviews_cleaned.to_csv(os.path.join(output_dir, "olist_reviews_cleaned.csv"), index=False)
geolocation.to_csv(os.path.join(output_dir, "olist_geolocation_cleaned.csv"), index=False)
print("******* Data cleaning and export process completed successfully! *******")
 