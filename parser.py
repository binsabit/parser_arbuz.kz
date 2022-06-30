from lib2to3.pytree import Base
from attr import s
from bs4 import BeautifulSoup
from urllib import request,response
from pkg_resources import cleanup_resources
import requests
import json
import psycopg2
from config import host,user,password,db_name,port
import logging
import datetime
logging.basicConfig()

BaseURL = "https://arbuz.kz/"
CatalogURL = "ru/almaty/catalog/cat/"

connection = None
cursor = None

class Product:
    name = str
    price = int
    brandName = str
    link = str
    catalog = str

    def __init__(self,name,price,link, catalog,brandName):
        self.name = name
        self.price = price
        self.link = BaseURL + link
        self.catalog = catalog
        self.brandName = brandName
    def __repr__(self):
        return str(self.__dict__)


def iterateTgroughProductPages(BaseURL, CatalogURL, catalogID):
    url = BaseURL + CatalogURL + catalogID
    res = requests.get(url)
    print(res.url)
    soup = BeautifulSoup(res.text,"html.parser")
    pages = soup.find_all("a", class_="page-link")
    Pagehref = ""
    if len(pages) > 0:
        for i in range (len(pages)-1): 
            
            print(f"Parsing Page {i+1}")
            print()
            href = pages[i+1]['href']
            Pagehref = href
            getProducts(BaseURL, CatalogURL, catalogID, Pagehref)
    else:
        getProducts(BaseURL, CatalogURL, catalogID, Pagehref)


def getProducts(BaseURL, CatalogURL, catalogID,Pagehref):
    a = 0
    url = BaseURL + CatalogURL + catalogID + Pagehref
    res = requests.get(url)
    soup = BeautifulSoup(res.text,"html.parser")
    catalog_name = soup.find("h1").text
    catalog_name = catalog_name.replace("\n", "")
    catalog_name = catalog_name.replace(" ", "")
    products = soup.findAll("div", class_="product-card-list")
    for product in products:
        p = product.find_all("div")
        for product_info in p:
            if ':product' in product_info.attrs:
                dic_product = json.loads(product_info[':product'])
                name = dic_product['name']
                price = int(dic_product['priceActual'])
                link = dic_product['uri'].replace("\\", "")
                brand_name = dic_product['brandName']
                a = a + addProduct(Product(name,price,link,catalog_name,brand_name))
    print(f"Number of Products Addd {a}")
def addProduct(Product):
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            port=port
        )
        cursor = connection.cursor()
        create_table = '''CREATE TABLE IF NOT EXISTS product(
            id SERIAL PRIMARY KEY,
            name text,
            price int,
            brandname text,
            link text,
            catalog text)'''
        cursor.execute(create_table)   

    
        insert_product = '''INSERT INTO product (name, price, brandname,link,catalog) VALUES (%s,%s,%s,%s,%s)'''
        insert_values = (Product.name,Product.price,Product.brandName,Product.link,Product.catalog)
        cursor.execute(insert_product,insert_values)
        connection.commit()
        return 1 
    except Exception as _ex:
        print("[INFO] Error while working with postgres", _ex)
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def getCatalogs(url):
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    containers = soup.findAll("div", class_="container my-4")
    catalogIDs = []
    for cont in containers:
        cont_info = cont.findAll("div", class_="px-1")
        # print(len(cont_info))
        for inn in cont_info:
            items = inn.find_all("div")
            if ':product' in items[0].attrs:
                dic_obj = json.loads(items[0][':product'])
                if dic_obj['catalogId'] not in catalogIDs:
                    catalogIDs.append(dic_obj['catalogId'])
    return catalogIDs


catalogIDs = getCatalogs(BaseURL)

for catalog in catalogIDs:
    print(catalog)
    iterateTgroughProductPages(BaseURL, CatalogURL, catalog)       
