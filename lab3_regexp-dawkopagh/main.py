import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple
from sqlalchemy import create_engine

db_string = "postgresql://wbauer_adb:adb2020@pgsql-196447.vipserv.org:5432/wbauer_adb"

db = create_engine(db_string)

connection_sqlalchemy = db.connect()

def film_in_category(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str, dokładnie taki jak podana wartość
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(category, (int,str)):
        return None
    elif isinstance(category, int):
        df = pd.read_sql(f"""
                select f.title, lang.name as languge, cat.name as category
                from film f
                join language lang on f.language_id = lang.language_id
                join film_category fc on f.film_id = fc.film_id
                join category cat on fc.category_id = cat.category_id
                where cat.category_id = {category}
                order by f.title, lang.name""", con=connection_sqlalchemy)
        return df
    else:
        df = pd.read_sql(f"""
                select f.title, lang.name as languge, cat.name as category
                from film f
                join language lang on f.language_id = lang.language_id
                join film_category fc on f.film_id = fc.film_id
                join category cat on fc.category_id = cat.category_id
                where cat.name LIKE '{category}'
                order by f.title, lang.name""", con=connection_sqlalchemy)
        return df

def film_in_category_case_insensitive(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(category, (int, str)):
        return None
    elif isinstance(category,int):
        df = pd.read_sql(f"""
                        select f.title, lang.name as languge, cat.name as category
                        from film f
                        join language lang on f.language_id = lang.language_id
                        join film_category fc on f.film_id = fc.film_id
                        join category cat on fc.category_id = cat.category_id
                        where cat.category_id = {category}
                        order by f.title, lang.name""", con=connection_sqlalchemy)
        return df
    else:
        df = pd.read_sql(f"""
                select f.title, lang.name as languge, cat.name as category
                from film f
                join language lang on f.language_id = lang.language_id
                join film_category fc on f.film_id = fc.film_id
                join category cat on fc.category_id = cat.category_id
                where cat.name ilike '{category}'
                order by f.title, lang.name""", con=connection_sqlalchemy)
        return df
def film_cast(title:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o obsadę filmu o dokładnie zadanym tytule.
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |
    |0	|Greg       |Chaplin    | 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    title (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(title, str):
        return None
    else:
        df = pd.read_sql(f"""
                        select a.first_name, a.last_name
                        from actor a
                        join film_actor fa on a.actor_id = fa.actor_id
                        join film f on fa.film_id = f.film_id and f.title like '{title}'
                        order by a.last_name, a.first_name""", con=connection_sqlalchemy)
        return df

    

def film_title_case_insensitive(words:list) :
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuły filmów zawierających conajmniej jedno z podanych słów z listy words.
    Przykład wynikowej tabeli:
    |   |title              |
    |0	|Crystal Breaking 	| 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    words(list): wartość minimalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(words, list):
        return None
    else:
        wordstr = '|'.join(words)
        df = pd.read_sql(f"""
                        select title
                        from film
                        where title ~* '(?:^| )({wordstr})"""+"""{1,}(?:$| )'
                        order by title""", con=connection_sqlalchemy)
        return df