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

def film_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(category_id, int):
        return None
    df = pd.read_sql(f"""
    select f.title, language.name as languge, cat.name as category
    from film f
    join film_category fc on f.film_id = fc.film_id
    join category cat on fc.category_id = cat.category_id
    join language on language.language_id = f.language_id
    where fc.category_id = {category_id}
    order by f.title
    """, con=connection_sqlalchemy)
    return df
    
def number_films_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(category_id, int):
        return None
    else:
        df = pd.read_sql(f"""
        select category.name category, count(f.title) count
        from category
        join film_category fc on category.category_id = fc.category_id
        join film f on fc.film_id = f.film_id
        where category.category_id = {category_id}
        group by category.name""", con=connection_sqlalchemy)
        return df

def number_film_by_length(min_length: Union[int,float] = 0, max_length: Union[int,float] = 1e6 ) :
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(min_length, (int, float)):
        return None
    if not isinstance(max_length, (int, float)):
        return None
    if max_length < min_length:
        return None
    else:
        df = pd.read_sql(f"""
        select length, count(title)
        from film f 
        where length between {min_length} and {max_length}
        group by length
        """, con=connection_sqlalchemy)
        return df

def client_from_city(city:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(city, str):
        return None
    else:
        df = pd.read_sql(f"""
        select c.city, cu.first_name, cu.last_name
        from city c 
        join address ad on c.city_id = ad.city_id
        join customer cu on ad.address_id = cu.address_id
        where c.city = '{city}'
        order by cu.last_name, cu.first_name""", con=connection_sqlalchemy)
        return df

def avg_amount_by_length(length:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389
    
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(length, (int,float)):
        return None
    else:
        df = pd.read_sql(f"""
        select f.length, avg(p.amount)
        from payment p 
        join rental r on p.rental_id = r.rental_id
        join inventory inv on r.inventory_id = inv.inventory_id
        join film f on inv.film_id = f.film_id
        where f.length = {length}
        group by f.length""", con=connection_sqlalchemy)
        return df

def client_by_sum_length(sum_min:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265
    
    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(sum_min, (int,float)):
        return None
    else:
        df = pd.read_sql(f"""
        select cu.first_name, cu.last_name, sum(film.length) as sum
        from customer cu
        join rental r on cu.customer_id = r.customer_id
        join inventory inv on r.inventory_id = inv.inventory_id
        join film on inv.film_id = film.film_id
        group by cu.first_name, cu.last_name
        having sum(film.length) > {sum_min}
        order by sum, cu.last_name, cu.first_name""", con=connection_sqlalchemy)
        return df

def category_statistic_length(name:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(name,str):
        return None
    else:
        df = pd.read_sql(f"""
        select c.name as category, avg(f.length), sum(f.length), min(f.length), max(f.length)
        from film f
        join film_category fc on f.film_id = fc.film_id
        join category c on fc.category_id = c.category_id
        where c.name = '{name}'
        group by c.name""", con=connection_sqlalchemy)
        return df