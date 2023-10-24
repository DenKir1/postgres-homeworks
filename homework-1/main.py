"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2


def from_csv(path_: str):
    """ Выдает список кортежей с данными из файла"""
    thing = []
    with open(path_, newline='', encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            row_ = tuple(row)
            thing.append(row_)
        return thing[1:]


def insert_bd(my_table: str, data: list):
    """ SQL запрос на ввод данных"""
    conn = psycopg2.connect(host="localhost", database="north",
                            user="postgres", password="12345")
    data_sql = ", ".join(data)
    text_to_sql = f"INSERT INTO {my_table} VALUES {data_sql}"
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(text_to_sql)
                # cur.execute("SELECT * FROM mytable")
                # rows = cur.fetchall()
                # for row in rows:
                #    print(row)
    finally:
        conn.close()


path1 = "north_data/orders_data.csv"
path2 = "north_data/customers_data.csv"
path3 = "north_data/employees_data.csv"
print(from_csv(path1))
