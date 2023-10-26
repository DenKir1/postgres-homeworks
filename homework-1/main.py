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
    data_sql = ", ".join(map(str, data))
    text_to_sql = f"INSERT INTO {my_table} VALUES {data_sql}"
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(text_to_sql)
                # cur.execute("SELECT * FROM mytable")
                # rows = cur.fetchall()
                # for row in rows:
                #    print(row)
            print("Execution done")
    finally:
        conn.close()


def insert_customers():
    """ SQL запрос на ввод данных в customers из-за "BONAP","Bon app'",..."""
    conn = psycopg2.connect(host="localhost", database="north",
                            user="postgres", password="12345")

    try:
        with conn:
            with conn.cursor() as cur:
                with open(path_customers, newline='', encoding="utf-8") as csvfile:
                    reader = csv.DictReader(csvfile)
                    for line in reader:
                        cus_id = line['customer_id']
                        comp_name = line['company_name']
                        cont_name = line['contact_name']
                        cur.execute('INSERT INTO customers VALUES (%s, %s, %s)', (cus_id, comp_name, cont_name))

            print("Execution done")
    finally:
        conn.close()


path_orders = "north_data/orders_data.csv"
path_customers = "north_data/customers_data.csv"
path_employees = "north_data/employees_data.csv"

orders = from_csv(path_orders)
customers = from_csv(path_customers)
employees = from_csv(path_employees)

insert_bd("employees", employees)
#insert_bd("customers", customers)
insert_customers()
insert_bd("orders", orders)

