"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2

# Подключение к базе данных
conn = psycopg2.connect(host="localhost", database="north", user="postgres", password="J3h7k2")

# Создание курсора
try:
    with conn:
        cur = conn.cursor()
        with open("north_data/employees_data.csv", "r", encoding="utf-8") as file:
            # Загрузка данных из файла "employees.csv" в таблицу employees
            reader = csv.DictReader(file)
            for row in reader:
                cur.execute(
                    f'INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes)'
                    f'VALUES (%s, %s, %s, %s, %s, %s)',
                    (row["employee_id"], row["first_name"], row["last_name"], row["title"], row["birth_date"],
                     row["notes"]))

        with open("north_data/customers_data.csv", "r", encoding="utf-8") as file:
            # Загрузка данных из файла "customers.csv" в таблицу customers
            reader = csv.DictReader(file)
            for row in reader:
                cur.execute(
                    f'INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)',
                    (row["customer_id"], row["company_name"], row["contact_name"]))

        with open("north_data/orders_data.csv", "r", encoding="utf-8") as file:
            # Загрузка данных из файла "orders.csv" в таблицу orders
            reader = csv.DictReader(file)
            for row in reader:
                cur.execute(
                    f'INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city)'
                    f'VALUES (%s, %s, %s, %s, %s)',
                    (row["order_id"], row["customer_id"], row["employee_id"], row['order_date'], row['ship_city']))

        # Сохранение изменений
        # conn.commit()

finally:
    # Закрытие курсора и соединения
    cur.close()
    conn.close()
