import csv
import psycopg2


# Подключение к базе данных PostgreSQL
def connect_to_db():
    conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="sql",
        host="localhost",
        port="5432"
    )
    return conn


# Создание таблиц loadstatic и resultstatic
def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS loadstatic (
            id SERIAL PRIMARY KEY,
            price DECIMAL(10, 2),
            count INTEGER,
            add_cost DECIMAL(10, 2),
            company VARCHAR(255),
            product VARCHAR(255)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS resultstatic (
            id SERIAL PRIMARY KEY,
            product VARCHAR(255),
            price_per_unit INTEGER
        )
    """)

    conn.commit()
    cursor.close()


# Загрузка из CSV в таблицу loadstatic
def save_to_loadstatic(file_path):
    conn = connect_to_db()
    cursor = conn.cursor()

    create_tables(conn)

    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute(
                "INSERT INTO loadstatic (price, count, add_cost, company, product) "
                "VALUES (%s, %s, %s, %s, %s)",
                (row['price'], row['count'], row['add_cost'], row['company'], row['product']))

    conn.commit()
    cursor.close()
    conn.close()


# Вычисление результата по условиям:
# - суммировать данные по столбцам price, count, add_cost, если значение в столбце product совпадают
# - прогнозная цена = (стоимость + дополнительные расходы) / количество
def calculate_price_per_unit():
    conn = connect_to_db()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT product, SUM(price + add_cost) / SUM(count) AS price_per_unit "
        "FROM loadstatic GROUP BY product")
    result = cursor.fetchall()

    cursor.close()
    conn.close()

    return result

# Сохранение результата в таблицу resultstatic
def save_to_resultstatic():
    conn = connect_to_db()
    cursor = conn.cursor()

    create_tables(conn)

    cursor.execute("TRUNCATE TABLE resultstatic")

    result = calculate_price_per_unit()

    for row in result:
        price_per_unit = round(row[1])
        cursor.execute("INSERT INTO resultstatic (product, price_per_unit) "
                       "VALUES (%s, %s)", (row[0], price_per_unit))

    conn.commit()
    cursor.close()
    conn.close()

# Вывод данных из таблицы resultstatic
def get_result_from_db():
    conn = connect_to_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM resultstatic")
    result = cursor.fetchall()

    cursor.close()
    conn.close()

    return result
