import csv
import os

import psycopg2
import pytest

# Фикстура для подключения к базе данных PostgreSQL
@pytest.fixture(scope="module")
def db_connection():
    conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="sql",
        host="localhost",
        port="5432"
    )
    yield conn
    conn.close()

# Фикстура для создания таблиц loadstatic и resultstatic
@pytest.fixture(scope="module")
def create_tables(db_connection):
    cursor = db_connection.cursor()
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
    db_connection.commit()
    cursor.close()

# Фикстура для загрузки данных из CSV в таблицу loadstatic
@pytest.fixture(scope="module")
def save_to_loadstatic(create_tables, db_connection):
    def _save_to_loadstatic(file_path):
        cursor = db_connection.cursor()
        with open(file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                cursor.execute(
                    "INSERT INTO loadstatic (price, count, add_cost, company, product) VALUES (%s, %s, %s, %s, %s)",
                    (row['price'], row['count'], row['add_cost'], row['company'], row['product'])
                )
        db_connection.commit()
        cursor.close()

    return _save_to_loadstatic

# Фикстура для расчета цены на единицу
@pytest.fixture(scope="module")
def calculate_price_per_unit(create_tables, db_connection):
    def _calculate_price_per_unit():
        cursor = db_connection.cursor()
        cursor.execute(
            "SELECT product, SUM(price + add_cost) / SUM(count) AS price_per_unit FROM loadstatic GROUP BY product"
        )
        result = cursor.fetchall()
        cursor.close()
        return result

    return _calculate_price_per_unit

# Фикстура для сохранения результата в таблицу resultstatic
@pytest.fixture(scope="module")
def save_to_resultstatic(create_tables, db_connection, calculate_price_per_unit):
    def _save_to_resultstatic():
        cursor = db_connection.cursor()
        cursor.execute("TRUNCATE TABLE resultstatic")
        result = calculate_price_per_unit()
        for row in result:
            price_per_unit = round(row[1])
            cursor.execute("INSERT INTO resultstatic (product, price_per_unit) VALUES (%s, %s)", (row[0], price_per_unit))
        db_connection.commit()
        cursor.close()

    return _save_to_resultstatic

# Фикстура для получения данных из таблицы resultstatic
@pytest.fixture(scope="module")
def get_result_from_db(db_connection):
    def _get_result_from_db():
        cursor = db_connection.cursor()
        cursor.execute("SELECT * FROM resultstatic")
        result = cursor.fetchall()
        cursor.close()
        return result

    return _get_result_from_db


# Тест для загрузки данных из CSV в таблицу loadstatic
def test_save_to_loadstatic(save_to_loadstatic):
    data = os.path.join('tests', 'test_data.csv')
    save_to_loadstatic(data) # Заменить "data.csv" на путь к CSV файлу с данными
    # Проверка, что данные были успешно загружены в таблицу
    with save_to_loadstatic.db_connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM loadstatic")
        count = cursor.fetchone()[0]
        assert count == 2, "Неправильное количество записей в таблице loadstatic"

# Тест для расчета цены на единицу
def test_calculate_price_per_unit(calculate_price_per_unit):
    result = calculate_price_per_unit()
    # Проверка, что результат расчета соответствует ожидаемому
    expected_result = [
        ('Product A', 0.65000000000000000000),
        ('Product B', 1161.9130292914334480)
    ]
    assert result == expected_result, "Неправильный результат расчета цены на единицу"

# Тест для сохранения результата в таблицу resultstatic
def test_save_to_resultstatic(save_to_resultstatic):
    save_to_resultstatic()
    # Проверка, что результат был успешно сохранен в таблицу
    with save_to_resultstatic.db_connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM resultstatic")
        count = cursor.fetchone()[0]
        assert count == 2, "Неправильное количество записей в таблице resultstatic"

# Тест для получения данных из таблицы resultstatic
def test_get_result_from_db(get_result_from_db):
    result = get_result_from_db()
    # Проверка, что полученные данные соответствуют ожидаемым
    expected_result = [
        ('Product A', '1'),
        ('Product B', '1162')
    ]
    assert result == expected_result, "Неправильные данные из таблицы resultstatic"
