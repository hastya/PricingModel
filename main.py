from utils import (save_to_loadstatic, calculate_price_per_unit, save_to_resultstatic,
                   get_result_from_db)


def main():

    while True:
        print("Здравствуйте, напишите полный путь до файла с данными(в формате .csv)")
        print("Для выхода из программы напишите 'exit'")
        file_path = input()
        if file_path.lower() == 'exit':
            break
        if file_path.endswith('csv') is False:
            print('Путь неправильный')
            break
        else:
            file_path.replace("\\", "/")

            # Сохранение данных из csv в таблицу loadstatic
            save_to_loadstatic(file_path)

            # Расчет цены за единицу товара и сохранение результатов в таблицу resultstatic
            calculate_price_per_unit()
            save_to_resultstatic()

        # Вывод результатов из таблицы resultstatic
        print("Результаты:")
        result = get_result_from_db()
        for row in result:
            print(f"Продукт: {row[0]} - прогнозная цена: {row[1]}")

        # Предложение продолжить работу
        continue_work = input("Хотите посчитать другие данные? (yes/no): ")
        if continue_work.lower() != 'yes':
            break


if __name__ == "__main__":
    main()
