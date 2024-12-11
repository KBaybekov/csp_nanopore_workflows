import os
import yaml

def get_dirs_in_dir(dir:str):
    """
    Генерирует список подпапок в указанной папке.
    Выдаёт ошибку, если итоговый список пустой.

    :param dir: Директория, где искать файлы.
    :param extensions: Расширения файлов для поиска.
    :return: Список путей к файлам.
    """
    # Ищем все файлы в директории с указанными расширениями
    dirs = [os.path.join(dir, s, '') for s in os.listdir(dir) if os.path.isdir(f'{dir}{s}')]
    if not dirs:
        raise FileNotFoundError("Образцы не найдены. Проверьте входные и исключаемые образцы, а также директорию с исходными файлами.")
    return dirs

def load_yaml(file_path:str, critical:bool = False, subsection:str = ''):
    """
    Универсальная функция для загрузки данных из YAML-файла.

    :param file_path: Путь к YAML-файлу. Ожидается строка, указывающая на местоположение файла с данными.
    :param critical: Возвращает ошибку, если файл не найден
    :param subsection: Опциональный параметр. Если передан, функция вернёт только данные из указанной
                       секции (например, конкретного этапа пайплайна). Если пусто, возвращаются все данные.
                       По умолчанию - пустая строка, что означает возврат всего содержимого файла.
    
    :return: Возвращает словарь с данными из YAML-файла. Если указан параметр subsection и он присутствует
             в YAML, возвращается соответствующая секция, иначе — всё содержимое файла.
    """
    # Открываем YAML-файл для чтения
    try:
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)  # Загружаем содержимое файла в словарь с помощью safe_load
        
        # Если subsection не указан, возвращаем весь YAML-файл
        if subsection == '':
            return data
        else:
            # Если subsection указан и существует в файле, возвращаем только эту секцию
            if subsection  in data.keys():
                return data[subsection]
            else:
                raise ValueError(f"Раздел '{subsection}' не найден в {file_path}")
    except FileNotFoundError as e:
        # Если файл не найден, возвращаем пустой словарь или ошибку, если данные необходимы для дальнейшей работы
        if critical:
            raise FileNotFoundError(f"Не найден: {file_path}")
        return {}