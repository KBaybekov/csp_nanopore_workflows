import os
import yaml
import subprocess

def get_samples_in_dir(dir:str, extensions:tuple, empty_ok:bool=False):
    """
    Генерирует список файлов на основе включающих и исключающих образцов.
    Выдаёт ошибку, если итоговый список пустой.

    :param dir: Директория, где искать файлы.
    :param extensions: Расширения файлов для поиска.
    :return: Список путей к файлам.
    """
    # Ищем все файлы в директории с указанными расширениями
    files = [os.path.join(dir, s) for s in os.listdir(dir) if s.endswith(extensions)]
    if not files and not empty_ok:
        raise FileNotFoundError("Образцы не найдены. Проверьте входные и исключаемые образцы, а также директорию с исходными файлами.")
    return files


def get_samples_in_dir_tree(dir:str, extensions:tuple, empty_ok:bool=False) -> list:
    """
    Генерирует список файлов, проходя по дереву папок, корнем которого является dir.
    Выдаёт ошибку, если итоговый список пустой.

    :param dir: Директория, где искать файлы.
    :param extensions: Кортеж расширений файлов для поиска.
    :return: Список файлов с путями.
    """
    files = []
    for root, _ds, fs in os.walk(dir):
        samples = [os.path.join(root, f) for f in fs 
                    if f.endswith(extensions)]
        files.extend(samples)
    if not files and not empty_ok:
        raise FileNotFoundError("Образцы не найдены. Проверьте входные и исключаемые образцы, а также директорию с исходными файлами.")
    return files

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
    
def run_shell_cmd(cmd:str, timeout:int=None) -> tuple:
    result = subprocess.Popen(args=cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True, executable="/bin/bash", bufsize=1, cwd=None, env=None)
    stdout, stderr = result.communicate(timeout=timeout)
    return (stdout, stderr)

def split_list_in_chunks(lst:list, chunks:int):
    """Yield successive n chunks from lst.
    Usage: x = list(split_list_in_chunks(lst, chunks))

    !! ДОРАБОТАТЬ !!
    :return: [1,2,3,4,5,6,7,8] -> [[1, 2], [3, 4], [5, 6], [7, 8]]
    """
    #Проводим целочисленное деление
    n = len(lst)//chunks
    #Определяем остаток
    residue = len(lst)%chunks
    #Если остаток есть, то делаем
    if residue > 0:
        n+=1
    for i in range(0, len(lst), n):
        yield lst[i:i + n]