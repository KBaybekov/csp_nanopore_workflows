import unittest
from unittest.mock import patch, MagicMock
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.common import get_dirs_in_dir, load_yaml

class TestCommonUtils(unittest.TestCase):

    @patch('os.listdir')
    def test_get_dirs_in_dir(self, mock_listdir):
        mock_listdir.return_value = ['sample1', 'sample2', 'file.txt']
        os.path.isdir = MagicMock(side_effect=lambda path: not path.endswith('file.txt'))

        result = get_dirs_in_dir('/test')
        self.assertEqual(result, ['/test/sample1/', '/test/sample2/'])

        # Проверка исключения
        mock_listdir.return_value = []
        with self.assertRaises(FileNotFoundError):
            get_dirs_in_dir('/empty')

    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='key: value\nsubsection:\n  subkey: subvalue')
    def test_load_yaml(self, mock_open):
        # Проверка загрузки всего файла
        result = load_yaml('/test/config.yaml')
        self.assertEqual(result, {'key': 'value', 'subsection': {'subkey': 'subvalue'}})

        # Проверка загрузки подраздела
        result = load_yaml('/test/config.yaml', subsection='subsection')
        self.assertEqual(result, {'subkey': 'subvalue'})

        # Проверка обработки отсутствия подраздела
        with self.assertRaises(ValueError):
            load_yaml('/test/config.yaml', subsection='missing')

if __name__ == '__main__':
    unittest.main()