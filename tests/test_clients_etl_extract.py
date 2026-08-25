from unittest.mock import Mock, patch

from src.components.tools.private import etl


def test_extract_reconciles_manual_file_that_arrives_during_extraction():
    config = {
        'files': [
            {'name': 'manual', 'backup_name': 'manual.csv', 'extract_func': None},
            {'name': 'downloaded', 'backup_name': 'downloaded.csv', 'extract_func': Mock()},
        ]
    }
    drive = Mock()
    drive.get_files_in_folder.side_effect = [[], [{'name': 'manual.csv'}]]

    with patch.object(etl, 'Drive', drive):
        overview = etl.extract_data(config)

    assert overview['status'] == 'success'
    assert overview['summary'] == {'total': 2, 'successful': 1, 'skipped': 1, 'failed': 0}
    assert overview['steps'][0] == {'name': 'manual', 'status': 'skipped'}


def test_extract_keeps_truly_missing_manual_file_as_failure():
    config = {
        'files': [
            {'name': 'manual', 'backup_name': 'manual.csv', 'extract_func': None},
        ]
    }
    drive = Mock()
    drive.get_files_in_folder.side_effect = [[], []]

    with patch.object(etl, 'Drive', drive):
        overview = etl.extract_data(config)

    assert overview['status'] == 'partial'
    assert overview['summary']['failed'] == 1
    assert overview['steps'][0]['status'] == 'failed'
