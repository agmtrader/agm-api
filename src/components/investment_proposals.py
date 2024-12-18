from src.utils.api import access_api
from src.utils.logger import logger
from src.utils.response import Response

import pandas as pd

def backup_investment_proposals():

    logger.announcement("Starting backup of investment proposals")
    
    files = []

    response = access_api('/drive/get_files_in_folder', 'POST', {
        'parent_id': '0ANt_SwXfHuoDUk9PVA'
    })
    if response['status'] != 'success':
        logger.announcement("Failed to get yearly folders", 'error')
        return Response.error('Failed to get yearly folders')
    
    yearly_folders = response['content']

    for yearly_folder in yearly_folders:
        logger.info(f"Getting files in folder: {yearly_folder['name']}")
        response = access_api('/drive/get_files_in_folder', 'POST', {
            'parent_id': yearly_folder['id']
        })

        if response['status'] != 'success':
            logger.announcement("Failed to get files in folder", 'error')
            return Response.error('Failed to get files in folder')

        proposal_folders = response['content']

        for proposal_folder in proposal_folders:

            logger.announcement(f"Getting files for client: {proposal_folder['name']}")
            response = access_api('/drive/get_files_in_folder', 'POST', {
                'parent_id': proposal_folder['id']
            })

            for pdf_file in response['content']:
                if pdf_file['name'].endswith('.pdf'):
                    
                    split_file_name = pdf_file['name'].split(' ', 1)
                    split_folder_name = proposal_folder['name'].split(' ', 1)

                    try:
                        timestamp = split_file_name[0]
                    except:
                        try:
                            timestamp = split_folder_name[0]
                        except:
                            timestamp = ''
                            logger.warning(f"No timestamp found for file: {pdf_file['name']}")
                    try:
                        client_full_name = split_file_name[1].split('.')[0]
                    except:
                        try:
                            client_full_name = split_folder_name[1].split('.')[0]
                        except:
                            client_full_name = ''
                            logger.warning(f"No client full name found for file: {pdf_file['name']}")

                    files.append({
                        'FileInfo': pdf_file,
                        'ClientName': client_full_name,  # Keep full name with spaces
                        'YearEmmitted': yearly_folder['name'],
                        'Timestamp': timestamp  # New column for the timestamp
                    })

    response = access_api('/database/upload_collection', 'POST', {
        'path': 'db/document_center/investment_proposals',
        'data': files
    })

    if response['status'] != 'success':
        logger.announcement("Failed to upload investment proposals", 'error')
        return Response.error('Failed to upload investment proposals')

    logger.announcement("Backup of investment proposals completed", 'success')
    return Response.success('Backup of investment proposals completed')