from src.utils.api import access_api
from src.utils.logger import logger
from src.utils.response import Response
from io import BytesIO
import pandas as pd

def migrate_account_accessess():
    logger.announcement('Migrating reports.', type='info')
    response = access_api('/drive/export_file', method='POST', data={'file_id': '1hhYzIBQ3sEVlmiYpiOP9sCyHXGUefajVU7WMvNgXLYk', 'mime_type': 'text/csv'})
    try:
        file_data = BytesIO(response)
    except:
        return Response.error(f'Error downloading file.')
    old_accounts_df = pd.read_csv(file_data)

    accounts_df = pd.DataFrame(old_accounts_df, columns=['AccountID', 'AccountNumber', 'IBKRPassword', 'IBKRUsername', 'TemporalEmail', 'TemporalPassword', 'TicketID'])

    accounts_df['AccountNumber'] = old_accounts_df['Numero de cuenta'].astype(str)
    accounts_df['IBKRPassword'] = old_accounts_df['Contraseña 1'].astype(str)
    accounts_df['IBKRUsername'] = old_accounts_df['Usuario'].astype(str)
    accounts_df['TemporalEmail'] = old_accounts_df['Dirección de Correo'].astype(str)
    accounts_df['TemporalPassword'] = old_accounts_df['Contraseña'].astype(str)
    accounts_df['Uploader'] = old_accounts_df['Dirección de correo electrónico'].astype(str)
    accounts_df = accounts_df.fillna('')

    logger.info(f'{accounts_df}')

    logger.info(f'Uploading accounts to database.')
    response = access_api('/database/upload_collection', method='POST', data={'path': 'db/clients/accounts', 'data': accounts_df.to_dict('records')})
    if response['status'] == 'error':
        logger.error(f'Error uploading accounts to database.')
        return Response.error('Error uploading accounts to database.')
    logger.success('Accounts uploaded to database.')

    return Response.success('Reports migrated.')