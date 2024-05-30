import tabula
import pandas as pd
from sqlalchemy import create_engine
import logging

def read_bank_statement(pdf_path, log_enabled=True):
    if log_enabled:
        logging.basicConfig(filename='data_processing.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        if not pdf_path.lower().endswith('.pdf'):
            if log_enabled:
                logging.error('The provided file is not a PDF.')
            return None
        tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True, lattice=True)
        df = pd.concat(tables)

        df.reset_index(drop=True, inplace=True)

    # Select required columns and rename them
        df1 = df[['Transaction\rDate', 'Description', 'Debit', 'Credit']]
        df1.columns = ['Date', 'Description', 'Debit', 'Credit']

        df1 = df1.dropna(subset=['Date'])

    # Create an engine to connect to your database
        engine = create_engine('sqlite:///bank_statement.db') 

    # Store the DataFrame in the SQL database
        df1.to_sql('bank_statement', engine, if_exists='replace', index=False)
        if log_enabled:
            logging.info('Data processing completed successfully.')

        return df1
    except Exception as e:  
        if log_enabled:
            logging.error(f'An error occurred: {str(e)}')
        return None
    # data store in sqllit table and demo data export in bank_statements,csv file 

read_bank_statement('yes.pdf')
