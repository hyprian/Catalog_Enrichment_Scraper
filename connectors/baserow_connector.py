# Catalog_Enrichment_Scraper/connectors/baserow_connector.py
import requests
import pandas as pd
import logging

# Basic logging configuration for this module
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaserowConnector:
    def __init__(self, api_token, base_url):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Token {api_token}",
            "Content-Type": "application/json"
        }
        if not api_token:
            logger.error("Baserow API token is not provided.")
            raise ValueError("Baserow API token is required.")

    def get_table_as_dataframe(self, table_id):
        logger.info(f"Fetching data for Baserow table ID: {table_id}")
        all_rows = []
        page = 1
        size = 200
        while True:
            url = f"{self.base_url}/api/database/rows/table/{table_id}/?user_field_names=true&page={page}&size={size}"
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                results = data.get("results", [])
                all_rows.extend(results)
                if data.get("next") is None or not results:
                    break
                page += 1
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data from Baserow table {table_id}, page {page}: {e}")
                raise
        
        if not all_rows:
            logger.warning(f"No data found in Baserow table {table_id}.")
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        logger.info(f"Successfully fetched and processed {len(df)} rows from table {table_id}.")
        return df

    def update_rows(self, table_id, rows_data):
        """
        Updates one or more rows in a Baserow table, automatically handling batching.
        """
        if not rows_data:
            logger.info("No rows data provided to update.")
            return True # Consider it a success if there's nothing to do

        url = f"{self.base_url}/api/database/rows/table/{table_id}/batch/?user_field_names=true"
        batch_size = 200  # Baserow's hard limit
        overall_success = True

        for i in range(0, len(rows_data), batch_size):
            chunk = rows_data[i:i + batch_size]
            payload = {"items": chunk}
            
            logger.info(f"Updating chunk {i//batch_size + 1}/{(len(rows_data)-1)//batch_size + 1} with {len(chunk)} items...")
            
            try:
                response = requests.patch(url, headers=self.headers, json=payload)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to update chunk in table {table_id}: {e} - Response: {e.response.text}")
                overall_success = False
                # Optionally, you could stop here or continue with other chunks
                # For now, we'll log the error and continue
        
        return overall_success