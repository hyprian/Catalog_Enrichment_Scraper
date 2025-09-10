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
        url = f"{self.base_url}/api/database/rows/table/{table_id}/batch/?user_field_names=true"
        payload = {"items": rows_data}
        try:
            response = requests.patch(url, headers=self.headers, json=payload)
            response.raise_for_status()
            logger.info(f"Successfully updated {len(rows_data)} row(s) in table {table_id}.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update rows in table {table_id}: {e} - Response: {e.response.text}")
            return None