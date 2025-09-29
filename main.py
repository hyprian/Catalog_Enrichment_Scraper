# Catalog_Enrichment_Scraper/main.py
import yaml
import logging
import os
import json
from datetime import datetime
import pandas as pd
from connectors.baserow_connector import BaserowConnector
from scrapers.amazon_scraper import AmazonScraper

# --- Custom JSON Logger Setup (Unchanged) ---
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage()
        }
        if hasattr(record, 'extra_data'):
            log_record.update(record.extra_data)
        return json.dumps(log_record)

def setup_logging(log_file_path: str):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)

def load_config(path="config.yaml"):
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
            logging.info("Configuration loaded successfully.")
            return config
    except FileNotFoundError:
        logging.error(f"FATAL: Configuration file not found at '{path}'."); return None
    except Exception as e:
        logging.error(f"FATAL: Error loading configuration file: {e}"); return None
    
# --- Checkpoint Functions (Unchanged) ---
CHECKPOINT_FILE = "checkpoint.json"

def save_checkpoint(processed_asins):
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(list(processed_asins), f)
    logging.info(f"Checkpoint saved. {len(processed_asins)} ASINs processed so far.")

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            processed_asins = set(json.load(f))
            logging.warning(f"Checkpoint file found. Resuming from last run. {len(processed_asins)} ASINs will be skipped.")
            return processed_asins
    return set()

# --- Ancillary Table Update Function is now REMOVED ---

def main(start_time):
    """Main function to run the enrichment process."""
    logging.info("🚀 Starting the Master Catalogue Enrichment Scraper...")
    
    config = load_config()
    if not config: return

    # --- 1. Fetch data from the Master Catalogue in Baserow ---
    baserow_config = config.get('baserow', {})
    # UPDATED: Use the new config key
    table_id = baserow_config.get('master_catalogue_table_id')
    
    if not all([baserow_config.get('api_token'), baserow_config.get('base_url'), table_id]):
        logging.error("FATAL: Baserow configuration is incomplete in config.yaml."); return

    connector = BaserowConnector(api_token=baserow_config['api_token'], base_url=baserow_config['base_url'])
    master_catalogue_df = connector.get_table_as_dataframe(table_id)
        
    if master_catalogue_df.empty:
        logging.warning("The Master Catalogue table is empty. Nothing to process."); return
    
    # Standardize column names for robust access
    master_catalogue_df.columns = [col.lower().strip().replace(' ', '_') for col in master_catalogue_df.columns]
    
    # UPDATED: Use the correct, standardized column name 'asin'
    asin_column_name = 'asin'
    if asin_column_name not in master_catalogue_df.columns:
        logging.error(f"FATAL: The required '{asin_column_name}' column was not found in the Master Catalogue table after standardization."); return

    # Get a unique, non-empty list of ASINs to scrape
    unique_asins = master_catalogue_df[
        master_catalogue_df[asin_column_name].notna() &
        (master_catalogue_df[asin_column_name].astype(str).str.strip() != '')
    ][asin_column_name].unique()

    if len(unique_asins) == 0:
        logging.info("No valid ASINs found in the Master Catalogue to scrape. Exiting.")
        return

    processed_asins = load_checkpoint()
    asins_to_scrape = [asin for asin in unique_asins if asin not in processed_asins]
    
    # Apply the scrape limit from config
    scrape_limit = config.get('scraper', {}).get('max_items_to_scrape', 0)
    if scrape_limit > 0:
        logging.info(f"Limiting run to the first {scrape_limit} ASINs based on configuration.")
        asins_to_scrape = asins_to_scrape[:scrape_limit]
    else:
        logging.info("Processing all available ASINs.")
    
    logging.info(f"Found {len(unique_asins)} unique ASINs in total. After checkpoint, processing {len(asins_to_scrape)} new ASINs.")

    if not asins_to_scrape:
        logging.info("No new items to process. Exiting.")
        if os.path.exists(CHECKPOINT_FILE): os.remove(CHECKPOINT_FILE)
        return

    # --- 2. Initialize the Scraper (Unchanged) ---
    scraper = AmazonScraper(config=config.get('scraper', {}))
    if not scraper.driver: return

    updates_to_send = []
    failed_asins_for_retry = []
    checkpoint_counter = 0
    
    try:
        # --- 3. First Scraping Pass ---
        for i, asin in enumerate(asins_to_scrape):
            logging.info(f"Scraping (Pass 1) [{i+1}/{len(asins_to_scrape)}] for ASIN: {asin}")
            try:
                product_data = scraper.scrape_product(asin)
                if product_data.get('page_status') == 'not_found':
                    product_data['Enrichment Status'] = 'ASIN Not Found'
                    product_data['Status'] = 'Deleted' # Update listing status
                updates_to_send.append(product_data) # Append regardless of success for status updates
                
                processed_asins.add(asin)
                checkpoint_counter += 1
            except Exception as e:
                logging.warning(f"Could not scrape ASIN {asin} on first pass. Adding to retry list. Error: {e}")
                failed_asins_for_retry.append(asin)
                processed_asins.add(asin)
                checkpoint_counter += 1

            if checkpoint_counter >= 50:
                save_checkpoint(processed_asins)
                checkpoint_counter = 0

        # --- 4. Retry Pass for Failed ASINs ---
        if failed_asins_for_retry:
            logging.info(f"\n--- Retrying {len(failed_asins_for_retry)} failed ASINs ---")
            for i, asin in enumerate(failed_asins_for_retry):
                logging.info(f"Scraping (Pass 2) [{i+1}/{len(failed_asins_for_retry)}] for ASIN: {asin}")
                try:
                    product_data = scraper.scrape_product(asin)
                    if product_data.get('page_status') == 'not_found':
                        product_data['Enrichment Status'] = 'ASIN Not Found'
                        product_data['Status'] = 'Deleted'
                    else:
                        product_data['Enrichment Status'] = 'Success (on retry)'
                    updates_to_send.append(product_data)
                except Exception as e:
                    logging.error(f"Failed to scrape ASIN {asin} on second pass. Error: {e}")
                    updates_to_send.append({'ASIN': asin, 'Enrichment Status': 'Scrape Failed'})
    finally:
        scraper.quit_driver()
        save_checkpoint(processed_asins)

    # --- 5. Prepare and Update Baserow ---
    if updates_to_send:
        final_payload = []
        current_timestamp = datetime.now().isoformat()

        for update_data in updates_to_send:
            scraped_asin = update_data['ASIN']
            
            # Find all rows in the original DataFrame that match this ASIN
            matching_rows = master_catalogue_df[master_catalogue_df[asin_column_name] == scraped_asin]
            
            if matching_rows.empty:
                logging.warning(f"Could not find ASIN {scraped_asin} in the initially fetched data. Skipping update for it.")
                continue

            # Get the Baserow IDs for all matching rows
            row_ids_to_update = matching_rows['id'].tolist()
            
            # Prepare the common data to update for all these rows
            payload_item_data = {
                'Enrichment Status': update_data.get('Enrichment Status', 'Success'),
                'Last Enriched At': current_timestamp
            }
            # Add scraped fields if they were found
            if update_data.get('Status'): payload_item_data['Status'] = update_data['Status']
            if update_data.get('Title'): payload_item_data['Title'] = update_data['Title']
            if update_data.get('Brand'): payload_item_data['Brand'] = update_data['Brand']
            if update_data.get('Price'): payload_item_data['Price'] = update_data['Price']
            if update_data.get('Rating'): payload_item_data['Rating'] = update_data['Rating']
            if update_data.get('Review Count'): payload_item_data['Review Count'] = update_data['Review Count']
            if update_data.get('Bullet Points'): payload_item_data['Bullet Points'] = update_data['Bullet Points']
            if update_data.get('Product Description'): payload_item_data['Product Description'] = update_data['Product Description']
            if update_data.get('Image URLs'):
                payload_item_data['All Image URLs'] = update_data['Image URLs']
                first_image = update_data['Image URLs'].split(',')[0].strip()
                payload_item_data['Product Image 1'] = first_image
            
            # Create a payload entry for each matching row ID
            for row_id in row_ids_to_update:
                item = {'id': row_id}
                item.update(payload_item_data)
                final_payload.append(item)
        
        logging.info(f"\nSending a total of {len(final_payload)} row updates to Baserow...")
        if connector.update_rows(table_id, final_payload):
            logging.info("✅ Success! Master Catalogue table has been updated.")
        else:
            logging.error("❌ Failed to update Master Catalogue table.")
    else:
        logging.info("No new data to update in Master Catalogue table.")

    # The ancillary table update call is now removed.
    
    logging.info("Run completed successfully. Deleting checkpoint file.")
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

    end_time = datetime.now()
    logging.info(f"Enrichment process finished. Total runtime: {end_time - start_time}")

if __name__ == "__main__":
    LOG_DIR = "logs"
    os.makedirs(LOG_DIR, exist_ok=True)
    start_time = datetime.now()
    timestamp = start_time.strftime("%Y-%m-%d_%H-%M-%S")
    log_file_path = os.path.join(LOG_DIR, f"enrichment_run_{timestamp}.json")
    setup_logging(log_file_path)
    main(start_time)