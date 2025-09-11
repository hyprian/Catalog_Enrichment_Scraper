# Catalog_Enrichment_Scraper/main.py
import yaml
import logging
import os
import json
from datetime import datetime
from connectors.baserow_connector import BaserowConnector
from scrapers.amazon_scraper import AmazonScraper

# --- Custom JSON Logger Setup ---
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
    """
    Configures logging to write to a specific, timestamped JSON file.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # Clear any existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # File handler for the unique JSON log file
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)
    
    # Console handler for human-readable output during the run
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)

def load_config(path="config.yaml"):
    """Loads the YAML configuration file."""
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
            logging.info("Configuration loaded successfully.")
            return config
    except FileNotFoundError:
        logging.error(f"FATAL: Configuration file not found at '{path}'."); return None
    except Exception as e:
        logging.error(f"FATAL: Error loading configuration file: {e}"); return None
    
# --- NEW: Checkpoint Functions ---
CHECKPOINT_FILE = "checkpoint.json"

def save_checkpoint(processed_asins):
    """Saves the list of processed ASINs to a file."""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(list(processed_asins), f)
    logging.info(f"Checkpoint saved. {len(processed_asins)} ASINs processed so far.")

def load_checkpoint():
    """Loads the list of processed ASINs from a file if it exists."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            processed_asins = set(json.load(f))
            logging.warning(f"Checkpoint file found. Resuming from last run. {len(processed_asins)} ASINs will be skipped.")
            return processed_asins
    return set()

# --- NEW HELPER FUNCTION FOR ANCILLARY UPDATES ---
def update_ancillary_tables(config, connector, catalogue_df, successful_updates):
    """
    Updates the status and timestamp in ancillary tables based on successfully scraped items.
    """
    ancillary_config = config.get('ancillary_table_updates', [])
    if not ancillary_config:
        logging.info("No ancillary tables configured for updates.")
        return

    active_asins = {update['ASIN'] for update in successful_updates}
    if not active_asins:
        logging.info("No successful scrapes to use for ancillary updates.")
        return
        
    asin_to_msku = catalogue_df.set_index('Marketplace ASIN/Product ID')['msku'].to_dict()
    active_mskus = {asin_to_msku.get(asin) for asin in active_asins if asin_to_msku.get(asin)}

    logging.info(f"Found {len(active_asins)} active ASINs and {len(active_mskus)} corresponding MSKUs for ancillary updates.")

    current_timestamp = datetime.now().isoformat() # Get a single timestamp for this run

    for table_config in ancillary_config:
        if not table_config.get('enabled'):
            logging.info(f"Skipping ancillary table '{table_config.get('name')}' as it is disabled.")
            continue

        name = table_config.get('name')
        table_id = table_config.get('table_id')
        match_col = table_config.get('match_column')

        if not all([name, table_id, match_col]):
            logging.error(f"Skipping ancillary table due to incomplete configuration: {table_config}")
            continue

        logging.info(f"--- Processing ancillary table: {name} ---")
        df_to_update = connector.get_table_as_dataframe(table_id)
        if df_to_update.empty:
            logging.warning(f"Table '{name}' is empty, skipping update.")
            continue
        
        if match_col == 'Asin':
            rows_to_activate = df_to_update[df_to_update[match_col].isin(active_asins)]
        elif match_col == 'Msku':
            rows_to_activate = df_to_update[
                (df_to_update[match_col].isin(active_mskus)) &
                (df_to_update['Panel'] == 'Amazon')
            ]
        else:
            logging.warning(f"Unknown match_column '{match_col}' for table '{name}'. Skipping.")
            continue

        if not rows_to_activate.empty:
            ids_to_update = rows_to_activate['id'].tolist()
            
            # --- THIS IS THE ONLY CHANGE ---
            # The payload now includes the 'Last Enriched At' timestamp.
            payload = [{'id': row_id, 'Status': 'Active', 'Last Enriched At': current_timestamp} for row_id in ids_to_update]
            
            logging.info(f"Found {len(ids_to_update)} rows in '{name}' to update to 'Active'.")
            
            if connector.update_rows(table_id, payload):
                logging.info(f"✅ Successfully updated status in '{name}'.")
            else:
                logging.error(f"❌ Failed to update status in '{name}'.")
        else:
            logging.info(f"No matching rows found in '{name}' to update.")

def main(start_time):
    """Main function to run the enrichment process."""
    logging.info("🚀 Starting the Standalone Catalog Enrichment Scraper...")
    
    config = load_config()
    if not config: return

    # --- 1. Fetch data from Baserow ---
    baserow_config = config.get('baserow', {})
    table_id = baserow_config.get('catalogue_table_id')
    
    if not all([baserow_config.get('api_token'), baserow_config.get('base_url'), table_id]):
        logging.error("FATAL: Baserow configuration is incomplete in config.yaml."); return

    connector = BaserowConnector(api_token=baserow_config['api_token'], base_url=baserow_config['base_url'])
    catalogue_df = connector.get_table_as_dataframe(table_id)
        
    if catalogue_df.empty:
        logging.warning("The Catalogue table is empty. Nothing to process."); return
            
    asins_to_scrape = catalogue_df[
        catalogue_df['Marketplace ASIN/Product ID'].notna() &
        (catalogue_df['Marketplace ASIN/Product ID'].str.strip() != '')
    ].copy()

    processed_asins = load_checkpoint()
    if processed_asins:
        asins_to_scrape = asins_to_scrape[~asins_to_scrape['Marketplace ASIN/Product ID'].isin(processed_asins)]

    
    # --- Apply the scrape limit from config ---
    scrape_limit = config.get('scraper', {}).get('max_items_to_scrape', 0)
    if scrape_limit > 0:
        logging.info(f"Limiting run to the first {scrape_limit} items based on configuration.")
        asins_to_scrape = asins_to_scrape.head(scrape_limit)
    else:
        logging.info("Processing all available items.")
    
    logging.info(f"Processing a total of {len(asins_to_scrape)} listings.")

    if asins_to_scrape.empty:
        logging.info("No new items to process. Exiting.")
        if os.path.exists(CHECKPOINT_FILE): os.remove(CHECKPOINT_FILE) # Clean up if nothing to do
        return

    
    # --- 2. Initialize the Scraper ---
    scraper = AmazonScraper(config=config.get('scraper', {}))
    if not scraper.driver: return

    updates_to_send = []
    failed_asins_for_retry = []
    checkpoint_counter = 0
    
    try:
        # --- 3. First Scraping Pass ---
        for i, row in asins_to_scrape.iterrows():
            baserow_id, asin = row['id'], row['Marketplace ASIN/Product ID']
            logging.info(f"Scraping (Pass 1) [{i+1}/{len(asins_to_scrape)}] for ASIN: {asin}")
            try:
                product_data = scraper.scrape_product(asin)
                if product_data.get('page_status') == 'not_found':
                    updates_to_send.append({'id': baserow_id, 'Listing Status': 'Deleted', 'Enrichment Status': 'ASIN Not Found'})
                    continue
                if product_data.get('Title'):
                    product_data['id'] = baserow_id
                    updates_to_send.append(product_data)
                else: 
                    raise ValueError("Scrape returned no title (but page was found).")
                processed_asins.add(asin)
                checkpoint_counter += 1
            except Exception as e:
                logging.warning(f"Could not scrape ASIN {asin} on first pass. Adding to retry list. Error: {e}")
                failed_asins_for_retry.append({'id': baserow_id, 'ASIN': asin})
                processed_asins.add(asin) # Also mark failed items as processed so we don't retry them on next full run
                checkpoint_counter += 1

            # --- NEW: Save checkpoint periodically ---
            if checkpoint_counter >= 50:
                save_checkpoint(processed_asins)
                checkpoint_counter = 0

        # --- 4. Retry Pass for Failed ASINs ---
        if failed_asins_for_retry:
            logging.info(f"\n--- Retrying {len(failed_asins_for_retry)} failed ASINs ---")
            for i, item in enumerate(failed_asins_for_retry):
                baserow_id, asin = item['id'], item['ASIN']
                logging.info(f"Scraping (Pass 2) [{i+1}/{len(failed_asins_for_retry)}] for ASIN: {asin}")
                try:
                    product_data = scraper.scrape_product(asin)
                    if product_data.get('page_status') == 'not_found':
                        updates_to_send.append({'id': baserow_id, 'Listing Status': 'Deleted', 'Enrichment Status': 'ASIN Not Found'})
                        continue
                    if product_data.get('Title'):
                        product_data['id'] = baserow_id
                        product_data['Enrichment Status'] = 'Success (on retry)'
                        updates_to_send.append(product_data)
                    else: raise ValueError("Retry scrape returned no title.")
                except Exception as e:
                    logging.error(f"Failed to scrape ASIN {asin} on second pass. Error: {e}")
                    updates_to_send.append({'id': baserow_id, 'Enrichment Status': 'Scrape Failed'})
    finally:
        scraper.quit_driver()
        save_checkpoint(processed_asins)

    # --- 5. Prepare and Update Baserow ---
    if updates_to_send:
        # ... (Payload preparation logic is unchanged) ...
        final_payload = []
        for update in updates_to_send:
            payload_item = {'id': update['id'], 'Enrichment Status': update.get('Enrichment Status', 'Success'), 'Last Enriched At': datetime.now().isoformat()}
            if update.get('Status'): payload_item['Listing Status'] = update['Status']
            if update.get('Title'): payload_item['Title'] = update['Title']
            if update.get('Brand'): payload_item['Brand'] = update['Brand']
            if update.get('Price'): payload_item['Price'] = update['Price']
            if update.get('Rating'): payload_item['Rating'] = update['Rating']
            if update.get('Review Count'): payload_item['Review Count'] = update['Review Count']
            if update.get('Bullet Points'): payload_item['Bullet Points'] = update['Bullet Points']
            if update.get('Product Description'): payload_item['Product Description'] = update['Product Description']
            if update.get('Image URLs'):
                payload_item['All Image URLs'] = update['Image URLs']
                first_image = update['Image URLs'].split(',')[0].strip()
                payload_item['Product Image 1'] = first_image
            final_payload.append(update)
        
        logging.info(f"\nSending {len(final_payload)} updates to Baserow...")
        if connector.update_rows(table_id, final_payload):
            logging.info("✅ Success! Main Catalogue table has been updated.")
        else:
            logging.error("❌ Failed to update main Catalogue table")
    else:
        logging.info("No new data to update in main Catalogue table.")

    successful_updates = [u for u in updates_to_send if u.get('Enrichment Status', 'Success') in ['Success', 'Success (on retry)']]
    update_ancillary_tables(config, connector, catalogue_df, successful_updates)

    logging.info("Run completed successfully. Deleting checkpoint file.")
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

    end_time = datetime.now()
    logging.info(f"Enrichment process finished. Total runtime: {end_time - start_time}")

if __name__ == "__main__":
    # --- THIS IS THE NEW, DYNAMIC LOGGING SETUP ---
    
    # 1. Define the logs directory and create it if it doesn't exist
    LOG_DIR = "logs"
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # 2. Generate a unique, timestamped filename
    start_time = datetime.now()
    timestamp = start_time.strftime("%Y-%m-%d_%H-%M-%S")
    log_file_path = os.path.join(LOG_DIR, f"enrichment_run_{timestamp}.json")
    
    # 3. Initialize the logging system with the unique file path
    setup_logging(log_file_path)
    
    # 4. Run the main application logic
    main(start_time)