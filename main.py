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
            
    asins_to_scrape = catalogue_df[catalogue_df['Marketplace ASIN/Product ID'].notna()].copy()
    
    # --- Apply the scrape limit from config ---
    scrape_limit = config.get('scraper', {}).get('max_items_to_scrape', 0)
    if scrape_limit > 0:
        logging.info(f"Limiting run to the first {scrape_limit} items based on configuration.")
        asins_to_scrape = asins_to_scrape.head(scrape_limit)
    else:
        logging.info("Processing all available items.")
    
    logging.info(f"Processing a total of {len(asins_to_scrape)} listings.")
    
    # --- 2. Initialize the Scraper ---
    scraper = AmazonScraper(config=config.get('scraper', {}))
    if not scraper.driver: return

    updates_to_send = []
    failed_asins_for_retry = []
    
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
            except Exception as e:
                logging.warning(f"Could not scrape ASIN {asin} on first pass. Adding to retry list. Error: {e}")
                failed_asins_for_retry.append({'id': baserow_id, 'ASIN': asin})

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
            final_payload.append(payload_item)
        
        logging.info(f"\nSending {len(final_payload)} updates to Baserow...")
        if connector.update_rows(table_id, final_payload):
            logging.info("✅ Success! Baserow has been updated.")
        else:
            logging.error("❌ Failed to update Baserow.")
    else:
        logging.info("No new data to update in Baserow.")

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