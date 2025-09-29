# Catalog_Enrichment_Scraper/scrapers/amazon_scraper.py
import logging
import random
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

class AmazonScraper:
    def __init__(self, config):
        self.domain = config.get("domain", "amazon.in")
        self.min_delay = config.get("min_delay_seconds", 2.5)
        self.max_delay = config.get("max_delay_seconds", 5.5)
        self.base_url = f"https://www.{self.domain}/dp/"
        self.driver = self._setup_driver()

    def _setup_driver(self):
        logging.info("Setting up local Chrome driver...")
        options = Options()
        # options.add_argument("--headless=new")
        options.add_argument("start-maximized")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        try:
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            logging.info("Driver setup successful.")
            return driver
        except Exception as e:
            logging.error(f"FATAL: Driver setup failed: {e}")
            return None

    def scrape_product(self, asin ):
        """
        Navigates to an Amazon product page and scrapes all required data.
        This is the full, original version of the scraper function.
        """
        if not self.driver:
            raise Exception("Driver is not initialized.")

        url = self.base_url + asin
        self.driver.get(url)
        wait = WebDriverWait(self.driver, 15)
        
        page_title = self.driver.title
        if "page not found" in page_title.lower() or "sorry! we couldn't find that page" in page_title.lower():
            logging.warning(f"ASIN {asin} resulted in a 'Page Not Found'.")
            return {'ASIN': asin, 'page_status': 'not_found'}

        scraped_data = {'ASIN': asin, 'URL': url}

        try: scraped_data['Title'] = wait.until(EC.presence_of_element_located((By.ID, 'productTitle'))).text.strip()
        except Exception: scraped_data['Title'] = None
        try: scraped_data['Brand'] = self.driver.find_element(By.ID, 'bylineInfo').text.strip()
        except Exception: scraped_data['Brand'] = None
        try:
            price_whole = self.driver.find_element(By.CSS_SELECTOR, '.a-price-whole').text.strip()
            price_symbol = self.driver.find_element(By.CSS_SELECTOR, '.a-price-symbol').text.strip()
            scraped_data['Price'] = f"{price_symbol}{price_whole}"
        except Exception: scraped_data['Price'] = None
        try: scraped_data['Rating'] = self.driver.find_element(By.ID, 'acrPopover').get_attribute('title').strip()
        except Exception: scraped_data['Rating'] = None
        try: scraped_data['Review Count'] = self.driver.find_element(By.ID, 'acrCustomerReviewText').text.strip()
        except Exception: scraped_data['Review Count'] = None
        try:
            bullet_points_ul = self.driver.find_element(By.ID, 'feature-bullets')
            bullets = bullet_points_ul.find_elements(By.TAG_NAME, 'li')
            scraped_data['Bullet Points'] = '\n'.join([bullet.text.strip() for bullet in bullets if bullet.text.strip()])
        except Exception: scraped_data['Bullet Points'] = None
        try:
            image_urls = set()
            thumbnails = self.driver.find_elements(By.CSS_SELECTOR, 'li.thumbnail img')
            for thumb in thumbnails:
                img_url = thumb.get_attribute('src').split('._')[0] + '._AC_SL1500_.jpg'
                image_urls.add(img_url)
            if not image_urls:
                main_image = self.driver.find_element(By.ID, 'landingImage')
                image_urls.add(main_image.get_attribute('src'))
            scraped_data['Image URLs'] = ', '.join(list(image_urls)) if image_urls else None
        except Exception: scraped_data['Image URLs'] = None
        try: scraped_data['Product Description'] = self.driver.find_element(By.ID, 'productDescription').text.strip()
        except Exception: scraped_data['Product Description'] = None
        
        scraped_data['Status'] = 'Active' if scraped_data['Title'] else 'Inactive'
        scraped_data['page_status'] = 'found'
        
        time.sleep(random.uniform(self.min_delay, self.max_delay))
        return scraped_data

    def quit_driver(self):
        if self.driver:
            logging.info("Closing the browser.")
            self.driver.quit()