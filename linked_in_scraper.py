from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
from scrapy.selector import Selector
from datetime import datetime
from pytz import timezone
from pathlib import Path
import pymongo
import pprint
import time


class LinkedInScraper:

    def __init__(self):
        self.mongocon = pymongo.MongoClient("mongodb://localhost:27017/bot_scrapy")
        db = self.mongocon["Local_DB"]
        self.index_col = db["selenium_crawled_index"]
        self.product_col = db["selenium_scrapde_product"]
        now = datetime.now(timezone('UTC'))
        self.scrap_datetime = now.strftime("%Y-%m-%d %H:%M:%S")
        self.vpn_path = os.path.abspath(str(Path.home())+'/.selenium/urbanvpn')
        self.insert_count = 0

    def web_open(self):
        options = Options()
        options.add_argument('--log-level=3')
        options.add_argument(f'--load-extension={self.vpn_path}')
        self.driver = uc.Chrome(options=options)
        self.driver.get("https://www.linkedin.com/login")
        self.driver.implicitly_wait(10)
        cookies = self.driver.get_cookies()
        for cookie in cookies:
            self.driver.add_cookie(cookie)

    def login(self):
        self.driver.find_element(By.ID, "username").send_keys("")
        self.driver.find_element(By.ID, "password").send_keys("")
        time.sleep(2)
        self.driver.find_element(By.CLASS_NAME, "btn__primary--large").click()

    def get_clean_text(self, xpath_expr, response):
        return ' '.join(text.strip() for text in response.xpath(xpath_expr).extract() if isinstance(text, str) and text.strip())

    def get_profile_data(self, profile_url):
        self.driver.get(profile_url)
        time.sleep(5)
        html = self.driver.page_source
        response = Selector(text=html)

        name = response.xpath('//h1//text()').get()
        headline = self.get_clean_text('//div[@class="text-body-medium break-words"]//text()', response)
        address = self.get_clean_text('//span[@class="text-body-small inline t-black--light break-words"]'
                                      '//text()', response)
        total_connections = response.xpath('//span[@class="t-black--light"]//span[@class="t-bold"]//text()').get()
        company_name = self.get_clean_text('//button[contains(@aria-label,"Current company")]'
                                           '//span/div//text()', response)
        followers = response.xpath('substring-before(//p[contains(@class,"pvs-header__optional-link text-body-small")]'
                                   '//span[@class="pvs-entity__caption-wrapper"]//text(),"followers")').get()
        about = self.get_clean_text('//div[@class="display-flex ph5 pv3"]//span[@aria-hidden="true"]//text()', response)

        # Activity extraction
        activity = []
        for i in response.xpath('//ul[@class="artdeco-carousel__slider ember-view"]//@data-item-index').extract():
            comenter_name = response.xpath(f'//li[@data-item-index={i}]//span[@class="update-components-actor__title"]//span[@dir="ltr"]//span//text()').get()
            posted_date = response.xpath(f"//li[@data-item-index={i}]//span[contains(@class, 'update-components-actor__sub-description')]//span[1]//text()").get()
            post_content = self.get_clean_text(f"//li[@data-item-index={i}]//div[contains(@class, 'update-components-text')]//span[@dir='ltr'][1]//text()", response)
            image = self.get_clean_text(f"//li[@data-item-index={i}]//button[@class='update-components-image__image-link']//img/@src", response)
            if any([comenter_name, posted_date, post_content, image]):
                activity.append([comenter_name, posted_date, post_content, image])

        experience = response.xpath('//a[@data-field="experience_company_logo"]//span[@aria-hidden="true"]//text()').extract()
        education = response.xpath('//span[contains(text(),"Education")]/following::span[@aria-hidden="true"]//text()').extract()

        for stop_word in ["Projects", "Volunteering", "Recommendations", "Licenses & certifications", "Skills", "Interests"]:
            if stop_word in education:
                education = education[:education.index(stop_word)]
                break

        return {
            'name': name,
            'profile_url': profile_url,
            'headline': headline,
            'company_name': company_name,
            'total_connections': total_connections,
            'address': address,
            'about': about,
            'activity': activity,
            'experience': experience,
            'education': education,
            'followers': followers,
            'scrap_datetime': self.scrap_datetime,
            'crawler_name': 'linked_in_profiles'
        }

    def finding_profile_data(self):
        urls = self.index_col.find({'crawler_name': 'linked_in_profiles', 'status': False}, {'profile_url': 1})
        for doc in urls:
            profile_url = doc['profile_url']
            try:
                data = self.get_profile_data(profile_url)
                pprint.pprint(data)
                self.product_col.update_one({"profile_url": profile_url}, {"$set": data}, upsert=True)
                self.index_col.update_one({"profile_url": profile_url}, {'$set': {'status': True}}, upsert=True)
                self.insert_count += 1
                print('Inserted:', self.insert_count)
            except Exception as e:
                print(f"Error processing {profile_url}: {e}")

    def driver_close(self):
        self.driver.quit()


if __name__ == "__main__":
    scraper = LinkedInScraper()
    scraper.web_open()
    scraper.login()
    scraper.finding_profile_data()
    scraper.driver_close()
