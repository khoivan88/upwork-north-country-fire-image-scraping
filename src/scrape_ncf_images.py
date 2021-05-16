import csv
import json
import logging
import re
import sys
from functools import partial
from multiprocessing import Pool
from pathlib import Path, PurePath
from urllib.parse import urlparse

import scrapy
from furl import furl
from itemadapter import ItemAdapter
from PIL import Image
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import BarColumn, Progress, SpinnerColumn, TimeElapsedColumn
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import DropItem

from items import ImageItem

console = Console()
sys.setrecursionlimit(20000)

# # Set logger using Rich: https://rich.readthedocs.io/en/latest/logging.html
# logging.basicConfig(
#     level="INFO",
#     format="%(message)s",
#     datefmt="[%X]",
#     handlers=[RichHandler(rich_tracebacks=True)]
# )
# log = logging.getLogger("rich")


CURRENT_FILEPATH = Path(__file__).resolve().parent
DATA_FOLDER = CURRENT_FILEPATH / 'data'
DATA_FOLDER.mkdir(exist_ok=True)

# INPUT_FILE = DATA_FOLDER / 'imageNames-test.csv'
INPUT_FILE = DATA_FOLDER / 'imageNames.csv'
IMAGE_NOT_FOUND_RESULT_FILE = DATA_FOLDER / 'images_not_found_ncf.csv'

LOG_FOLDER = CURRENT_FILEPATH / 'logs'
LOG_FOLDER.mkdir(exist_ok=True)
LOG_FILE = LOG_FOLDER / 'scrape_log_ncf.log'

DOWNLOAD_FOLDER = CURRENT_FILEPATH.parent / 'images' / 'downloads'
DOWNLOAD_FOLDER.mkdir(exist_ok=True)


from scrapy.pipelines.files import FilesPipeline

# TODO: fix for item with SKU is '???'

# TODO: TEST: find correct url for 'GL10B', 'GL10FR'
# TODO: TEST: find correct url for 'vdy24/18nmp', 'RAK35/40', 'TM/R2-A', 'CEG-SMOKES/5', 'GC-40/15', 'IFV2-100/15', TH-WTC/LP'
# TODO: TEST: for those found match but without any images, such as '10K81+Thurmalox'
# TODO: TEST: for those with input in different cases (lower vs upper), such as '58dva-wtec', rf571', 'w175-0726'
# TODO: TEST: for those "HDLOGS-ODCOUG, GR-ODCOUG", "SDLOGS-ODCOUG, GR-ODCOUG"
# TODO: TEST: for those "BZLB-BLNI RAP54", "BZLB-BLNI RAP42", 'MHS HEAT-ZONE-TOP'
# TODO: FIX: for those say 'webpage found but no data'

class MyFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        # Save to 'brand' folder with the sku (lower case) as filename
        extension = Path(urlparse(request.url).path).suffix
        file_path = f"{item['image_brand']}/{item['image_name']}{extension}"
        # console.log(f'{file_path=}')
        return file_path

    # def get_media_requests(self, item, info):
    #     '''This is a web to disable FilesPipeline duplicate filter
    #     using url parameter and request header
    #     to trick scrapy into thinking it is different item
    #     Ref: https://stackoverflow.com/a/27756421/6596203

    #     Another way: https://stackoverflow.com/a/45234135/6596203
    #     However, not good since it would prevent updating Scrapy
    #     '''
    #     adapter = ItemAdapter(item)
    #     for file_url in adapter['file_urls']:
    #         request = scrapy.Request(f"{file_url}&image_name={item['image_name']}")
    #         request.meta['item'] = item
    #         request.headers['fpBuster'] = item['image_name']
    #         yield request


class ImageWriterPipeline:
    def process_item(self, item, spider):
        file = DOWNLOAD_FOLDER / item['image_brand'] / f"{item['image_name']}{item['image_extension']}"
        with open(file, 'wb') as f:
            f.write(item['image_body'])
        return item


def import_item_list(file):
    with open(file, 'r', newline='') as csv_file:
        dict_reader = csv.DictReader(csv_file)
        yield from dict_reader


def write_not_found_item_to_csv(file, line):
    '''Write item without found images into csv file'''
    file_exists = Path(file).exists()
    with open(Path(file), 'a') as csvfile:
        headers = list(line.keys())
        writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n',fieldnames=headers)

        if not file_exists:
            writer.writeheader()  # file doesn't exist yet, write a header

        writer.writerow(line)


class NCFImageSpider(scrapy.Spider):
    item_list = import_item_list(INPUT_FILE)
    name = 'ncf-images-spider'
    # allowed_domains = ['ultimate-dot-acp-magento.appspot.com', 'www.northcountryfire.com']

    # item_sku_list = (item["manufacturerSKU"] for item in item_list)
    # start_urls = [f'https://ultimate-dot-acp-magento.appspot.com/?q={sku}&store_id=14034773&UUID=34efc3a6-91d4-4403-99fa-5633d6e9a5bd'
    #               for sku in item_sku_list]

    # base_url = 'https://ultimate-dot-acp-magento.appspot.com/'
    # handle_httpstatus_list = [301, 302]

    # Use `start_requests()` to be able to pass the whole csv line as keyword arguments into `parse`
    # this way, csv line info for not found image can be written into result file
    def start_requests(self):
        for item in self.item_list:
            sku_string = item['manufacturerSKU']
            brand = item['brand']

            url = f'https://ultimate-dot-acp-magento.appspot.com/?q={sku_string}+{brand}&store_id=14034773&UUID=34efc3a6-91d4-4403-99fa-5633d6e9a5bd'
            yield scrapy.Request(url, callback=self.parse, cb_kwargs=item,
                                 dont_filter=True
                                 )

            # # Sometimes, the 'manufacturerSKU' contains items with commas:
            # # e.g.  "HDLOGS-ODCOUG, GR-ODCOUG", "SDLOGS-ODCOUG, GR-ODCOUG", "DLE, RAK35/40"
            # # In these cases, split the on commas and search for each SKU
            # skus = [sku.strip() for sku in sku_string.split(',')]
            # if len(skus) > 1:
            #     console.log(f'This line has manufacturerSKU "{sku_string}".')

            #     # console.log(f'Searching for "{sku_string}" now...')
            #     # guessed_url = f"https://www.northcountryfire.com/products/{item['ID']}"
            #     # yield scrapy.Request(url=guessed_url,
            #     #         # callback=self.parse_guessed_url,
            #     #         callback=self.parse,
            #     #         # errback=self.errback_guessed_url,
            #     #         cb_kwargs=item)

            #     separated_search_terms_string = ', '.join(f'"{term}"' for term in skus)
            #     console.log(f'Searching separately for {separated_search_terms_string} now...')
            #     urls = [f'https://ultimate-dot-acp-magento.appspot.com/?q={sku}+{brand}&store_id=14034773&UUID=34efc3a6-91d4-4403-99fa-5633d6e9a5bd'
            #             for sku in skus]
            #     for url, sku in zip(urls, skus):
            #         item.update({'manufacturerSKU': sku, 'mainImageName(.png)': sku.lower()})
            #         yield scrapy.Request(url, callback=self.parse,
            #                              cb_kwargs=item,
            #                             #  dont_filter=True
            #                              )

    def parse(self, response, **item):
        json_res = json.loads(response.body)

        # Check for special manufacturerSKU containing spaces
        # e.g: "BZLB-BLNI RAP54", "BZLB-BLNI RAP42", 'MHS HEAT-ZONE-TOP'
        number_of_word = len(item['manufacturerSKU'].split(' '))
        exact_match = next((match
                            for match in json_res['items']
                            if (item['manufacturerSKU'].lower() == ' '.join(match['skus'][:number_of_word]).lower().rstrip(',')     # Sometimes, item returned by the API has SKU ends with comma (such as 'DLE,')
                                and item['brand'].lower() == match['v'].lower()             # match return in API has 'v' containing 'brand'
                                and match['t2']                             # match return in API has 't2' containing image url
                                )
                            ),
                           None)

        exact_match_without_image = {}
        if not exact_match:
            number_of_word = len(item['manufacturerSKU'].split(' '))
            exact_match_without_image = (
                next((match
                      for match in json_res['items']
                      if (item['manufacturerSKU'].lower() == ' '.join(match['skus'][:number_of_word]).lower().rstrip(',')     # Sometimes, item returned by the API has SKU ends with comma (such as 'DLE,')
                          and item['brand'].lower() == match['v'].lower())),
                     None)
            )

        # Have to check this condition before the other
        if exact_match_without_image:
            item['comment'] = 'manufacturerSKU found but has NO images'
            write_not_found_item_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE,
                                        line=item)
            return None

        # If the API returns no match at all,
        if (json_res['total_results'] == 0 or not exact_match):
            guessed_url = f"https://www.northcountryfire.com/products/{item['ID']}"
            yield scrapy.Request(url=guessed_url,
                                 callback=self.parse_guessed_url,
                                 errback=self.errback_guessed_url,
                                 cb_kwargs=item,
                                 dont_filter=True
                                 )
            return None

        desired_image_url = exact_match['t2'].replace('_small.', '_1000x1000.')
        # image_extension = re.findall(r'.*(\.\w+)\?.*', desired_image_url)[0]

        # Some sku contain forward slash, not good for filename, e.g 'VDY24/18NMP', 'RAK35/40'
        # or space, e.g. "BZLB-BLNI RAP54", "BZLB-BLNI RAP42", 'MHS HEAT-ZONE-TOP'
        desired_image_name = item['mainImageName(.png)'].replace('/', '_').replace(' ', '-')

        # Add url query as a way to distinguish URL,
        # therefore can trick FilesPipeline into disable duplicate filter
        desired_image_url = furl(desired_image_url).add({'image_name': desired_image_name}).url

        brand = item['brand']
        item = {
            'image_name': desired_image_name,
            'image_brand': brand,
            # 'image_extension': image_extension,
            'file_urls': [desired_image_url],
        }
        yield ImageItem(item)
        # yield scrapy.Request(url=desired_image_url,
        #                      callback=self.parse_image,
        #                      dont_filter=True,
        #                      cb_kwargs=item)

    def parse_guessed_url(self, response, **item):
        data = re.findall("var product =(.+?);\n", response.text, re.S)
        if data:
            image_url = json.loads(data[0])['featured_image']
            if not image_url:
                item['comment'] = "Product webpage found via direct link with 'ID' field but no image"
                write_not_found_item_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE,
                                            line=item)
            desired_image_url = re.sub(r'(.*)(\.\w+\?.*)', 'https:\\1_1000x1000\\2', image_url)
            # image_extension = re.findall(r'.*(\.\w+)\?.*', desired_image_url)[0]

            # Some sku contain forward slash, not good for filename, e.g 'VDY24/18NMP', 'RAK35/40'
            # or space, e.g. "BZLB-BLNI RAP54", "BZLB-BLNI RAP42", 'MHS HEAT-ZONE-TOP'
            desired_image_name = item['mainImageName(.png)'].replace('/', '_').replace(' ', '-')

            # Add url query as a way to distinguish URL,
            # therefore can trick FilesPipeline into disable duplicate filter
            desired_image_url = furl(desired_image_url).add({'image_name': desired_image_name}).url

            brand = item['brand']
            item = {
                'image_name': desired_image_name,
                'image_brand': brand,
                # 'image_extension': image_extension,
                'file_urls': [desired_image_url],
            }
            yield ImageItem(item)
            # yield scrapy.Request(url=desired_image_url,
            #                      callback=self.parse_image,
            #                      dont_filter=True,
            #                      cb_kwargs=item)
        else:
            item['comment'] = "No product via direct link with 'ID' field"
            write_not_found_item_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE,
                                        line=item)

    def errback_guessed_url(self, failure):
        # Ref: https://docs.scrapy.org/en/latest/topics/request-response.html#accessing-additional-data-in-errback-functions
        item = failure.request.cb_kwargs
        item['comment'] = "manufacturerSKU not found through either quick-search API or direct link with 'ID' field"
        write_not_found_item_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE,
                                    line=item)

    def parse_image(self, response, **item):
        item['image_body'] = response.body
        yield ImageItem(item)


if __name__ == '__main__':
    # Remove the result file if exists
    IMAGE_NOT_FOUND_RESULT_FILE.unlink(missing_ok=True)
    LOG_FILE.unlink(missing_ok=True)

    settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
        # 'HTTPCACHE_ENABLED': False,
        # 'DEFAULT_REQUEST_HEADERS': {
        #   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        #   'Accept-Language': 'en'
        # },
        # 'CSV_EXPORT_FILE': THIS_SPIDER_RESULT_FILE,
        'MEDIA_ALLOW_REDIRECTS': True,
        'FILES_STORE': str(DOWNLOAD_FOLDER),
        # 'MYFILESPIPELINE_FILES_EXPIRES': 0,
        'ITEM_PIPELINES': {
            # 'scrapy.pipelines.images.FilesPipeline': 1,
            '__main__.MyFilesPipeline': 1,
            # '__main__.ImageWriterPipeline': 2,
        },
        # 'FEEDS': {
        #     Path(THIS_SPIDER_RESULT_FILE): {
        #         'format': 'json',
        #         'encoding': 'utf8',
        #         'indent': 2,
        #         # 'fields': FIELDS_TO_EXPORT,
        #         'fields': None,
        #         'overwrite': True,
        #         'store_empty': False,
        #         'item_export_kwargs': {
        #             'export_empty_fields': True,
        #         },
        #     },
        # },
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': LOG_FILE,
        'ROBOTSTXT_OBEY': False,
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(NCFImageSpider)
    process.start()
    # with console.status("[bold green]Scraping images...") as status:
    #     process.start()
