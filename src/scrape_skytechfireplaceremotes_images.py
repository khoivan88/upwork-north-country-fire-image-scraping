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
# INPUT_FILE = DATA_FOLDER / 'images_not_found_ibuyfireplaces.csv'
INPUT_FILE = DATA_FOLDER / 'images_not_found_ncf.csv'
# INPUT_FILE = DATA_FOLDER / 'imageNames.csv'
IMAGE_NOT_FOUND_RESULT_FILE = DATA_FOLDER / 'images_not_found_skytechfireplaceremotes.csv'

LOG_FOLDER = CURRENT_FILEPATH / 'logs'
LOG_FOLDER.mkdir(exist_ok=True)
LOG_FILE = LOG_FOLDER / 'scrape_log_skytechfireplaceremotes.log'

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
        return f"{item['image_brand']}/{item['image_name']}{extension}"


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


class SkytechfireplaceremotesSpider(scrapy.Spider):
    item_list = import_item_list(INPUT_FILE)
    name = 'skytechfirelaceremotes-spider'

    # start_urls = [f'https://ultimate-dot-acp-magento.appspot.com/?q={sku}&store_id=14034773&UUID=34efc3a6-91d4-4403-99fa-5633d6e9a5bd'
    #               for sku in item_sku_list]

    # base_url = 'https://ultimate-dot-acp-magento.appspot.com/'

    # # iBuyFireplaces.com gave 500 error code on some pages even if the result is found.
    # Use this so scrapy does not ignore those with 500 error code
    # handle_httpstatus_list = [500]

    # Use `start_requests()` to be able to pass the whole csv line as keyword arguments into `parse`
    # this way, csv line info for not found image can be written into result file
    def start_requests(self):
        for item in self.item_list:
            # Only search for Skytech products.
            if item['brand'].lower() == 'Skytech'.lower():
                sku_string = item['manufacturerSKU']
                yield scrapy.Request(url=f'https://www.skytechfireplaceremotes.com/search/?q={sku_string}',
                                    callback=self.parse,
                                    errback=self.errback_guessed_url,
                                    cb_kwargs=item,
                                    dont_filter=True)
            else:
                item['comment'] = "manufacturerSKU is not searched on Skytechfireplaceremotes.com"
                write_not_found_item_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE,
                                            line=item)


    def parse(self, response, **item):
        # Some search redirected to a product immediately
        redirected = 0
        if response.meta.get('redirect_reasons'):
            redirected = (code for code in response.meta['redirect_reasons'] if code in [301, 302])
        if redirected:
            yield scrapy.Request(url=response.url,
                                 callback=self.parse_match,
                                 errback=self.errback_guessed_url,
                                 cb_kwargs=item)
        else:
            # Get the first match that has `title` attribute containing the SKU:
            sku_string = item['manufacturerSKU']
            a_tag_with_image_url_containing_sku = f'a[.//img[contains(@src,"{sku_string.lower()}.")]]//@href'
            match = response.css('.product-items .prolabels-wrapper').xpath(a_tag_with_image_url_containing_sku).get()
            if match:
                yield scrapy.Request(url=match,
                                    callback=self.parse_match,
                                    errback=self.errback_guessed_url,
                                    cb_kwargs=item)
            else:
                item['comment'] = "manufacturerSKU not found on Skytechfireplaceremotes.com"
                write_not_found_item_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE,
                                            line=item)

    def parse_match(self, response, **item):
        # This is the original size image url
        image_url = response.css('.fotorama__loaded--img img.fotorama__img::attr(src)').get()
        if not image_url:
            item['comment'] = "manufacturerSKU found on iBuyFireplaces.com but no image"
            write_not_found_item_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE,
                                        line=item)
            return None

        # Some sku contain forward slash, not good for filename, e.g 'VDY24/18NMP', 'RAK35/40'
        # or space, e.g. "BZLB-BLNI RAP54", "BZLB-BLNI RAP42", 'MHS HEAT-ZONE-TOP'
        desired_image_name = item['mainImageName(.png)'].replace('/', '_').replace(' ', '-')

        # Add url query as a way to distinguish URL,
        # therefore can trick FilesPipeline into disable duplicate filter
        desired_image_url = furl(image_url).add({'image_name': desired_image_name}).url

        brand = item['brand']
        item = {
            'image_name': desired_image_name,
            'image_brand': brand,
            'file_urls': [desired_image_url],
        }
        yield ImageItem(item)

    def errback_guessed_url(self, failure):
        # Ref: https://docs.scrapy.org/en/latest/topics/request-response.html#accessing-additional-data-in-errback-functions
        item = failure.request.cb_kwargs
        item['comment'] = "manufacturerSKU not found on Skytechfireplaceremotes.com"
        write_not_found_item_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE,
                                    line=item)


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
        'FILES_EXPIRES': 0,
        'ITEM_PIPELINES': {
            '__main__.MyFilesPipeline': 1,
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
        # 'AUTOTHROTTLE_ENABLED': True,    # enable this so iBuyFireplaces.com does not block IP
        # 'RETRY_HTTP_CODES': [
        #                     #  500,     # iBuyFireplaces.com gave 500 error code on some pages even if the result is found. Use this so scrapy does not retry. Ref: https://docs.scrapy.org/en/latest/topics/downloader-middleware.html?highlight=downloader%20middleware#retry-http-codes
        #                      502, 503, 504, 522, 524, 408, 429]
        # 'AUTOTHROTTLE_TARGET_CONCURRENCY': 0.5,
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(SkytechfireplaceremotesSpider)

    # process.start()

    # with console.status("[bold green]Scraping images from skytechFireplaceRemotes.com ...") as status:
    #     process.start()

    # # Use Python Rich Progress
    # Ref: https://github.com/willmcgugan/rich/issues/121
    progress = Progress(SpinnerColumn(),
                        "[bold green]{task.description}",
                        # BarColumn(),
                        # "[progress.percentage]{task.percentage:>3.1f}%",
                        # "({task.completed} of {task.total})"
                        "•",
                        TimeElapsedColumn(),
                        # transient=True,
                        # start=False,
                        console=console)

    with progress:
        progress.log(f'Scraping images from skytechFireplaceRemotes.com')
        task_description = f'Scraping images ...'
        task_id = progress.add_task(task_description, start=False)

        try:
            progress.start_task(task_id)
            process.start()
        except Exception as error:
            # if debug:
            # traceback_str = ''.join(traceback.format_exception(etype=type(error), value=error, tb=error.__traceback__))
            # log.error(traceback_str)
            # log.exception(error)
            console.log(error)
