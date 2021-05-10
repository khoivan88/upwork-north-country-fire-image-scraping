import re
from datetime import datetime, timedelta
from pathlib import Path, PurePath
from urllib.parse import urlparse, parse_qs
import logging

import scrapy
import csv
import json

from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import DropItem
from scrapy.exporters import CsvItemExporter

from items import ImageItem

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, BarColumn, SpinnerColumn, TimeElapsedColumn


console = Console()
# sys.setrecursionlimit(20000)

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
INPUT_FILE = DATA_FOLDER / 'imageNames-test.csv'
IMAGE_NOT_FOUND_RESULT_FILE = DATA_FOLDER / 'images_not_found.csv'
DOWNLOAD_FOLDER = CURRENT_FILEPATH.parent / 'downloads'
DOWNLOAD_FOLDER.mkdir(exist_ok=True)


from scrapy.pipelines.files import FilesPipeline

class MyFilesPipeline(FilesPipeline):

    def file_path(self, request, response=None, info=None, *, item=None):
        # Save to 'brand' folder with the sku (lower case) as filename
        return f"{item['image_brand']}/{item['image_name']}.png"


def import_item_list(file):
    with open(file, 'r', newline='') as fin:
        return list(csv.DictReader(fin))


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
    allowed_domains = ['ultimate-dot-acp-magento.appspot.com']
    # item_sku_list = (item["manufacturerSKU"] for item in item_list)
    # start_urls = [f'https://ultimate-dot-acp-magento.appspot.com/?q={sku}&store_id=14034773&UUID=34efc3a6-91d4-4403-99fa-5633d6e9a5bd'
    #               for sku in item_sku_list]
    # base_url = 'https://ultimate-dot-acp-magento.appspot.com/'
    # handle_httpstatus_list = [301, 302]

    # Use `start_requests()` to be able to pass the whole csv line as keyword arguments into `parse`
    # this way, csv line info for not found image can be written into result file
    def start_requests(self):
        for item in self.item_list:
            sku = item['manufacturerSKU']
            url = f'https://ultimate-dot-acp-magento.appspot.com/?q={sku}&store_id=14034773&UUID=34efc3a6-91d4-4403-99fa-5633d6e9a5bd'
            yield scrapy.Request(url, callback=self.parse, cb_kwargs=item)

    def parse(self, response, **item):
        json_res = json.loads(response.body)
        exact_match = next((item
                            for item in json_res['items']
                            if json_res['term'] == item['sku'].lower()),
                           None)
        # if json_res['total_results'] < 1 or not exact_match:
        if json_res['total_results'] != 1 or not exact_match:
            # self.logger.info(f'Item with manufacturerSKU {json_res["term"]} not found')
            console.log(f'Item with manufacturerSKU "{item["manufacturerSKU"]}" not found')
            write_not_found_item_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE,
                                        line=item)
            return None

        item_url = f'https://www.northcountryfire.com{exact_match["u"]}'

        cb_kwargs = { 'desired_image_name': json_res['term'] }

        desired_image_url = exact_match['t2'].replace('_small.png', '_1000x1000.png')

        item = {
            'image_name': json_res['term'],
            'image_brand': exact_match['v'],
            'file_urls': [desired_image_url],
            # 'files': [json_res["term"]],
        }

        yield ImageItem(item)

        # yield scrapy.Request(url=item_url, cb_kwargs=cb_kwargs, callback=self.parse_item)

    # def parse_item(self, response, **cb_kwargs):
    #     breakpoint()
    #     date = parse_qs(urlparse(response.url).query)['eventSearchDate'][0]
    #     date += 'T00:00:00-0500'
    #     # breakpoint()
    #     track = '[ORGN] Division of Organic Chemistry'
    #     # Get all the sessions listing
    #     # sessions = response.css('.panel.panel-default.panel-session')
    #     sessions = response.xpath('//div[@id="event-content"]/div[contains(@class, "panel") and contains(@class, "panel-default") and contains(@class, "panel-session")]')

    #     for session in sessions:
    #         session_id = session.css('.panel-heading').xpath('@id').get()
    #         id_num = re.search(r'\D*(\d+)', session_id)
    #         zoom_link = f'https://acs.digitellinc.com/acs/events/{id_num[1]}/attend'
    #         info = session.css('.panel-heading .panel-title .session-panel-title')
    #         title = info.css('a::text').get().strip()
    #         time = info.css('.session-panel-heading')[0].css('::text').get().strip()
    #         time = re.sub(r"\s{2,}", '', time)
    #         presiders_info = info.css('.session-panel-heading')[1].css('::text').getall()
    #         presiders = [t for t in (s.strip() for s in presiders_info) if t and t != '|']
    #         # print(f'{title=}')
    #         # breakpoint()

    #         presentations = []
    #         session_content = session.css('.panel-body .panel.panel-default.panel-session')
    #         for presentation in session_content:
    #             presentation_id = presentation.css('.panel-heading').xpath('@id').get()
    #             presentation_id_num = re.search(r'\D*(\d+)', presentation_id)
    #             presentation_zoom_link = f'https://acs.digitellinc.com/acs/events/{presentation_id_num[1]}/attend'

    #             presentation_info = presentation.css('.panel-heading .panel-title .session-panel-title')
    #             presentation_title = presentation_info.css('a::text').get().strip()
    #             presentation_time = presentation_info.css('.session-panel-heading')[0].css('::text').get().strip()
    #             presentation_time = re.sub(r"\s{2,}", '', presentation_time)
    #             presenters_info = presentation_info.css('.session-panel-heading')[1].css('::text').getall()
    #             presenters = [t for t in (s.strip() for s in presenters_info) if t and t != '|']
    #             presentation_kwargs = {
    #                 'title': presentation_title,
    #                 'time': presentation_time,
    #                 'presenters': presenters,
    #                 'zoom_link': presentation_zoom_link,
    #             }
    #             presentations.append(PresentationItem(presentation_kwargs))
    #             # breakpoint()

    #         cb_kwargs = {
    #             'date': date,
    #             'title': title,
    #             'time': time,
    #             'presiders': presiders,
    #             'presentations': presentations,
    #             'track': track,
    #             'zoom_link': zoom_link,
    #         }
    #         yield SessionItem(cb_kwargs)

    #     # Find next page url if exists:
    #     next_page_url = response.css('.pagination.pagination-sm.pull-right')[0].css('li:nth-last-of-type(2) a').xpath('@href').get()
    #     # # print(f'{next_page_partial_url=}')
    #     if next_page_url:
    #         # next_page_url = response.urljoin(next_page_partial_url)
    #         # print(f'{next_page_url=}')
    #         # breakpoint()
    #         yield scrapy.Request(url=next_page_url, callback=self.parse)


if __name__ == '__main__':
    # Remove the result file if exists
    IMAGE_NOT_FOUND_RESULT_FILE.unlink(missing_ok=True)

    settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
        # 'HTTPCACHE_ENABLED': True,
        # 'DEFAULT_REQUEST_HEADERS': {
        #   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        #   'Accept-Language': 'en'
        # },
        # 'CSV_EXPORT_FILE': THIS_SPIDER_RESULT_FILE,
        # 'ITEM_PIPELINES': {
            # '__main__.RemoveIgnoredKeywordsPipeline': 100,
            # },
        'FILES_STORE': str(DOWNLOAD_FOLDER),
        'ITEM_PIPELINES': {
            # 'scrapy.pipelines.images.FilesPipeline': 1,
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
        'LOG_LEVEL': 'WARNING',
        # 'ROBOTSTXT_OBEY': False,
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(NCFImageSpider)
    process.start()