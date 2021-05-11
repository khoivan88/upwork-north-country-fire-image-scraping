import sys
import re
from pathlib import Path, PurePath
from urllib.parse import urlparse, parse_qs
import logging

import scrapy
import csv
import json
from functools import partial
from multiprocessing import Pool

from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import DropItem
from scrapy.exporters import CsvItemExporter
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from items import ImageItem

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, BarColumn, SpinnerColumn, TimeElapsedColumn

from PIL import Image

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
# INPUT_FILE = DATA_FOLDER / 'imageNames-test.csv'
INPUT_FILE = DATA_FOLDER / 'imageNames.csv'
IMAGE_NOT_FOUND_RESULT_FILE = DATA_FOLDER / 'images_not_found.csv'
DOWNLOAD_FOLDER = CURRENT_FILEPATH.parent / 'downloads'
DOWNLOAD_FOLDER.mkdir(exist_ok=True)


from scrapy.pipelines.files import FilesPipeline


# TODO: fix for item with SKU is '???'

# TODO: TEST: find correct url for 'GL10B', 'GL10FR'
# TODO: TEST: find correct url for 'vdy24/18nmp', 'RAK35/40', 'TM/R2-A', 'CEG-SMOKES/5', 'GC-40/15', 'IFV2-100/15', TH-WTC/LP'
# TODO: TEST: for those found match but without any images, such as '10K81+Thurmalox'
# TODO: TEST: for those with input in different cases (lower vs upper), such as '58dva-wtec', rf571', 'w175-0726'
# TODO: TEST: for those "HDLOGS-ODCOUG, GR-ODCOUG", "SDLOGS-ODCOUG, GR-ODCOUG"
# TODO: TEST: for those "BZLB-BLNI RAP54", "BZLB-BLNI RAP42", 'MHS HEAT-ZONE-TOP'

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
            sku_string = item['manufacturerSKU']
            brand = item['brand']

            # Sometimes, the 'manufacturerSKU' contains items with commas:
            # e.g.  "HDLOGS-ODCOUG, GR-ODCOUG", "SDLOGS-ODCOUG, GR-ODCOUG", "DLE, RAK35/40"
            # In these cases, split the on commas and search for each SKU
            skus = [sku.strip() for sku in sku_string.split(',')]
            if len(skus) > 1:
                console.log(f'This line contains 2 manufacturerSKU "{sku_string}".\nSearching for each now...')

                urls = [f'https://ultimate-dot-acp-magento.appspot.com/?q={sku}+{brand}&store_id=14034773&UUID=34efc3a6-91d4-4403-99fa-5633d6e9a5bd'
                        for sku in skus]
                for url, sku in zip(urls, skus):
                    item.update({'manufacturerSKU': sku, 'mainImageName(.png)': sku.lower()})
                    yield scrapy.Request(url, callback=self.parse, cb_kwargs=item)

            url = f'https://ultimate-dot-acp-magento.appspot.com/?q={sku_string}+{brand}&store_id=14034773&UUID=34efc3a6-91d4-4403-99fa-5633d6e9a5bd'
            yield scrapy.Request(url, callback=self.parse, cb_kwargs=item)

    def parse(self, response, **item):
        # breakpoint()
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
            # console.log(f'Item with manufacturerSKU "{item["manufacturerSKU"]}" not found')
            item['comment'] = 'manufacturerSKU not found'
            write_not_found_item_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE,
                                        line=item)
            return None

        desired_image_url = exact_match['t2'].replace('_small.', '_1000x1000.')

        # Some sku contain forward slash, not good for filename, e.g 'VDY24/18NMP', 'RAK35/40'
        # or space, e.g. "BZLB-BLNI RAP54", "BZLB-BLNI RAP42", 'MHS HEAT-ZONE-TOP'
        desired_image_name = item['mainImageName(.png)'].replace('/', '_').replace(' ', '-')

        item = {
            # 'image_name': json_res['term'],
            # 'image_name': exact_match['sku'].lower(),
            'image_name': desired_image_name,
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


def transform_images():
    image_files = {f.resolve() for f in Path(DOWNLOAD_FOLDER).glob('**/*.*')}
    # image_files = {f.resolve()
    #             #    for f in Path(DOWNLOAD_FOLDER).glob('**/cdfi500-pro.png')    # !Only for testing purpose
    #                for f in Path(DOWNLOAD_FOLDER).glob('**/*')
    #                if (f.suffix in ['.png', '.jpg', '.jpeg'])}
    # log.info(f'{image_files=}')
    # Ref: https://github.com/willmcgugan/rich/issues/121
    progress = Progress(SpinnerColumn(),
                        "[bold green]{task.description}",
                        BarColumn(),
                        "[progress.percentage]{task.percentage:>3.1f}%",
                        "•",
                        TimeElapsedColumn(),
                        # transient=True,
                        console=console)
    with progress:
        task_id = progress.add_task("Transforming images...", total=len(image_files), start=True)

        try:
            pool_size = 25
            with Pool(processes=pool_size) as p:
                results = p.imap(partial(convert_and_resize,
                                        #  indir=Path(indir).resolve(),
                                         ),
                                 image_files,
                                 chunksize=8)
                for result in results:
                    progress.advance(task_id)

        except Exception as error:
            # if debug:
            # traceback_str = ''.join(traceback.format_exception(etype=type(error), value=error, tb=error.__traceback__))
            # log.error(traceback_str)
            # log.exception(error)
            console.log(error)


def convert_and_resize(file: PurePath) -> None:
    file = Path(file)
    try:
        save_new_image = False
        with Image.open(file) as im:
            if im.size != (1000, 1000):
                im = im.resize((1000, 1000))
                # im = im.thumbnail((1000, 1000))     # use `Image.thumbnail` instead of `resize` to keep the same aspect ration
                save_new_image = True
            if save_new_image or im.format != 'png':
                im.save(file.with_suffix('.png'),
                        optimize=True    # To give the smallest size possible
                        )
        # logging.info(f'{file.suffix=}')

        # Remove non-png files
        if save_new_image and file.suffix != '.png':
            file.unlink(missing_ok=True)
    except OSError:
        logging.exception("cannot convert", file)
    except Exception as error:
        print(file)
        logging.exception(error)


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
        'MEDIA_ALLOW_REDIRECTS': True,
        'FILES_STORE': str(DOWNLOAD_FOLDER),
        # 'MYPIPELINE_FILES_EXPIRES': 0,
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
        'LOG_LEVEL': 'INFO',
        # 'ROBOTSTXT_OBEY': False,
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(NCFImageSpider)
    with console.status("[bold green]Scraping images...") as status:
        process.start()

    # Disable for now!!
    # transform_images()
