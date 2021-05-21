import csv
import logging
import shutil
import sys
import re
from functools import partial
from multiprocessing import Pool
from pathlib import Path, PurePath
from typing import Dict, List, Optional, Union
from operator import itemgetter

from fuzzywuzzy import fuzz, process

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import BarColumn, Progress, SpinnerColumn, TimeElapsedColumn


console = Console()
sys.setrecursionlimit(20000)

# Set logger using Rich: https://rich.readthedocs.io/en/latest/logging.html
logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)
log = logging.getLogger("rich")


CURRENT_FILEPATH = Path(__file__).resolve().parent
DATA_FOLDER = CURRENT_FILEPATH / 'data'
DATA_FOLDER.mkdir(exist_ok=True)
INPUT_FILE = DATA_FOLDER / 'imageNames.csv'
IMAGE_NOT_FOUND_RESULT_FILE = DATA_FOLDER / 'images_not_found_superior_from_source.csv'
SUPERIOR_DIRECTORY_FILE = DATA_FOLDER / 'superior_image_directory.csv'

IMAGE_FOLDER = CURRENT_FILEPATH.parent / 'images'
SUPERIOR_SOURCE_FOLDER = IMAGE_FOLDER / 'superior_source'
SUPERIOR_IMAGES_FOR_NCF = IMAGE_FOLDER / 'superior_images_for_ncf'

LOG_FOLDER = CURRENT_FILEPATH / 'logs'
LOG_FOLDER.mkdir(exist_ok=True)
LOG_FILE = LOG_FOLDER / 'found_superior_images.csv'


def import_item_list(file):
    with open(file, 'r', newline='') as csv_file:
        dict_reader = csv.DictReader(csv_file)
        for line in dict_reader:
            if line['brand'] == 'Superior':
                yield line


def extract_superior_images(file):
    # Remove the not found result file and log file if exists
    IMAGE_NOT_FOUND_RESULT_FILE.unlink(missing_ok=True)
    LOG_FILE.unlink(missing_ok=True)

    superior_items = import_item_list(file)

    directory = load_directory_file()
    for item in superior_items:
        try:
            find_superior_image(item, directory=directory)
        except Exception as error:
            log.error(f'{item=}')
            log.exception(error)

    # # Ref: https://github.com/willmcgugan/rich/issues/121
    # progress = Progress(SpinnerColumn(),
    #                     "[bold green]{task.description}",
    #                     BarColumn(),
    #                     "[progress.percentage]{task.percentage:>3.1f}%",
    #                     "({task.completed} of {task.total})"
    #                     "•",
    #                     TimeElapsedColumn(),
    #                     # transient=True,
    #                     console=console)

    # # for item in superior_items:
    # #     find_superior_image(item, directory=directory)

    # with progress:
    #     task_description = f'Generating images ...'
    #     task_id = progress.add_task(task_description, total=len(superior_items), start=True)

    #     try:
    #         pool_size = 25
    #         with Pool(processes=pool_size) as p:
    #             results = p.imap(partial(find_superior_image,
    #                                      directory=directory
    #                                      ),
    #                              superior_items,
    #                              chunksize=8)
    #             for result in results:
    #                 progress.advance(task_id)

    #     except Exception as error:
    #         # if debug:
    #         # traceback_str = ''.join(traceback.format_exception(etype=type(error), value=error, tb=error.__traceback__))
    #         # log.error(traceback_str)
    #         # log.exception(error)
    #         log.error(error)


def find_superior_image(item: Dict[str, str], directory: List[Dict[str, str]]) -> None:
    img_path = ''
    categories_to_search = ['fireplace', 'firebox', 'stove', 'insert']
    # Special case to ignore (high chance of mistmatch or found not to be accurate)
    if item['manufacturerSKU'].lower() in ['wrt4038is', 'wrt4043is', 'drt4045ten']:
        # Write to log
        item.update({'comment': 'Not search Superior provided images due to not good images or no correct match.'})
        write_items_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE, lines=[item])
    elif any(term in item['c__productCategory'].lower() for term in categories_to_search):
        img_path = find_match(item, directory)
        if img_path:
            copy_image(item=item,
                       img_path=img_path,
                       out_dir=SUPERIOR_IMAGES_FOR_NCF)
            # Write the match image file to log
            item.update({'matched_image': Path(img_path).relative_to(SUPERIOR_SOURCE_FOLDER)})
            write_items_to_csv(file=LOG_FILE, lines=[item])
        else:
            # Write not found item to log
            item.update({'comment': 'Not found from Superior provided images'})
            write_items_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE, lines=[item])
    else:
        # Write not found item to log
        item.update({'comment': 'Not search Superior provided images due to high chance of false positives'})
        write_items_to_csv(file=IMAGE_NOT_FOUND_RESULT_FILE, lines=[item])



def find_match(item: Dict[str, str], directory: List[Dict[str, str]]
               ) -> Optional[Union[str, PurePath]]:
    if '|' not in item['name__default']:
        # breakpoint()
        return None
    product_line = item['name__default'].split('|')[1].strip()

    possible_images = []

    # Special cases such as: 'WRT/WCT 2036'
    if '/' in product_line and ' ' in product_line:
        console.log(f'{product_line=}')
        part1, part2 = product_line.split(' ')
        models = part1.split('/')
        for model in models:
            series = f'{model}{part2}'
            possible_images.append(find_possible_match(product_line=series,
                                                       directory=directory))
    else:
        possible_images.append(find_possible_match(product_line=product_line,
                                                    directory=directory))

    # breakpoint()
    if len(possible_images) > 1:
        possible_images = sorted(possible_images, key=itemgetter('image_type'))

    if possible_images:
        return IMAGE_FOLDER / possible_images[0]['filepath']


def find_possible_match(product_line, directory):
    # return product_line.lower() in line['series'].lower()
    # image_choices = {line['series']: index for index, line in enumerate(directory)}
    # text_choices = image_choices.keys()
    # options = process.extract(product_line, text_choices, limit=4, scorer=fuzz.token_set_ratio)

    results = []
    startswith_search_results = find_matches_startwith(product_line, directory)
    if startswith_search_results:
        results = find_fuzzy(product_line, startswith_search_results)
    else:
        results = find_fuzzy(product_line, directory)
    # breakpoint()

    # Sort by priority:
    results = sorted(results, key=itemgetter('priority'))
    return results[0]


def find_fuzzy(product_line, directory_choices):
    image_choices_hi_res = {index: line['series']
                            for index, line in enumerate(directory_choices)
                            if line['image_type'] == 'Hi-Res Images'}

    # Remove any '...ST' (see-through) product if it is not indicated in the 'product_line'
    # breakpoint()
    if not product_line.lower().endswith('st'):
        image_choices_hi_res = {key: value for key, value in image_choices_hi_res.items() if not value.lower().endswith('st')}

    # Return as a tuple of 3 (because the choices was added as dict):
    # (the match value of the dict (which was compared to the string), the score, and the key of the value)
    options = process.extract(product_line, image_choices_hi_res, limit=5, scorer=fuzz.token_sort_ratio)
    # breakpoint()
    top_score = options[0][1] if options else 0

    if top_score < 70:
        image_choices_web_images = {index: line['series']
                                for index, line in enumerate(directory_choices)
                                if line['image_type'] == 'Web Images'}
        options = process.extract(product_line, image_choices_web_images, limit=5, scorer=fuzz.token_sort_ratio)
        top_score = options[0][1]

    top_score_results = [option for option in options if option[1] >= top_score]
    # breakpoint()
    return [directory_choices[index] for _, _, index in top_score_results]


def find_matches_startwith(product_line, directory
                           ) -> Optional[List[Dict[str, str]]]:
    base_sku = re.search(r'^([a-z]{3}\d{2})', product_line, flags=re.IGNORECASE)
    if base_sku:
        # !Special cases
        if product_line.lower() == 'vre4536':
            return [line for line in directory
                    if line['series'].lower().startswith('vre4500')]

        return [line for line in directory
                if line['series'].lower().startswith(base_sku[1].lower())]


def load_directory_file() -> List[Dict[str, str]]:
    with open(SUPERIOR_DIRECTORY_FILE, 'r') as f:
        dict_reader = csv.DictReader(f)
        return [line for line in dict_reader]


def copy_image(item: Dict, img_path: Union[str, PurePath], out_dir: Union[str, PurePath]) -> None:
    # Some sku contain forward slash, not good for filename, e.g 'VDY24/18NMP', 'RAK35/40'
    # or space, e.g. "BZLB-BLNI RAP54", "BZLB-BLNI RAP42", 'MHS HEAT-ZONE-TOP'
    desired_image_name = item['mainImageName(.png)'].replace('/', '_').replace(' ', '-')
    img_file = Path(img_path)
    # destination = Path(out_dir) / item['brand'] / f'{desired_image_name}{img_file.suffix}'
    destination = Path(out_dir) / f'{desired_image_name}{img_file.suffix}'
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(Path(img_path), Path(destination))
    except Exception as error:
        print()
        log.error(error)


def write_items_to_csv(file, lines):
    '''Write item without found images into csv file'''
    file_exists = Path(file).exists()
    with open(Path(file), 'a') as csvfile:
        headers = list(lines[0].keys())
        writer = csv.DictWriter(csvfile, delimiter=',',
                                lineterminator='\n',
                                fieldnames=headers)

        if not file_exists:
            writer.writeheader()  # file doesn't exist yet, write a header

        for line in lines:
            writer.writerow(line)


if __name__ == '__main__':
    # Remove log files, not found result file,
    extract_superior_images(file=INPUT_FILE)
