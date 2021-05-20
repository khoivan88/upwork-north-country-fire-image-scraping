import sys
import logging
import re
import csv
from pathlib import Path, PurePath
from typing import Tuple, List, Dict
from itertools import chain, combinations
from functools import partial
from multiprocessing import Pool

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import BarColumn, Progress, SpinnerColumn, TimeElapsedColumn, track

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
DATA_FOLDER = CURRENT_FILEPATH.parent / 'src' / 'data'
# INPUT_FOLDER = DATA_FOLDER / 'manuals_test'
INPUT_FOLDER = DATA_FOLDER / 'manuals'
RESULT_FILE = DATA_FOLDER / 'superior_image_directory.csv'

IMAGE_FOLDER = CURRENT_FILEPATH.parent / 'images'
SUPERIOR_SOURCE_FOLDER = IMAGE_FOLDER / 'superior_source'
DESIRED_IMAGE_SIZE = (1000, 1000)


def create_directory_for_superior_images(files, result_file):
    # Synchronouse fashion, easy for debug
    for file in files:
        extract_sku(file=file, result_file=result_file)
        # try:
        #     extract_sku(file=file, result_file=result_file)
        # except Exception as error:
        #     log.error(f'{file=}')
        #     log.exception(error)

    # # Asynchronous fashion, faster
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

    # with progress:
    #     progress.log(f'Extracting model numbers from Installation Manuals')
    #     task_description = f'Extracting ...'
    #     task_id = progress.add_task(task_description, total=len(files), start=True)

    #     # for image_file in files:
    #     #     extract_sku(image_file)
    #     #     progress.advance(task_id)

    #     try:
    #         pool_size = 25
    #         with Pool(processes=pool_size) as p:
    #             results = p.imap(partial(extract_sku,
    #                                      result_file=result_file,
    #                                     ),
    #                              files,
    #                              chunksize=8)
    #             for result in results:
    #                 progress.advance(task_id)

    #     except Exception as error:
    #         # if debug:
    #         # traceback_str = ''.join(traceback.format_exception(etype=type(error), value=error, tb=error.__traceback__))
    #         # log.error(traceback_str)
    #         # log.exception(error)
    #         # console.log(f'{file}')
    #         console.log(error)


def extract_sku(file, result_file):
    try:
        # Each type of folder ('Hi-Res Images', 'Web Images') has different names style
        relative_path = file.relative_to(SUPERIOR_SOURCE_FOLDER)
        number_of_parent_folders = len(relative_path.parents)
        type = str(relative_path.parents[number_of_parent_folders - 2])
        result = extract_sku_from_type(type=type, file=file)
        # console.log(f'{result=}')
        write_items_to_csv(file=result_file, lines=result)
    except Exception as error:
        print()
        log.error(f'{file}')
        log.exception(error)


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


def extract_sku_from_type(type: str, file: PurePath
                          ) -> Dict[str, str]:
    type_dict = {
        'Hi-Res Images': extract_sku_for_hi_res_images,
        'Web Images': extract_sku_for_web_images,
    }
    return type_dict[type](type=type, file=file)


def extract_sku_for_hi_res_images(type: str, file: PurePath
                                  ) -> List[Dict[str, str]]:
    series = [file.stem.split('-')[2]]
    variant = str(file.stem.split('-')[3])
    if variant.isdigit():
        re_replacement = re.compile(rf'(.*)\d{{{len(variant)}}}(.*)', flags=re.UNICODE | re.MULTILINE)
        new_series = re_replacement.sub(r'\1___\2', series[0]).replace('___', variant)
        # breakpoint()
        series.append(new_series)
    return [
        {
            'series': each_series,
            'product_type': str(
                file.relative_to(SUPERIOR_SOURCE_FOLDER).parent
            ).split('/')[-1],
            'image_type': type,
            'filename': file.name,
            'filepath': str(file.relative_to(IMAGE_FOLDER)),
            'priority': file.stem[-1],
        }
        for each_series in series
    ]


def extract_sku_for_web_images(type: str, file: PurePath
                               ) -> List[Dict[str, str]]:
    series = re.sub(r'superior', '', file.stem, flags=re.IGNORECASE)
    # If there are multiple image, set priority according to the last digit
    # breakpoint()
    priority = re.findall(r'\b(\d)$', file.stem)
    # 'product_type' is the concatenated names of the folders in the path
    product_type = str(file.relative_to(SUPERIOR_SOURCE_FOLDER).parent).lstrip(type)
    return [{
        'series': series,
        'product_type': product_type,
        'image_type': type,
        'filename': file.name,
        'filepath': str(file.relative_to(IMAGE_FOLDER)),
        'priority': priority[0] if priority else None,
    }]


if __name__ == '__main__':
    # Remove the result file if exists
    RESULT_FILE.unlink(missing_ok=True)

    # files = {f.resolve() for f in Path(INPUT_FOLDER).glob('**/*.pdf')}
    # files = {f.resolve() for f in Path(SUPERIOR_SOURCE_FOLDER).glob('**/*.*')}
    files = {f.resolve()
             for f in Path(SUPERIOR_SOURCE_FOLDER).glob('**/*.*')
            #  for f in Path(SUPERIOR_SOURCE_FOLDER).glob('Web Images/**/Superior Stoves/**/*.*')
            #  for f in Path(SUPERIOR_SOURCE_FOLDER).glob('Hi-Res Images/Electric Fireplaces/*.*')
             if f.name != '.DS_Store'
             }
    # breakpoint()
    create_directory_for_superior_images(files=files, result_file=RESULT_FILE)
