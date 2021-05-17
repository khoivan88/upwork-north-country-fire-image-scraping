import csv
import logging
import shutil
import sys
from functools import partial
from multiprocessing import Pool
from pathlib import Path, PurePath
from typing import Dict

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
IMAGE_NOT_FOUND_RESULT_FILE = DATA_FOLDER / 'images_not_found.csv'
IMAGE_FOLDER = CURRENT_FILEPATH.parent / 'images'

LOGO_FOLDER = IMAGE_FOLDER / 'logos'
OUTPUT_FOLDER = IMAGE_FOLDER / 'manually_created'
OUTPUT_FOLDER.mkdir(exist_ok=True)


def generate_images(file):
    with open(file, 'r', newline='') as csv_file:
        dict_reader = csv.DictReader(csv_file)
        items = [line for line in dict_reader]

    # Ref: https://github.com/willmcgugan/rich/issues/121
    progress = Progress(SpinnerColumn(),
                        "[bold green]{task.description}",
                        BarColumn(),
                        "[progress.percentage]{task.percentage:>3.1f}%",
                        "({task.completed} of {task.total})"
                        "•",
                        TimeElapsedColumn(),
                        # transient=True,
                        console=console)

    # for image_file in image_files:
    #     convert_and_resize(in_file=image_file,
    #                        input_folder=input_folder,
    #                        output_folder=output_folder)

    with progress:
        task_description = f'Generating images ...'
        task_id = progress.add_task(task_description, total=len(items), start=True)

        # for image_file in items:
        #     convert_and_resize(image_file)
        #     progress.advance(task_id)

        try:
            pool_size = 25
            with Pool(processes=pool_size) as p:
                results = p.imap(partial(generate_image,
                                        #  input_folder=input_folder,
                                         ),
                                 items,
                                 chunksize=8)
                for result in results:
                    progress.advance(task_id)

        except Exception as error:
            # if debug:
            # traceback_str = ''.join(traceback.format_exception(etype=type(error), value=error, tb=error.__traceback__))
            # log.error(traceback_str)
            # log.exception(error)
            log.error(error)


def generate_image(item: Dict) -> None:
    # Some sku contain forward slash, not good for filename, e.g 'VDY24/18NMP', 'RAK35/40'
    # or space, e.g. "BZLB-BLNI RAP54", "BZLB-BLNI RAP42", 'MHS HEAT-ZONE-TOP'
    desired_image_name = item['mainImageName(.png)'].replace('/', '_').replace(' ', '-')
    logo_file = LOGO_FOLDER / f'{item["brand"].lower()}_logo.png'
    destination = OUTPUT_FOLDER / item['brand'] / f'{desired_image_name}{logo_file.suffix}'
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(logo_file, destination)
    except Exception as error:
        print()
        log.error(error)


if __name__ == '__main__':
    generate_images(IMAGE_NOT_FOUND_RESULT_FILE)
