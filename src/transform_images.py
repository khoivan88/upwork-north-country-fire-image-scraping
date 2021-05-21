import csv
import json
import logging
import re
import sys
from functools import partial
from multiprocessing import Pool
from pathlib import Path, PurePath
from urllib.parse import urlparse
from typing import Tuple

from PIL import Image
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

IMAGE_FOLDER = CURRENT_FILEPATH.parent / 'images'

INPUT_AUTO_DOWNLOAD_FOLDER = IMAGE_FOLDER / 'downloads'
INPUT_MANUAL_DOWNLOAD_FOLDER = IMAGE_FOLDER / 'manual_downloads'
INPUT_MANUALLY_CREATED_FOLDER = IMAGE_FOLDER / 'manually_created'
INPUT_SUPERIOR_FOLDER = IMAGE_FOLDER / 'superior_images_for_ncf'

OUTPUT_FOLDER = IMAGE_FOLDER / 'final'
OUTPUT_FOLDER.mkdir(exist_ok=True)

DESIRED_IMAGE_SIZE = (1000, 1000)


def transform_images(input_folder,
                     output_folder,
                     use_cropped_image=False,
                     crop_to_square=False):
    # breakpoint()
    image_files = {f.resolve() for f in Path(input_folder).glob('**/*.*')
                   if f.name != '.DS_Store'    # to ignore hidden file in MacOS
                   }

    # # Manually cropped images are saved in the same name but in .png
    # if use_cropped_image:
    #     image_files = {f.resolve() for f in Path(input_folder).glob('**/manually_cropped_fireplace_focus/*.png')
    #                    if f.name != '.DS_Store'    # to ignore hidden file in MacOS
    #                    }

    # image_files = {f.resolve() for f in Path(INPUT_FOLDER).glob('**/62140.jpg')}
    # image_files = {f.resolve()
    #             #    for f in Path(INPUT_FOLDER).glob('**/cdfi500-pro.png')    # !Only for testing purpose
    #                for f in Path(INPUT_FOLDER).glob('**/*')
    #                if (f.suffix in ['.png', '.jpg', '.jpeg'])}
    # log.info(f'{image_files=}')

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
        progress.log(f'Input folder: {str(input_folder)}')
        task_description = f'Transforming images ...'
        task_id = progress.add_task(task_description, total=len(image_files), start=True)

        # for image_file in image_files:
        #     convert_and_resize(image_file)
        #     progress.advance(task_id)

        try:
            pool_size = 25
            with Pool(processes=pool_size) as p:
                results = p.imap(partial(convert_and_resize,
                                         input_folder=input_folder,
                                         output_folder=output_folder,
                                         use_cropped_image=use_cropped_image,
                                         crop_to_square=crop_to_square
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


def convert_and_resize(in_file: PurePath,
                       input_folder: PurePath,
                       output_folder: PurePath,
                       use_cropped_image: bool = False,
                       crop_to_square: bool = False) -> None:
    in_file = Path(in_file)
    outfile = output_folder / in_file.relative_to(input_folder)

    # breakpoint()
    try:
        with Image.open(in_file) as im:
            # new_image = enlarge_to_square(im,
            #                               desired_size=DESIRED_IMAGE_SIZE,
            #                               background_color=(255, 255, 255)  # Choose white as background colo
            #                               )

            new_image = im
            if crop_to_square and (min(im.size) > max(DESIRED_IMAGE_SIZE)):
                # desired_one_dimension = -0.3 * min(im.size) + 2100
                desired_cropping_size = (min(im.size), min(im.size))
                new_image = crop_center(im=im, desired_size=desired_cropping_size)

            new_image = enlarge_to_square(new_image,
                                        desired_size=DESIRED_IMAGE_SIZE,
                                        background_color=(255, 255, 255)  # Choose white as background colo
                                        )


            if not outfile.parent.exists():
                outfile.parent.mkdir(parents=True, exist_ok=True)

            # Some images are in 'CMYK' mode and have to be converted to RGB first
            new_image.convert('RGB').save(outfile.with_suffix('.png'),
                                          optimize=True    # To give the smallest size possible
                                          )
        # logging.info(f'{outfile.suffix=}')

    except OSError:
        logging.exception("cannot convert", in_file)
    except Exception as error:
        console.log(in_file)
        logging.exception(error)


def crop_center(im: Image, desired_size: Tuple[int]) -> Image:
    """Crop a Pillow Image with desired size in the center

    Ref: https://stackoverflow.com/a/61948697/6596203
    Also checkout: https://note.nkmk.me/en/python-pillow-image-crop-trimming/

    Parameters
    ----------
    im : Image
        The Pillow Image that needs cropping
    desired_size : Tuple[int]
        desired crop size tuple of (width, height)

    Returns
    -------
    Image
        the cropped Pillow image
    """
    width, height = im.size   # Get dimensions
    new_width, new_height = desired_size
    left = round((width - new_width)/2)
    top = round((height - new_height)/2)
    x_right = round(width - new_width) - left
    x_bottom = round(height - new_height) - top
    right = width - x_right
    bottom = height - x_bottom
    # Crop the center of the image
    return im.crop((left, top, right, bottom))


def enlarge_to_square(im: Image,
                      desired_size: Tuple[int],
                      background_color: Tuple[int]) -> Image:
    old_size = im.size  # old_size[0] is in (width, height) format
    ratio = float(desired_size[0]) / max(old_size)

    # If the old size is almost square, increase the smallest dimention instead.
    if max(old_size)/min(old_size) <= 1.1:
        ratio = float(desired_size[0]) / min(old_size)

    # # Restrict expansion to not more than 2 times
    # ratio = min(ratio, 2)

    new_size = tuple(int(x*ratio) for x in old_size)
    new_im = im
    if im.size != desired_size:
        # scale_ratio = min(desired_size[0] / im.size[0],
        #                   desired_size[1] / im.size[1])
        im = im.resize(new_size,
                       resample=Image.LANCZOS)
        # create a new image and paste the resized on it
        new_im = Image.new("RGB", desired_size, background_color)
        new_im.paste(im, ((desired_size[0] - new_size[0]) // 2,
                          (desired_size[1] - new_size[1]) // 2))
    return new_im


if __name__ == '__main__':
    input = [
        # (INPUT_AUTO_DOWNLOAD_FOLDER, {}),
        # (INPUT_SUPERIOR_FOLDER, {'crop_to_square': True}),
        # (INPUT_MANUALLY_CREATED_FOLDER, {}),    # ! Run next to last
        # (INPUT_MANUAL_DOWNLOAD_FOLDER, {}),     # ! Run last
    ]
    output_folder = OUTPUT_FOLDER
    for folder, kwargs in input:
        transform_images(input_folder=folder, output_folder=output_folder, **kwargs)

    image_files = {f.resolve() for f in Path(OUTPUT_FOLDER).glob('**/*.*')
                   if f.name != '.DS_Store'}
    logging.info(f'{len(image_files)=}')
