import subprocess
import sys
from shutil import rmtree
from pathlib import Path

CURRENT_FILEPATH = Path(__file__).resolve().parent

DOWNLOAD_FOLDER = CURRENT_FILEPATH.parent / 'images' / 'downloads'
MANUALLY_CREATED_FOLDER = CURRENT_FILEPATH.parent / 'images' / 'manually_created'
FINAL_IMAGES_FOLDER = CURRENT_FILEPATH.parent / 'images' / 'final'


def execute(command):
    subprocess.check_call(command, stdout=sys.stdout, stderr=subprocess.STDOUT)


def main():
    # breakpoint()
    # Delete download folder, where most of the images are downloaded:
    rmtree(DOWNLOAD_FOLDER)

    # Scrape from multiple places
    execute(['python', 'src/scrape_ncf_images.py'])
    execute(['python', 'src/scrape_napoleon_catalog_images.py'])
    execute(['python', 'src/scrape_skytechfireplaceremotes_images.py'])
    execute(['python', 'src/scrape_ibuyfireplaces_images.py'])

    # # Get Superior images from the original images folder, not necessary to run at all time
    # execute(['python', 'src/find_superior_images_from_supplier_source.py'])

    # Tally all of the scraping results to find any missing images
    execute(['python', 'src/tally_images_not_found.py'])

    # Delete 'manually_created' image folder, where all of the images using brand logos live:
    rmtree(MANUALLY_CREATED_FOLDER)
    # Using the tally result, generate images for those items using brand logos
    execute(['python', 'src/generate_images_for_not_found_items.py'])

    # Delete 'final' image folder, containing all of final deliverable images:
    rmtree(FINAL_IMAGES_FOLDER)

    # Transform all of the downloaded, created images into the final form
    execute(['python', 'src/transform_images.py'])


if __name__ == '__main__':
    main()
