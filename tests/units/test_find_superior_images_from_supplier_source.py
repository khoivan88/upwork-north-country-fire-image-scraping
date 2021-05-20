# __Author__: Khoi Van 2021

import os
import sys
import csv

sys.path.append(os.path.realpath('src'))

from pathlib import Path
from typing import Dict, Set, Tuple, List

import pytest
from src.find_superior_images_from_supplier_source import (find_match)


CURRENT_FILEPATH = Path(__file__).resolve().parent.parent
DATA_FOLDER = CURRENT_FILEPATH.parent / 'src' / 'data'
SUPERIOR_DIRECTORY_FILE = DATA_FOLDER / 'superior_image_directory.csv'
IMAGE_FOLDER = CURRENT_FILEPATH.parent / 'images'
SUPERIOR_SOURCE_FOLDER = IMAGE_FOLDER / 'superior_source'


@pytest.fixture
def directory() -> List[Dict[str, str]]:
    with open(SUPERIOR_DIRECTORY_FILE, 'r') as f:
        dict_reader = csv.DictReader(f)
        return [line for line in dict_reader]


@pytest.mark.parametrize(
    "item, expect", [
        ({'manufacturerSKU': 'ERT3027',
          'name__default': 'Superior 27 Inch Radiant Electric Fireplace | ERT3027 |'},
         'Hi-Res Images/Electric Fireplaces/superior-elfp-ert3027-hires-1.tif'),
        ({'manufacturerSKU': 'MPE-33-3',
          'name__default': 'Superior 33 Inch Radiant Electric Fireplace | ERT3033 |'},
         'Hi-Res Images/Electric Fireplaces/superior-elfp-ert3000-33-hires-1.tif'),
        ({'manufacturerSKU': 'MPE-36-3',
          'name__default': 'Superior 36 Inch Radiant Electric Fireplace | ERT3036 |'},
         'Hi-Res Images/Electric Fireplaces/superior-elfp-ert3000-36-hires-1.tif'),
        ({'manufacturerSKU': 'WRT3036WSI',
          'name__default': 'Superior 36 Wood Fireplace | WRT/WCT 3036'},
         'Hi-Res Images/Wood Fireplaces/superior-wbfp-wrt3000-42-hires-1.tif'),
        ({'manufacturerSKU': 'WRT2036WSI',
          'name__default': 'Superior 36 Wood Fireplace | WRT/WCT 2036'},
         'Hi-Res Images/Wood Fireplaces/superior-wbfp-wct2000-36-hires-1.tif'),
        ({'manufacturerSKU': 'VRE4536WS',
          'name__default': 'Superior 36 Inch Vent Free Outdoor Gas Firebox | VRE4536 |'},
         'Hi-Res Images/Outdoor Products/superior-ovffp-vre4500-42-hires-1.tif'),
        ({'manufacturerSKU': 'BRT40STTMN',
          'name__default': 'Superior 40 B-Vent See-Thru Radiant Gas Fireplace | BRT40ST'},
         'Hi-Res Images/Gas Fireplaces/superior-bvfp-brt40ST-hires-1.tif'),
        ({'manufacturerSKU': 'BRT4342TEN-B',
          'name__default': 'Superior 42 B-Vent Radiant Gas Fireplace | BRT4342'},
         'Hi-Res Images/Gas Fireplaces/superior-bvfp-brt4000-4336-hires-1.tif'),
        ({'manufacturerSKU': 'BRT4542TEN-B',
          'name__default': 'Superior 42 B-Vent Radiant Gas Fireplace | BRT4542'},
         'Hi-Res Images/Gas Fireplaces/superior-bvfp-brt4000-4536-hires-1.tif'),
        ({'manufacturerSKU': 'DRL4060TEN',
          'name__default': 'Superior 60 Direct-Vent Contemporary Linear Gas Fireplace | DRL4060'},
         'Hi-Res Images/Gas Fireplaces/superior-dvfp-drl4000-60-hires-1.tif'),
        ({'manufacturerSKU': 'DRT35PFDEN',
          'name__default': 'Superior 35 Direct-Vent Traditional Peninsula Gas Fireplace | DRT35PF'},
         'Hi-Res Images/Gas Fireplaces/superior-dvfp-drt3500-40-hires-1.tif'),
    ]
)
def test_find_match(item: Dict[str, str], directory: List[Dict[str, str]],
                    expect: str):
    answer = find_match(item, directory).relative_to(SUPERIOR_SOURCE_FOLDER)
    assert str(answer) == expect

