import pandas as pd
from pathlib import Path
from functools import reduce


CURRENT_FILEPATH = Path(__file__).resolve().parent
DATA_FOLDER = CURRENT_FILEPATH / 'data'
DATA_FOLDER.mkdir(exist_ok=True)

IMAGE_NOT_FOUND_NCF = DATA_FOLDER / 'images_not_found_ncf.csv'
IMAGE_NOT_FOUND_NAPOLEON = DATA_FOLDER / 'images_not_found_napoleon_catalog.csv'
IMAGE_NOT_FOUND_SKYTECHREMOTE = DATA_FOLDER / 'images_not_found_skytechfireplaceremotes.csv'
IMAGE_NOT_FOUND_IBUYFIREPLACES = DATA_FOLDER / 'images_not_found_ibuyfireplaces.csv'
IMAGE_NOT_FOUND_SUPERIOR_ORIGINAL_IMAGES = DATA_FOLDER / 'images_not_found_superior_from_source.csv'
IMAGE_FOUND_MANUALLY = DATA_FOLDER / 'images_found_manually.csv'

IMAGE_NOT_FOUND_RESULT_FILE = DATA_FOLDER / 'images_not_found.csv'


COLUMNS_TO_MERGE = ['ID', 'brand', 'c__productCategory', 'manufacturerSKU',
                    'name__default', 'mainImageName(.png)',
                    'imageSize(pixels)', 'duplicatedManufacturerSku']


def tally_images_not_found():
    not_found_ncf = pd.read_csv(IMAGE_NOT_FOUND_NCF)
    not_found_napoleon = pd.read_csv(IMAGE_NOT_FOUND_NAPOLEON)
    not_found_skytechremote = pd.read_csv(IMAGE_NOT_FOUND_SKYTECHREMOTE)
    not_found_ibuyfireplaces = pd.read_csv(IMAGE_NOT_FOUND_IBUYFIREPLACES)
    not_found_superior_from_source = pd.read_csv(IMAGE_NOT_FOUND_SUPERIOR_ORIGINAL_IMAGES)
    found_manually = pd.read_csv(IMAGE_FOUND_MANUALLY)

    dfs = [not_found_ncf,
           not_found_napoleon,
           not_found_skytechremote,
           not_found_ibuyfireplaces,
           not_found_superior_from_source,
           ]

    # Combine all items that show up on all of the NOT FOUND lists
    inner_merged = reduce(
        lambda left, right: pd.merge(left, right, on=COLUMNS_TO_MERGE),
        dfs
        )


    # Remove those items in the list of images found manually
    # Ref: https://stackoverflow.com/a/63477977/6596203
    inner_merged = pd.merge(inner_merged, found_manually, how='left',
                            on=COLUMNS_TO_MERGE, indicator=True)
    inner_merged = inner_merged.loc[inner_merged._merge=='left_only', inner_merged.columns != '_merge']
    # Remove the last column since it is the 'comment column' from the manually found item list
    inner_merged = inner_merged.iloc[:,:-1]

    amount_of_kept_columns = len(COLUMNS_TO_MERGE)
    # inner_merged['comments'] = inner_merged.iloc[:, amount_of_kept_columns].str.cat((inner_merged.iloc[:, (amount_of_kept_columns + 1):]),
    #                                                                                 sep='; ')

    # inner_merged['comments'] = inner_merged.iloc[:, amount_of_kept_columns:].astype(str).sum(axis=1)
    inner_merged['comments'] = inner_merged.iloc[:, amount_of_kept_columns:].astype(str).agg('; '.join, axis=1)

    # Remove those new columns from inner join operation (i.e. comment from each list)
    inner_merged = inner_merged.drop(inner_merged.iloc[:, amount_of_kept_columns:-1], axis=1)

    # breakpoint()
    inner_merged.to_csv(IMAGE_NOT_FOUND_RESULT_FILE, index=False)


if __name__ == '__main__':
    tally_images_not_found()