from scrapy.item import Item, Field


class ImageItem(Item):
    image_name = Field()
    image_brand = Field()
    file_urls = Field()     # this variable name have to be this for scrapy Pipeline
    files = Field()         # this variable name have to be this for scrapy Pipeline
