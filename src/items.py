from scrapy.item import Item, Field


class ImageItem(Item):
    image_name = Field()
    image_brand = Field()
    image_body = Field()        # only needed for `ImageWriterPipeline`
    image_extension = Field()   # only needed for `ImageWriterPipeline`
    file_urls = Field()     # this variable name have to be this for scrapy Pipeline
    files = Field()         # this variable name have to be this for scrapy Pipeline
