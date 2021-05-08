from scrapy.item import Item, Field


class SessionItem(Item):
    date = Field()
    track = Field()
    title = Field()
    time = Field()
    presiders = Field()
    presentations = Field()
    zoom_link = Field()


class PresentationItem(SessionItem):
    presenters = Field()