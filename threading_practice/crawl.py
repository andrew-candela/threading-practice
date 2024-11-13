import logging

from threading_practice.lib.crawler import Crawler, ScrapedContent
from threading_practice.lib.processors import Processor

logging.basicConfig(level=logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)
logger = logging.getLogger(__name__)


def printer(content: ScrapedContent):
    if content is None:
        return
    url, link_count = content.get_link_count()
    logger.info(f"\n{url=}\n{link_count=}\n")


processor = Processor(printer, workers=3)


def main():
    url = "https://www.astronomer.io/docs/"
    c = Crawler(workers=4, max_depth=2)
    c.crawl(url=url, processor=processor)


if __name__ == "__main__":
    main()
