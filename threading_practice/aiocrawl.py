import asyncio
import logging

from threading_practice.lib.aiocrawler import AIOCrawler, ScrapedContent
from threading_practice.lib.processors import AIOProcessor

logging.basicConfig(level=logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)
logger = logging.getLogger(__name__)


async def printer(content: ScrapedContent):
    if content is None:
        return
    url, link_count = content.get_link_count()
    logger.info(f"\n{url=}\n{link_count=}\n")


async def content_previewer(content: ScrapedContent):
    if content is None:
        return
    print(f"-------\n\n{content.url}")
    for text in content.get_text():
        if text:
            print(f"{text}")


processor = AIOProcessor(content_previewer, workers=1)


async def main():
    url = "https://www.astronomer.io/docs/"
    c = AIOCrawler(workers=4, max_depth=1, max_pages=10)
    await c.crawl(url=url, processor=processor)


if __name__ == "__main__":
    asyncio.run(main())
