"""
I'll write the same crawler as before but with asyncio
instead of the threading module.

The idea will be the same.
I can use asyncio.Queue()
and asyncio.create_task
"""

import asyncio
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from threading_practice.types.urls import StrippedUrl, ScrapedContent
from threading_practice.lib.processors import AIOProcessor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AIOCrawler:
    def __init__(self, workers: int = 5, max_depth: int = 2, max_pages: int = 100):
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.worker_count = workers
        self.visited_lock = asyncio.Lock()
        self.visited_sites: set[str] = set()
        self.max_depth = max_depth
        self.max_pages = max_pages
        self._page_limit_reached = False

    async def crawl(self, url: str, processor: AIOProcessor | None = None):
        """
        The main loop that sets up and tears down the workers.
        """
        processor = processor or AIOProcessor()
        session = ClientSession()
        await self.queue.put(url)
        threads = [
            asyncio.create_task(self._spider(processor, session))
            for _ in range(self.worker_count)
        ]
        await processor.process()
        await self.queue.join()
        for thread in threads:
            thread.cancel()
        await processor.join()
        await session.close()
        if self._page_limit_reached:
            logger.info(f"Reached page limit of {self.max_pages}")
        else:
            logger.info(
                f"Scraped all links in {url} with max depth of {self.max_depth}."
            )

    async def _spider(self, processor: AIOProcessor, session: ClientSession):
        """
        An instance of a worker.
        Poll the queue for new URLs,
        processing each and pushing new URLs back to the queue
        """
        while True:
            url = await self.queue.get()
            try:
                html = await self.fetch(url, session)
                soup, links = self.parse(url, html)
                await processor.accept(ScrapedContent(url, soup))
                async with self.visited_lock:
                    for link in links:
                        if len(self.visited_sites) >= self.max_pages:
                            self._page_limit_reached = True
                            break
                        if link not in self.visited_sites:
                            await self.queue.put(link)
                            self.visited_sites.add(link)
            except Exception:
                logger.exception(f"Exception occurred while crawling: {url}")
            finally:
                self.queue.task_done()

    async def fetch(self, url: str, session: ClientSession) -> str:
        """
        Fetches a URL and return the HTML Content
        """
        try:
            async with session.get(url) as response:
                html = await response.text()
                return html
        except Exception as e:
            logger.exception(f"Error retrieving URL: {url}\n{e}")
        return ""

    def parse(self, url: str, html: str) -> tuple[BeautifulSoup, set[str]]:
        soup = BeautifulSoup(html, "html.parser")
        links: set[str] = set()
        for link in soup.find_all("a", href=True):
            stripped_url = StrippedUrl.from_url(urljoin(url, link["href"]))
            links.add(str(stripped_url).lower())
        return soup, links
