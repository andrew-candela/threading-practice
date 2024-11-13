"""
Exposes a class that can be used to 
"""

import logging
import requests
from queue import Queue
from bs4 import BeautifulSoup, ResultSet
import threading
from typing import Any
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

from threading_practice.lib.processors import Processor
from threading_practice.types.threading import Tombstone


logger = logging.getLogger(__name__)
tombstone = Tombstone()


@dataclass
class ScrapedContent:
    url: str
    parsed_content: BeautifulSoup

    def get_link_count(self) -> tuple[str, int]:
        links = self.parsed_content.find_all("a", href=True)
        return (self.url, len(links))


@dataclass
class WebUrlData:
    url: str
    depth: int = 0


@dataclass
class StrippedUrl:
    scheme: str
    netloc: str
    path: str

    def __str__(self) -> str:
        return f"{self.scheme}://{self.netloc}{self.path}"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, StrippedUrl):
            return False
        return (
            self.scheme == other.scheme
            and self.netloc == other.netloc
            and self.path == other.netloc
        )


def strip_url(url: str) -> StrippedUrl:
    p = urlparse(url)
    return StrippedUrl(p.scheme, p.netloc, p.path)


class Crawler:

    def __init__(self, workers: int = 5, max_depth: int = 1):
        self.queue: Queue[WebUrlData | Tombstone] = Queue()
        self.worker_count = workers
        self.visited_lock = threading.Lock()
        self.visited_sites: set[str] = set()
        self.max_depth = max_depth

    def _spider(self, processor: Processor):
        logger.debug(f"starting thread: {threading.get_ident()}")
        while True:
            url = self.queue.get()
            try:
                if isinstance(url, Tombstone):
                    break
                links = self.scrape_page(url.url, processor)
                for link in links or []:
                    href = link["href"]
                    full_url = urljoin(url.url, href)
                    stripped_url = str(strip_url(full_url))
                    with self.visited_lock:
                        if stripped_url in self.visited_sites:
                            continue
                        self.visited_sites.add(stripped_url)
                    if url.depth < self.max_depth:
                        self.queue.put(WebUrlData(stripped_url, url.depth + 1))
            except Exception as e:
                logger.exception(f"Error while _spidering: {e}")
            finally:
                self.queue.task_done()
        logger.debug(f"Shutting down thread {threading.get_ident()}...")

    def crawl(self, url: str, processor: Processor | None = None) -> None:
        """
        Manages:

        - seeding the URL queue
        - setup of the threads
        - joining the threads and queues
        - teardown of the threads
        """
        tombstone = Tombstone()
        processor = processor or Processor()
        # Configure the worker threads
        threads = [
            threading.Thread(target=self._spider, args=[processor], daemon=True)
            for _ in range(self.worker_count)
        ]

        # Seed the URL queue
        self.queue.put(WebUrlData(url, 0))
        for thread in threads:
            thread.start()

        processor.process()
        # Wait till the queues are empty
        self.queue.join()

        # Kills the threads gracefully
        for thread in threads:
            self.queue.put(tombstone)
        for thread in threads:
            thread.join()
        # Kill the processor threads
        processor.join()

    def scrape_page(self, url: str, processor: Processor) -> ResultSet | None:
        try:
            # Fetch the page content
            response = requests.get(url)
            if response.status_code != 200:
                logger.error(f"Failed to retrieve {url}: {response.status_code}")
                return None

            # Parse the HTML content
            soup = BeautifulSoup(response.text, "lxml")
            processor.accept(ScrapedContent(url, soup))

            # Extract all links from the page
            links = soup.find_all("a", href=True)

            return links

        except Exception as e:
            logger.exception(f"Error while scraping {url}: {e}")
            return None
