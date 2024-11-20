from dataclasses import dataclass
from urllib.parse import urlparse
from typing import Any, cast, Generator
from bs4 import BeautifulSoup


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

    @classmethod
    def from_url(cls, url: str) -> "StrippedUrl":
        p = urlparse(url)
        return cls(p.scheme, p.netloc, p.path)


@dataclass
class ScrapedContent:
    url: str
    parsed_content: BeautifulSoup

    def get_link_count(self) -> tuple[str, int]:
        links = self.parsed_content.find_all("a", href=True)
        return (self.url, len(links))

    def get_text(self) -> Generator[str, None, None]:
        allowlist = [
            "p",
        ]
        return (
            cast(str, t.get_text())
            for t in self.parsed_content.find_all(text=True)
            if t.parent.name in allowlist
        )
