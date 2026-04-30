#!/usr/bin/env python3
"""Scrape didactic content from IBKR Campus into JSON.

This script focuses on educational content only:
- hub intro text
- course summaries
- lesson metadata
- lesson body sections
- lesson images
- educational resource links

It intentionally skips:
- comments
- quizzes / login CTAs
- share widgets
- newsletters
- footer / disclosure text
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag


DEFAULT_HUB_URL = "https://www.interactivebrokers.com/campus/traders-academy/intro-to-ibkr-tools/"
DEFAULT_OUTPUT = Path("data/ibkr_campus/intro_to_ibkr_tools_didactic.json")
TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class FetchConfig:
    timeout: int = TIMEOUT_SECONDS


class IBKRCampusDidacticScraper:
    def __init__(self, fetch_config: FetchConfig | None = None) -> None:
        self.fetch_config = fetch_config or FetchConfig()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Upgrade-Insecure-Requests": "1",
            }
        )
        self.lesson_cache: dict[str, dict[str, Any]] = {}

    def run(self, hub_url: str) -> dict[str, Any]:
        hub_url = self._normalize_url(hub_url)
        hub_soup = self._fetch_soup(hub_url)
        hub_data = self._extract_hub_page(hub_soup, hub_url)

        courses_with_lessons: list[dict[str, Any]] = []
        unique_lessons: OrderedDict[str, dict[str, Any]] = OrderedDict()

        for course in hub_data["courses"]:
            course_payload = {
                key: value for key, value in course.items() if key != "lesson_links"
            }
            course_payload["lessons"] = []

            for lesson_link in course["lesson_links"]:
                lesson_url = self._normalize_url(lesson_link["url"])
                if lesson_url not in self.lesson_cache:
                    lesson_soup = self._fetch_soup(lesson_url)
                    self.lesson_cache[lesson_url] = self._extract_lesson_page(
                        lesson_soup,
                        lesson_url,
                    )

                lesson_data = copy.deepcopy(self.lesson_cache[lesson_url])
                course_payload["lessons"].append(lesson_data)
                unique_lessons.setdefault(lesson_url, lesson_data)

            courses_with_lessons.append(course_payload)

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": {
                "hub_url": hub_url,
                "scope": "didactic-content-only",
            },
            "hub": {
                "title": hub_data["title"],
                "intro": hub_data["intro"],
            },
            "courses": courses_with_lessons,
            "stats": {
                "course_count": len(courses_with_lessons),
                "lesson_count": len(unique_lessons),
            },
        }

    def _fetch_soup(self, url: str) -> BeautifulSoup:
        response = self.session.get(url, timeout=self.fetch_config.timeout)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _extract_hub_page(self, soup: BeautifulSoup, url: str) -> dict[str, Any]:
        title = self._clean_text(self._safe_text(soup.find("h1")) or self._safe_title(soup))

        intro = ""
        intro_section = soup.select_one("section.pb-0 .cat-description p")
        if intro_section:
            intro = self._clean_text(intro_section.get_text(" ", strip=True))

        course_articles = soup.select("article.group-item")
        courses: list[dict[str, Any]] = []

        for article in course_articles:
            content_col = article.select_one("div.col-12.col-md-9")
            if not content_col:
                continue

            title_anchor = content_col.find("a", href=True)
            title_heading = title_anchor.find("h2") if title_anchor else None
            course_title = self._clean_text(self._safe_text(title_heading))
            course_url = self._absolute_url(url, title_anchor.get("href", "")) if title_anchor else None

            level_node = article.select_one("h6")
            level_text = self._clean_text(self._safe_text(level_node))
            level = re.sub(r"^Level\s*", "", level_text).strip() if level_text else None

            description = None
            for child in content_col.find_all("p", recursive=False):
                text = self._clean_text(child.get_text(" ", strip=True))
                if text:
                    description = text
                    break

            thumbnail = article.select_one("a.thumb-link img")
            thumbnail_url = self._extract_image_url(thumbnail, url) if thumbnail else None

            lesson_links = []
            for lesson_anchor in content_col.select("a.btn-chevron[href]"):
                lesson_title = self._clean_text(lesson_anchor.get_text(" ", strip=True))
                lesson_url = self._absolute_url(url, lesson_anchor.get("href", ""))
                if lesson_title and lesson_url:
                    lesson_links.append({"title": lesson_title, "url": lesson_url})

            if course_title and course_url and lesson_links:
                courses.append(
                    {
                        "title": course_title,
                        "url": course_url,
                        "level": level,
                        "description": description,
                        "thumbnail_url": thumbnail_url,
                        "lesson_links": lesson_links,
                    }
                )

        return {
            "url": url,
            "title": title,
            "intro": intro,
            "courses": courses,
        }

    def _extract_lesson_page(self, soup: BeautifulSoup, url: str) -> dict[str, Any]:
        main = soup.select_one("main")
        if not main:
            raise ValueError(f"Could not locate lesson content area for {url}")

        title = self._clean_text(self._safe_text(soup.find("h1")) or self._safe_title(soup))

        duration = None
        level = None
        for meta in main.select(".meta-lesson h6"):
            text = self._clean_text(meta.get_text(" ", strip=True))
            if text.startswith("Duration"):
                duration = text.replace("Duration", "", 1).strip()
            elif text.startswith("Level"):
                level = text.replace("Level", "", 1).strip()

        video_block = main.select_one(".videoWrapper [data-youtube-id]")
        youtube_id = video_block.get("data-youtube-id") if video_block else None
        video = (
            {
                "youtube_id": youtube_id,
                "embed_url": f"https://www.youtube.com/embed/{youtube_id}",
                "watch_url": f"https://www.youtube.com/watch?v={youtube_id}",
                "thumbnail_url": f"https://img.youtube.com/vi/{youtube_id}/hqdefault.jpg",
            }
            if youtube_id
            else None
        )

        back_to_course_url = None
        back_anchor = soup.find("a", string=lambda s: s and "Back to Course" in s)
        if back_anchor and back_anchor.get("href"):
            back_to_course_url = self._absolute_url(url, back_anchor["href"])

        course_navigation = self._extract_course_navigation(main, url)
        lesson_position = self._extract_lesson_position(soup, course_navigation, title)

        content_container = main.select_one("div.row.page-content.page-content-margin > div.col-12")
        sections = self._extract_didactic_sections(content_container, url) if content_container else []

        return {
            "url": url,
            "title": title,
            "lesson_number": lesson_position["lesson_number"],
            "lesson_count": lesson_position["lesson_count"],
            "duration": duration,
            "level": level,
            "back_to_course_url": back_to_course_url,
            "course_navigation": course_navigation,
            "video": video,
            "sections": sections,
        }

    def _extract_course_navigation(self, main: Tag, page_url: str) -> dict[str, Any] | None:
        aside = main.find("aside")
        if not aside:
            return None

        course_title_node = aside.find("h5")
        course_title = self._clean_text(self._safe_text(course_title_node))

        lessons = []
        active_index = None
        for item in aside.select("ol li"):
            anchor = item.find("a", href=True)
            if not anchor:
                continue

            spans = anchor.find_all("span")
            ordinal = None
            lesson_title = self._clean_text(anchor.get_text(" ", strip=True))
            if len(spans) >= 2:
                ordinal = self._safe_int(self._clean_text(spans[0].get_text(" ", strip=True)))
                lesson_title = self._clean_text(spans[1].get_text(" ", strip=True))

            lesson = {
                "position": ordinal,
                "title": lesson_title,
                "url": self._absolute_url(page_url, anchor["href"]),
                "is_current": "active" in (item.get("class") or []),
            }
            if lesson["is_current"]:
                active_index = ordinal
            lessons.append(lesson)

        if not course_title and not lessons:
            return None

        return {
            "course_title": course_title,
            "lessons": lessons,
            "current_position": active_index,
        }

    def _extract_lesson_position(
        self,
        soup: BeautifulSoup,
        course_navigation: dict[str, Any] | None,
        title: str,
    ) -> dict[str, Any]:
        full_text = soup.get_text(" ", strip=True)
        match = re.search(r"LESSON\s+(\d+)\s+OF\s+(\d+)", full_text, re.IGNORECASE)
        if match:
            return {
                "lesson_number": int(match.group(1)),
                "lesson_count": int(match.group(2)),
            }

        if course_navigation:
            lesson_count = len(course_navigation["lessons"])
            lesson_number = course_navigation.get("current_position")

            if lesson_number is None:
                for lesson in course_navigation["lessons"]:
                    if self._clean_text(lesson["title"]) == self._clean_text(title):
                        lesson_number = lesson.get("position")
                        break

            return {
                "lesson_number": lesson_number,
                "lesson_count": lesson_count or None,
            }

        return {"lesson_number": None, "lesson_count": None}

    def _extract_didactic_sections(self, container: Tag, page_url: str) -> list[dict[str, Any]]:
        sections: list[dict[str, Any]] = []
        current = self._new_section("Overview")

        for child in container.find_all(recursive=False):
            if child.name == "h2":
                if self._section_has_content(current):
                    sections.append(current)
                current = self._new_section(self._clean_text(child.get_text(" ", strip=True)))
                continue

            if child.name == "p":
                self._add_paragraph_or_link(current, child, page_url)
                continue

            if child.name == "figure":
                image = self._extract_figure(child, page_url)
                if image:
                    current["images"].append(image)
                continue

            if child.name in {"ul", "ol"}:
                items = [self._clean_text(li.get_text(" ", strip=True)) for li in child.find_all("li")]
                items = [item for item in items if item]
                if items:
                    current["lists"].append(items)

        if self._section_has_content(current):
            sections.append(current)

        return sections

    def _new_section(self, title: str) -> dict[str, Any]:
        return {
            "title": title,
            "paragraphs": [],
            "images": [],
            "links": [],
            "lists": [],
        }

    def _section_has_content(self, section: dict[str, Any]) -> bool:
        return any(section[key] for key in ("paragraphs", "images", "links", "lists"))

    def _add_paragraph_or_link(self, section: dict[str, Any], node: Tag, page_url: str) -> None:
        text = self._clean_text(node.get_text(" ", strip=True))
        anchors = node.find_all("a", href=True)

        if text:
            section["paragraphs"].append(text)

        for anchor in anchors:
            link_title = self._clean_text(anchor.get_text(" ", strip=True))
            href = self._absolute_url(page_url, anchor["href"])
            if href:
                section["links"].append({"title": link_title or href, "url": href})

    def _extract_figure(self, figure: Tag, page_url: str) -> dict[str, Any] | None:
        image = figure.find("img")
        if not image:
            return None

        image_url = self._extract_image_url(image, page_url)
        if not image_url:
            return None

        alt_text = self._clean_text(image.get("alt", ""))
        return {
            "url": image_url,
            "alt": alt_text or None,
        }

    def _extract_image_url(self, image: Tag, page_url: str) -> str | None:
        for attr in ("src", "data-src"):
            value = image.get(attr)
            if value and not value.startswith("data:image"):
                return self._absolute_url(page_url, value)
        return None

    def _absolute_url(self, page_url: str, value: str) -> str:
        return self._normalize_url(urljoin(page_url, value))

    def _normalize_url(self, url: str) -> str:
        parsed = urlparse(url.strip())
        cleaned_path = parsed.path or "/"
        if cleaned_path != "/" and not cleaned_path.endswith("/"):
            cleaned_path += "/"
        return parsed._replace(path=cleaned_path, params="", query=parsed.query, fragment="").geturl()

    def _safe_title(self, soup: BeautifulSoup) -> str:
        return soup.title.get_text(" ", strip=True) if soup.title else ""

    def _safe_text(self, node: Tag | None) -> str:
        return node.get_text(" ", strip=True) if node else ""

    def _safe_int(self, value: str | None) -> int | None:
        if not value:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def _clean_text(self, value: str | None) -> str:
        return re.sub(r"\s+", " ", value or "").strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape didactic IBKR Campus content into JSON.")
    parser.add_argument(
        "--hub-url",
        default=DEFAULT_HUB_URL,
        help=f"Hub page to crawl. Defaults to {DEFAULT_HUB_URL}",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=f"Output JSON path. Defaults to {DEFAULT_OUTPUT}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    scraper = IBKRCampusDidacticScraper()
    data = scraper.run(args.hub_url)

    output_path.write_text(json.dumps(data, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    print(f"Wrote didactic IBKR Campus JSON to {output_path}")
    print(
        json.dumps(
            {
                "course_count": data["stats"]["course_count"],
                "lesson_count": data["stats"]["lesson_count"],
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
