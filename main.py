from __future__ import annotations

import asyncio
import json
import logging
import random
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from collections.abc import AsyncIterator
from typing import Any

import httpx
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl
from redis.asyncio import Redis
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import Base, engine, get_db, async_session_maker
from models import JobStatus, ScrapingJob


REDIS_QUEUE_NAME: str = 'taskcrawler:scraping-jobs'
logger = logging.getLogger(__name__)


redis_client: Redis = Redis.from_url(
    str(settings.redis_url),
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=None,
)


class JobCreateRequest(BaseModel):
    url: HttpUrl


class JobResponse(BaseModel):
    id: uuid.UUID
    url: str
    status: JobStatus
    retry_count: int
    scraped_title: dict[str, Any] | None
    error_log: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {'from_attributes': True}


def _serialize_job(job: ScrapingJob) -> JobResponse:
    scraped_title_value: dict[str, Any] | None
    if job.scraped_title is None:
        scraped_title_value = None
    elif isinstance(job.scraped_title, str):
        try:
            parsed_scraped_title = json.loads(job.scraped_title)
        except json.JSONDecodeError:
            scraped_title_value = {'value': job.scraped_title}
        else:
            scraped_title_value = parsed_scraped_title if isinstance(parsed_scraped_title, dict) else {'value': parsed_scraped_title}
    else:
        scraped_title_value = job.scraped_title

    return JobResponse.model_validate(
        {
            'id': job.id,
            'url': job.url,
            'status': job.status,
            'retry_count': job.retry_count,
            'scraped_title': scraped_title_value,
            'error_log': job.error_log,
            'created_at': job.created_at,
            'updated_at': job.updated_at,
        }
    )


def _build_queue_message(job_id: uuid.UUID, url: str) -> str:
    return json.dumps({'job_id': str(job_id), 'url': url})


def _parse_queue_message(message: str) -> tuple[uuid.UUID, str]:
    payload = json.loads(message)
    return uuid.UUID(payload['job_id']), str(payload['url'])


def _extract_title(html: str) -> str | None:
    lower_html = html.lower()
    start_index = lower_html.find('<title>')
    end_index = lower_html.find('</title>')
    if start_index == -1 or end_index == -1 or end_index <= start_index + 7:
        return None
    return html[start_index + 7:end_index].strip() or None


async def _persist_job_update(
    job_id: uuid.UUID,
    *,
    status_value: JobStatus | None = None,
    scraped_title: str | None = None,
    error_log: str | None = None,
    retry_count: int | None = None,
) -> None:
    async with async_session_maker() as session:
        job = await session.get(ScrapingJob, job_id)
        if job is None:
            return
        if status_value is not None:
            job.status = status_value
        if scraped_title is not None:
            job.scraped_title = scraped_title
        if error_log is not None:
            job.error_log = error_log
        if retry_count is not None:
            job.retry_count = retry_count
        job.updated_at = datetime.now(timezone.utc)
        await session.commit()


async def _prepare_database() -> None:
    attempt_count = 0
    while True:
        try:
            async with engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
                await connection.execute(
                    text(
                        "ALTER TABLE IF EXISTS scraping_jobs "
                        "ALTER COLUMN scraped_title TYPE TEXT USING scraped_title::TEXT"
                    )
                )
            return
        except Exception as exc:
            attempt_count += 1
            if attempt_count >= 6:
                raise RuntimeError('database startup failed') from exc
            await asyncio.sleep((2 ** attempt_count) + random.uniform(0.0, 1.0))


async def process_scraping_job(job_id: uuid.UUID, url: str) -> None:
    import re
    from collections.abc import Iterable
    from html import unescape
    from html.parser import HTMLParser
    from urllib.parse import urljoin, urlparse, urlunparse

    class _DocumentParser(HTMLParser):
        def __init__(self) -> None:
            super().__init__(convert_charrefs=True)
            self.page_title: str | None = None
            self.meta: dict[str, str] = {}
            self.og_tags: dict[str, str] = {}
            self.images: list[dict[str, str]] = []
            self.links: list[dict[str, str]] = []
            self.text_blocks: list[str] = []
            self._text_buffer: list[str] = []
            self._capture_text: bool = False
            self._capture_tags: list[str] = []
            self._in_script: bool = False
            self._in_style: bool = False
            self._in_title: bool = False
            self._current_image_attrs: dict[str, str] | None = None

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            attr_map = {name.lower(): value or '' for name, value in attrs}
            if tag == 'title':
                self._in_title = True
            elif tag == 'meta':
                name = attr_map.get('name', '').strip().lower()
                property_name = attr_map.get('property', '').strip().lower()
                content = attr_map.get('content', '').strip()
                if name:
                    self.meta[name] = content
                if property_name.startswith('og:'):
                    self.og_tags[property_name] = content
            elif tag == 'img':
                src = attr_map.get('src', '').strip()
                alt = attr_map.get('alt', '').strip()
                width = attr_map.get('width', '').strip()
                height = attr_map.get('height', '').strip()
                if src:
                    self.images.append({
                        'src': src,
                        'alt': alt,
                        'width': width,
                        'height': height,
                    })
            elif tag == 'a':
                href = attr_map.get('href', '').strip()
                if href:
                    self.links.append({'href': href, 'text': ''})
            elif tag in {'main', 'article'}:
                self._capture_tags.append(tag)
                self._capture_text = True
            elif tag in {'p', 'section', 'div', 'li'} and self._capture_text:
                self._capture_tags.append(tag)
            elif tag in {'script', 'style', 'nav', 'header', 'footer'}:
                if tag == 'script':
                    self._in_script = True
                elif tag == 'style':
                    self._in_style = True

        def handle_endtag(self, tag: str) -> None:
            if tag == 'title':
                self._in_title = False
            elif tag == 'script':
                self._in_script = False
            elif tag == 'style':
                self._in_style = False
            elif tag in {'main', 'article', 'p', 'section', 'div', 'li'} and self._capture_text:
                if self._text_buffer:
                    block = ' '.join(piece.strip() for piece in self._text_buffer if piece.strip()).strip()
                    if block:
                        self.text_blocks.append(block)
                    self._text_buffer.clear()
                if tag in self._capture_tags:
                    while self._capture_tags:
                        current = self._capture_tags.pop()
                        if current == tag:
                            break
                if tag in {'main', 'article'} and not any(current in {'main', 'article'} for current in self._capture_tags):
                    self._capture_text = False

        def handle_data(self, data: str) -> None:
            if self._in_script or self._in_style:
                return
            text = unescape(data).strip()
            if not text:
                return
            if self._in_title:
                self.page_title = f'{self.page_title} {text}'.strip() if self.page_title else text
            if self._capture_text:
                self._text_buffer.append(text)
            for link in self.links:
                if link['text'] == '':
                    link['text'] = text
                    break

    def _normalize_url(value: str, base_url: str) -> str:
        resolved = urljoin(base_url, value.strip())
        parsed = urlparse(resolved)
        cleaned = parsed._replace(fragment='')
        return urlunparse(cleaned)

    def _is_tracker_image(image: dict[str, str]) -> bool:
        src = image.get('src', '').lower()
        alt = image.get('alt', '').lower()
        width_text = image.get('width', '').strip().lower()
        height_text = image.get('height', '').strip().lower()
        if any(token in src for token in ('icon', 'pixel', 'analytics')):
            return True
        if any(token in alt for token in ('icon', 'pixel', 'analytics')):
            return True
        try:
            width = int(re.sub(r'[^0-9]', '', width_text) or '0')
            height = int(re.sub(r'[^0-9]', '', height_text) or '0')
            if 0 < width <= 16 or 0 < height <= 16:
                return True
        except ValueError:
            pass
        return False

    def _collapse_whitespace(value: str) -> str:
        return re.sub(r'\s+', ' ', value).strip()

    def _extract_clean_content_sample(parser: _DocumentParser, html_text: str) -> str:
        lower_html = html_text.lower()
        for start_tag, end_tag in (('main', 'main'), ('article', 'article')):
            start_index = lower_html.find(f'<{start_tag}')
            if start_index == -1:
                continue
            start_close = lower_html.find('>', start_index)
            end_index = lower_html.find(f'</{end_tag}>', start_close)
            if start_close != -1 and end_index != -1 and end_index > start_close:
                fragment = html_text[start_close + 1:end_index]
                fragment = re.sub(r'(?is)<(script|style|nav|header|footer)[^>]*>.*?</\1>', ' ', fragment)
                fragment = re.sub(r'(?is)<[^>]+>', ' ', fragment)
                fragment = unescape(fragment)
                fragment = _collapse_whitespace(fragment)
                if fragment:
                    return fragment[:1200]
        if parser.text_blocks:
            return _collapse_whitespace(' '.join(parser.text_blocks))[:1200]
        paragraph_matches = re.findall(r'(?is)<p[^>]*>(.*?)</p>', html_text)
        if paragraph_matches:
            fallback_text = ' '.join(
                _collapse_whitespace(unescape(re.sub(r'(?is)<[^>]+>', ' ', block)))
                for block in paragraph_matches
            )
            fallback_text = _collapse_whitespace(fallback_text)
            if fallback_text:
                return fallback_text[:1200]
        body_match = re.search(r'(?is)<body[^>]*>(.*?)</body>', html_text)
        if body_match:
            body_fragment = re.sub(r'(?is)<(script|style|nav|header|footer)[^>]*>.*?</\1>', ' ', body_match.group(1))
            body_fragment = re.sub(r'(?is)<[^>]+>', ' ', body_fragment)
            body_fragment = _collapse_whitespace(unescape(body_fragment))
            if body_fragment:
                return body_fragment[:1200]
        return ''

    retry_count = 0
    await _persist_job_update(job_id, status_value=JobStatus.PROCESSING)

    while retry_count < 3:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True) as client:
                response = await client.get(url)
                response.raise_for_status()

            html_text = response.text
            parser = _DocumentParser()
            parser.feed(html_text)
            parser.close()

            parsed_url = urlparse(url)
            base_domain = parsed_url.netloc.lower()

            internal_links: list[str] = []
            external_links: list[str] = []
            for link in parser.links:
                href = link.get('href', '').strip()
                if not href:
                    continue
                normalized_href = _normalize_url(href, url)
                href_domain = urlparse(normalized_href).netloc.lower()
                if href_domain and href_domain == base_domain:
                    internal_links.append(urlparse(normalized_href).path or '/')
                elif href_domain:
                    external_links.append(normalized_href)

            media_items: list[dict[str, str]] = []
            for image in parser.images:
                if _is_tracker_image(image):
                    continue
                normalized_src = _normalize_url(image.get('src', ''), url)
                media_items.append({
                    'src': normalized_src,
                    'alt': image.get('alt', '').strip(),
                })

            metadata = {
                'description': parser.meta.get('description', ''),
                'keywords': parser.meta.get('keywords', ''),
                'og_title': parser.og_tags.get('og:title', ''),
                'og_description': parser.og_tags.get('og:description', ''),
                'og_image': parser.og_tags.get('og:image', ''),
                'og_url': parser.og_tags.get('og:url', ''),
                'og_type': parser.og_tags.get('og:type', ''),
                'title': parser.page_title or parser.meta.get('title') or parser.og_tags.get('og:title') or '',
            }
            structured_payload: dict[str, Any] = {
                'metadata': metadata,
                'media': media_items,
                'links': {
                    'internal': internal_links,
                    'external': external_links,
                },
                'clean_content_sample': _extract_clean_content_sample(parser, html_text),
                'source_url': url,
                'http_status': response.status_code,
            }

            await _persist_job_update(
                job_id,
                status_value=JobStatus.COMPLETED,
                scraped_title=json.dumps(structured_payload, ensure_ascii=False, separators=(',', ':')),
                error_log=None,
                retry_count=retry_count,
            )
            return
        except (httpx.HTTPError, httpx.TimeoutException, asyncio.TimeoutError, ValueError, json.JSONDecodeError) as exc:
            delay_seconds = (2 ** retry_count) + random.uniform(0.0, 1.0)
            retry_count += 1
            if retry_count >= 3:
                await _persist_job_update(
                    job_id,
                    status_value=JobStatus.FAILED,
                    error_log=str(exc),
                    retry_count=retry_count,
                )
                return
            await _persist_job_update(job_id, retry_count=retry_count, error_log=str(exc))
            await asyncio.sleep(delay_seconds)


async def enqueue_scraping_job(job_id: uuid.UUID, url: str) -> None:
    await redis_client.lpush(REDIS_QUEUE_NAME, _build_queue_message(job_id, url))


async def redis_worker() -> None:
    while True:
        try:
            queue_result = await redis_client.brpop(REDIS_QUEUE_NAME)
            if queue_result is None:
                await asyncio.sleep(0.1)
                continue
            _, raw_message = queue_result
            job_id, url = _parse_queue_message(str(raw_message))
            await process_scraping_job(job_id, url)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception('Redis worker failed while processing a job')
            await asyncio.sleep(1.0)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await _prepare_database()
    worker_task = asyncio.create_task(redis_worker())
    app.state.redis_worker_task = worker_task
    yield
    worker_task.cancel()
    with suppress(asyncio.CancelledError):
        await worker_task
    await redis_client.aclose()

    


app = FastAPI(title='TaskCrawler', version='1.0.0', lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=False,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.post('/api/v1/jobs', response_model=JobResponse)
async def create_job(
    payload: JobCreateRequest,
    background_tasks: BackgroundTasks,
    response: Response,
    db: AsyncSession = Depends(get_db),
) -> JobResponse:
    url_text = str(payload.url)
    result = await db.execute(select(ScrapingJob).where(ScrapingJob.url == url_text))
    existing_job = result.scalar_one_or_none()
    if existing_job is not None:
        response.status_code = status.HTTP_200_OK
        return _serialize_job(existing_job)

    job = ScrapingJob(url=url_text, status=JobStatus.PENDING)
    db.add(job)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        retry_result = await db.execute(select(ScrapingJob).where(ScrapingJob.url == url_text))
        existing_job = retry_result.scalar_one_or_none()
        if existing_job is None:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail='Unable to create job')
        response.status_code = status.HTTP_200_OK
        return _serialize_job(existing_job)

    await db.refresh(job)
    background_tasks.add_task(enqueue_scraping_job, job.id, job.url)
    response.status_code = status.HTTP_202_ACCEPTED
    return _serialize_job(job)


@app.get('/api/v1/jobs/{job_id}', response_model=JobResponse)
async def get_job(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)) -> JobResponse:
    job = await db.get(ScrapingJob, job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Job not found')
    return _serialize_job(job)