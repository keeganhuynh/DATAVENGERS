import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import pandas as pd
import hashlib
from typing import List, Dict, Union, Set
from common.logger import Logger
import aiofiles
import chardet

class DataCrawler(Logger):
    def __init__(self, base_url: str = None, max_depth: int = None, download_folder: str = None):
        """
        Initialize the DataCrawler with optional parameters.

        Args:
        - base_url (str): The starting URL for the crawl.
        - max_depth (int): The maximum depth for recursive crawling.
        - download_folder (str): The folder where PDF files will be saved.
        """
        super().__init__()
        self.base_url = base_url if base_url else "https://tuyensinh.uel.edu.vn"
        self.page_urls: List[str] = []
        self.page_contents: List[Dict[str, Union[str, List[str]]]] = []
        self.pdf_urls: Set[str] = set()
        self.visited_urls: Set[str] = set()
        self.visited_hashes: Set[str] = set()  # For content deduplication
        self.max_depth = max_depth if max_depth else 0
        self.download_folder = download_folder if download_folder else "src/database/extracted_files/pdf_files"

        # Exclude social media prefixes and video file extensions
        self.excluded_prefixes = ['facebook', 'linkedin', 'zalo', 'tiktok', 'google']
        self.excluded_extensions = ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv']

        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)

        self.info(f"Initialized DataCrawler with base_url: {self.base_url}")

    async def fetch_text_with_retries(self, session, url, retries=3, delay=1, timeout=10):
        """
        Fetch the text content of a URL with retries in case of transient failures.

        Args:
        - session (aiohttp.ClientSession): The current session for HTTP requests.
        - url (str): The URL to fetch content from.
        - retries (int): The number of retry attempts.
        - delay (int): The delay between retries in seconds.
        - timeout (int): The timeout for the request in seconds.

        Returns:
        - str: The decoded HTML content, or None if all retries fail.
        """
        for attempt in range(retries):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    raw_data = await response.read()
                    encoding = chardet.detect(raw_data)['encoding']

                    if encoding is None:
                        self.warning(f"Encoding detection failed for URL: {url}. Falling back to 'ISO-8859-1'.")
                        encoding = 'ISO-8859-1'

                    html = raw_data.decode(encoding, errors='replace')
                    return html

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < retries - 1:
                    self.warning(f"Request failed for {url}: {e}. Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    self.error(f"Request failed for {url}: {e}. No more retries.")
                    return None

    async def fetch_binary_with_retries(self, session, url, retries=3, delay=1, timeout=20):
        """
        Fetch the binary content of a URL with retries in case of transient failures.

        Args:
        - session (aiohttp.ClientSession): The current session for HTTP requests.
        - url (str): The URL to fetch content from.
        - retries (int): The number of retry attempts.
        - delay (int): The delay between retries in seconds.
        - timeout (int): The timeout for the request in seconds.

        Returns:
        - bytes: The binary content, or None if all retries fail.
        """
        for attempt in range(retries):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    return await response.read()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < retries - 1:
                    self.warning(f"Request failed for {url}: {e}. Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    self.error(f"Request failed for {url}: {e}. No more retries.")
                    return None

    async def fetch_page_urls(self, session: aiohttp.ClientSession, url: str) -> List[str]:
        """
        Asynchronously fetch and return all valid URLs from the given page.

        Args:
        - session (aiohttp.ClientSession): The current session for HTTP requests.
        - url (str): The URL of the page to fetch links from.

        Returns:
        - List[str]: A list of valid URLs found on the page.
        """
        self.info(f"Fetching page URLs from: {url}")
        html = await self.fetch_text_with_retries(session, url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')

        def is_valid_url(href: str) -> bool:
            """
            Check if the URL is valid for crawling.

            Args:
            - href (str): The URL to validate.

            Returns:
            - bool: True if the URL is valid, False otherwise.
            """
            # Exclude URLs that contain unwanted prefixes or video files
            if any(prefix in href for prefix in self.excluded_prefixes):
                return False
            if any(href.endswith(ext) for ext in self.excluded_extensions):
                return False

            # Continue with existing checks
            return href.startswith('https') and 'uel' in href

        new_links = [urljoin(self.base_url, a_tag['href']) for a_tag in soup.find_all('a', href=True) if
                     is_valid_url(a_tag['href'])]
        self.info(f"Found {len(new_links)} URLs on {url}")
        return new_links

    async def fetch_page_contents(self, session: aiohttp.ClientSession, url: str) -> Dict[str, Union[str, List[str]]]:
        """
        Asynchronously fetch and return the content (title, metadata, text) of the page.

        Args:
        - session (aiohttp.ClientSession): The current session for HTTP requests.
        - url (str): The URL of the page to fetch content from.

        Returns:
        - Dict[str, Union[str, List[str]]]: A dictionary containing the page's title, metadata, text, and URL.
        """
        self.info(f"Fetching page contents from URL: {url}")
        html = await self.fetch_text_with_retries(session, url)
        if not html:
            return {}

        content_hash = hashlib.md5(html.encode('utf-8')).hexdigest()

        if content_hash in self.visited_hashes:
            self.info(f"Skipping duplicate content for URL: {url}")
            return {}

        self.visited_hashes.add(content_hash)

        soup = BeautifulSoup(html, 'html.parser')
        title = soup.title.string if soup.title else "No Title"

        # Extract all metadata tags
        metadata = {meta.attrs.get("name", meta.attrs.get("property", "unknown")): meta.attrs.get("content", "")
                    for meta in soup.find_all("meta")}

        # Join metadata into a single string with key-value pairs
        metadata_str = "; ".join([f"{key}: {value}" for key, value in metadata.items()])

        page_text = soup.get_text(separator='\n', strip=True)

        page_content = {
            "url": url,
            "title": title,
            "metadata": metadata_str,
            "contents": page_text
        }
        self.info(f"Extracted contents from {url}")
        return page_content

    async def fetch_pdf_urls(self, session: aiohttp.ClientSession, url: str) -> List[str]:
        """
        Asynchronously fetch and return all PDF URLs from the given page.

        Args:
        - session (aiohttp.ClientSession): The current session for HTTP requests.
        - url (str): The URL of the page to fetch PDF links from.

        Returns:
        - List[str]: A list of PDF URLs found on the page.
        """
        self.info(f"Fetching PDF URLs from: {url}")
        html = await self.fetch_text_with_retries(session, url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        pdfs_on_page = [
            urljoin(self.base_url, a_tag['href']) for a_tag in soup.find_all('a', href=True)
            if a_tag['href'].endswith('.pdf')
        ]
        self.info(f"Found {len(pdfs_on_page)} PDF URLs on {url}")
        return pdfs_on_page

    async def download_pdf(self, session: aiohttp.ClientSession, pdf_url: str):
        """
        Asynchronously download a single PDF file.

        Args:
        - session (aiohttp.ClientSession): The current session for HTTP requests.
        - pdf_url (str): The URL of the PDF to download.
        """
        pdf_name = os.path.join(self.download_folder, os.path.basename(pdf_url))
        self.info(f"Downloading PDF: {pdf_name}")
        raw_data = await self.fetch_binary_with_retries(session, pdf_url)
        if not raw_data:
            return

        try:
            async with aiofiles.open(pdf_name, 'wb') as pdf_file:
                await pdf_file.write(raw_data)
            self.info(f"Downloaded PDF: {pdf_name}")
        except aiohttp.ClientError as e:
            self.error(f"Failed to download {pdf_url}: {e}")

    async def crawl_and_update_links(self, session: aiohttp.ClientSession, url: str, depth: int = 0) -> None:
        """
        Recursively and asynchronously crawl the website starting from the given URL.

        Args:
        - session (aiohttp.ClientSession): The current session for HTTP requests.
        - url (str): The URL to start crawling from.
        - depth (int): The current depth of the crawl.
        """
        if depth > self.max_depth:
            self.info(f"Maximum depth reached at URL: {url}")
            return
        if url in self.visited_urls:
            return

        self.visited_urls.add(url)
        self.info(f"Crawling URL: {url} at depth: {depth}")

        new_links = await self.fetch_page_urls(session, url)
        for link in new_links:
            if link not in self.visited_urls:
                self.page_urls.append(link)
                await self.crawl_and_update_links(session, link, depth + 1)

    async def start_extract(self, output_csv: str = "src/database/extracted_files/page_contents.csv"):
        """
        Start the extraction process: crawling pages, fetching content and PDFs, and saving them.

        Args:
        - output_csv (str): The path to the CSV file where the page contents will be saved.
        """
        self.info("Starting extraction process.")
        async with aiohttp.ClientSession() as session:
            await self.crawl_and_update_links(session, self.base_url)

            # Fetch contents and PDFs for each URL
            tasks = []
            for url in self.page_urls:
                content_task = self.fetch_page_contents(session, url)
                pdf_task = self.fetch_pdf_urls(session, url)
                tasks.append(asyncio.gather(content_task, pdf_task))

            results = await asyncio.gather(*tasks)

            for content, pdfs in results:
                if content:
                    self.page_contents.append(content)
                if pdfs:
                    self.pdf_urls.update(pdfs)

            # Download PDFs asynchronously
            download_tasks = [self.download_pdf(session, pdf_url) for pdf_url in self.pdf_urls]
            await asyncio.gather(*download_tasks)

        # Save contents to CSV
        self.save_page_contents(output_csv)
        self.info("Extraction process completed.")

    def save_page_contents(self, filename: str) -> None:
        """
        Save the fetched page contents to a CSV file.

        Args:
        - filename (str): The path to the CSV file where the page contents will be saved.
        """
        self.info(f"Saving page contents to CSV file: {filename}")
        df = pd.DataFrame(self.page_contents)
        df.to_csv(filename, index=False)
        self.info(f"Page contents saved to {filename}")
