import asyncio
import os
import sys
from apify import Actor
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import ssl
import urllib3
import io
import tempfile

# PDF processing imports
try:
    import PyPDF2
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

# Disable SSL warnings globally
urllib3.disable_warnings()

class MRPLScraperV4_WithPDF:
    def __init__(self, max_pages=10, delay=2.0, extract_pdfs=True):
        self.max_pages = max_pages
        self.delay = delay
        self.extract_pdfs = extract_pdfs
        
        # Create session with aggressive SSL bypass
        self.session = requests.Session()
        
        # Completely disable SSL verification
        self.session.verify = False
        
        # Set up adapter with SSL context that ignores everything
        from requests.adapters import HTTPAdapter
        from urllib3.util.ssl_ import create_urllib3_context
        
        class SSLAdapter(HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                ctx = create_urllib3_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                kwargs['ssl_context'] = ctx
                return super().init_poolmanager(*args, **kwargs)
        
        self.session.mount('https://', SSLAdapter())
        self.session.mount('http://', HTTPAdapter())
        
        # Set headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
    
    async def extract_pdf_text(self, pdf_url):
        """Extract text from PDF file with multiple methods"""
        try:
            Actor.log.info(f"📄 Downloading PDF: {pdf_url}")
            
            # Download PDF with timeout
            response = self.session.get(pdf_url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Check file size (limit to 50MB)
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > 50 * 1024 * 1024:
                Actor.log.warning(f"⚠️ PDF too large ({content_length} bytes), skipping: {pdf_url}")
                return None
            
            # Read PDF content
            pdf_content = response.content
            Actor.log.info(f"✅ Downloaded PDF: {len(pdf_content)} bytes")
            
            # Try multiple PDF extraction methods
            extracted_text = None
            
            # Method 1: Try pdfplumber (best for complex PDFs)
            if PDFPLUMBER_AVAILABLE and not extracted_text:
                try:
                    Actor.log.info("🔧 Trying pdfplumber extraction...")
                    with io.BytesIO(pdf_content) as pdf_file:
                        with pdfplumber.open(pdf_file) as pdf:
                            text_parts = []
                            for page_num, page in enumerate(pdf.pages[:20]):  # Limit to 20 pages
                                page_text = page.extract_text()
                                if page_text:
                                    text_parts.append(page_text)
                            
                            if text_parts:
                                extracted_text = '\n'.join(text_parts)
                                Actor.log.info(f"✅ pdfplumber extracted {len(extracted_text)} characters")
                except Exception as e:
                    Actor.log.warning(f"⚠️ pdfplumber failed: {str(e)}")
            
            # Method 2: Try PyPDF2 (fallback)
            if PDF_AVAILABLE and not extracted_text:
                try:
                    Actor.log.info("🔧 Trying PyPDF2 extraction...")
                    with io.BytesIO(pdf_content) as pdf_file:
                        pdf_reader = PyPDF2.PdfReader(pdf_file)
                        text_parts = []
                        
                        for page_num in range(min(len(pdf_reader.pages), 20)):  # Limit to 20 pages
                            page = pdf_reader.pages[page_num]
                            page_text = page.extract_text()
                            if page_text:
                                text_parts.append(page_text)
                        
                        if text_parts:
                            extracted_text = '\n'.join(text_parts)
                            Actor.log.info(f"✅ PyPDF2 extracted {len(extracted_text)} characters")
                except Exception as e:
                    Actor.log.warning(f"⚠️ PyPDF2 failed: {str(e)}")
            
            if extracted_text:
                # Clean and limit text
                extracted_text = ' '.join(extracted_text.split())  # Remove extra whitespace
                extracted_text = extracted_text[:5000] if len(extracted_text) > 5000 else extracted_text
                
                return {
                    'pdf_url': pdf_url,
                    'pdf_text': extracted_text,
                    'pdf_text_length': len(extracted_text),
                    'extraction_method': 'pdfplumber' if PDFPLUMBER_AVAILABLE else 'PyPDF2',
                    'extracted_at': datetime.now().isoformat()
                }
            else:
                Actor.log.warning(f"⚠️ No text extracted from PDF: {pdf_url}")
                return {
                    'pdf_url': pdf_url,
                    'pdf_text': '',
                    'pdf_text_length': 0,
                    'extraction_method': 'failed',
                    'error': 'No text could be extracted'
                }
                
        except Exception as e:
            Actor.log.error(f"❌ PDF extraction failed for {pdf_url}: {str(e)}")
            return {
                'pdf_url': pdf_url,
                'pdf_text': '',
                'pdf_text_length': 0,
                'extraction_method': 'failed',
                'error': str(e)
            }
    
    async def discover_urls(self):
        """Discover actual URLs from the main page"""
        Actor.log.info("🔍 DISCOVERING ACTUAL MRPL URLS...")
        
        try:
            response = self.session.get('https://mrpl.co.in/en/', timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all internal links
            discovered_urls = set()
            discovered_urls.add('https://mrpl.co.in/en/')  # Add main page
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('/') and len(href) > 1:
                    full_url = 'https://mrpl.co.in' + href
                    discovered_urls.add(full_url)
            
            # Convert to list and limit
            url_list = list(discovered_urls)[:self.max_pages]
            
            Actor.log.info(f"✅ Discovered {len(discovered_urls)} total URLs, using first {len(url_list)}")
            for i, url in enumerate(url_list[:5]):  # Log first 5
                Actor.log.info(f"   {i+1}. {url}")
            
            return url_list
            
        except Exception as e:
            Actor.log.error(f"❌ URL discovery failed: {str(e)}")
            # Fallback to known working URLs
            return [
                'https://mrpl.co.in/en/',
                'https://mrpl.co.in/Parent/About_us',
                'https://mrpl.co.in/Content/Vision_and_Mission',
                'https://mrpl.co.in/Parent/Organization',
                'https://mrpl.co.in/Content/History'
            ]
    
    async def test_connection(self):
        """Test connection to MRPL website"""
        Actor.log.info("🔧 TESTING CONNECTION TO MRPL...")
        
        test_urls = [
            'https://mrpl.co.in/en/',
            'https://mrpl.co.in/',
            'http://mrpl.co.in/en/',
            'http://mrpl.co.in/'
        ]
        
        for url in test_urls:
            try:
                Actor.log.info(f"🧪 Testing: {url}")
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    content_size = len(response.content)
                    Actor.log.info(f"✅ SUCCESS! Status: {response.status_code}, Size: {content_size} bytes")
                    
                    # Validate content
                    if 'mrpl' in response.text.lower() or 'mangalore' in response.text.lower():
                        Actor.log.info("✅ Content validation passed - contains MRPL content")
                        return True
                    else:
                        Actor.log.warning("⚠️ Content validation failed")
                else:
                    Actor.log.warning(f"⚠️ Got status {response.status_code}")
                    
            except Exception as e:
                Actor.log.warning(f"❌ Connection failed: {str(e)}")
                continue
        
        return False
    
    async def scrape_page(self, url):
        """Scrape a single page with PDF text extraction"""
        try:
            Actor.log.info(f"🌐 Scraping: {url}")
            
            response = self.session.get(url, timeout=30)
            
            if response.status_code != 200:
                Actor.log.warning(f"⚠️ HTTP {response.status_code} for {url}")
                return None
            
            # Parse content
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract title
            title_elem = soup.find('title')
            title = title_elem.get_text().strip() if title_elem else 'No title'
            
            # Extract meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            description = meta_desc.get('content', '') if meta_desc else ''
            
            # Extract main content
            content_selectors = [
                '.main-content', '.content', 'main', '.page-content', 'article', '.container', 'body'
            ]
            
            content = ''
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    content = content_elem.get_text(strip=True)
                    break
            
            # If no specific content found, get body text
            if not content:
                body = soup.find('body')
                if body:
                    content = body.get_text(strip=True)
            
            # Clean and limit content
            content = ' '.join(content.split())  # Remove extra whitespace
            web_content = content[:3000] if len(content) > 3000 else content
            
            # Extract links
            internal_links = []
            external_links = []
            pdf_links = []
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href:
                    # Handle relative URLs
                    if href.startswith('/'):
                        absolute_url = 'https://mrpl.co.in' + href
                    elif href.startswith('http'):
                        absolute_url = href
                    else:
                        absolute_url = requests.compat.urljoin(url, href)
                    
                    if href.lower().endswith('.pdf'):
                        pdf_links.append(absolute_url)
                    elif 'mrpl.co.in' in absolute_url:
                        internal_links.append(absolute_url)
                    elif href.startswith('http'):
                        external_links.append(absolute_url)
            
            # Extract PDF text if enabled
            pdf_documents = []
            if self.extract_pdfs and pdf_links:
                Actor.log.info(f"📋 Found {len(pdf_links)} PDFs, extracting text...")
                
                for pdf_url in pdf_links[:5]:  # Limit to 5 PDFs per page
                    pdf_data = await self.extract_pdf_text(pdf_url)
                    if pdf_data:
                        pdf_documents.append(pdf_data)
                        
                        # Small delay between PDF downloads
                        if len(pdf_documents) < len(pdf_links[:5]):
                            time.sleep(1)
            
            # Combine all text content
            all_text_content = web_content
            if pdf_documents:
                pdf_texts = [doc['pdf_text'] for doc in pdf_documents if doc['pdf_text']]
                if pdf_texts:
                    all_text_content += '\n\n--- PDF CONTENT ---\n\n' + '\n\n'.join(pdf_texts)
            
            # Create result
            result = {
                'url': url,
                'title': title,
                'description': description,
                'web_content': web_content,
                'web_content_length': len(web_content),
                'pdf_documents': pdf_documents,
                'pdf_count': len(pdf_documents),
                'all_text_content': all_text_content,
                'total_text_length': len(all_text_content),
                'internal_links': internal_links[:15],
                'external_links': external_links[:5],
                'pdf_links': pdf_links,
                'scraped_at': datetime.now().isoformat(),
                'total_links': len(internal_links) + len(external_links),
                'status_code': response.status_code,
                'page_size_bytes': len(response.content)
            }
            
            Actor.log.info(f"✅ Successfully scraped: {title[:50]}...")
            Actor.log.info(f"📊 Web content: {len(web_content)} chars, PDFs: {len(pdf_documents)}, Total text: {len(all_text_content)} chars")
            
            return result
            
        except Exception as e:
            Actor.log.error(f"❌ Error scraping {url}: {str(e)}")
            return None
    
    async def run(self):
        """Run the scraper with comprehensive testing and PDF extraction"""
        Actor.log.info("🚀 MRPL SCRAPER V4 WITH PDF TEXT EXTRACTION!")
        Actor.log.info("📄 This version extracts text from BOTH web pages AND PDF files!")
        Actor.log.info(f"⚙️ Configuration: max_pages={self.max_pages}, delay={self.delay}, extract_pdfs={self.extract_pdfs}")
        
        # Check PDF libraries
        if self.extract_pdfs:
            if PDFPLUMBER_AVAILABLE:
                Actor.log.info("✅ pdfplumber available for PDF text extraction")
            elif PDF_AVAILABLE:
                Actor.log.info("✅ PyPDF2 available for PDF text extraction")
            else:
                Actor.log.warning("⚠️ No PDF libraries available, will skip PDF text extraction")
                self.extract_pdfs = False
        
        # Test connection first
        if not await self.test_connection():
            Actor.log.error("❌ Could not establish connection to MRPL website")
            return 0
        
        # Discover actual URLs
        urls_to_scrape = await self.discover_urls()
        
        if not urls_to_scrape:
            Actor.log.error("❌ No URLs discovered to scrape")
            return 0
        
        Actor.log.info(f"📋 Will scrape {len(urls_to_scrape)} pages with PDF text extraction")
        
        # Scrape pages
        pages_scraped = 0
        total_pdfs_processed = 0
        
        for i, url in enumerate(urls_to_scrape):
            Actor.log.info(f"📄 Processing page {i+1}/{len(urls_to_scrape)}")
            
            page_data = await self.scrape_page(url)
            
            if page_data:
                # Push data to Apify dataset
                await Actor.push_data(page_data)
                pages_scraped += 1
                total_pdfs_processed += page_data.get('pdf_count', 0)
                
                Actor.log.info(f"📊 Page {i+1} completed - Web: {page_data['web_content_length']} chars, PDFs: {page_data['pdf_count']}")
                
                # Delay between requests (except for last page)
                if i < len(urls_to_scrape) - 1:
                    Actor.log.info(f"⏱️ Waiting {self.delay} seconds...")
                    time.sleep(self.delay)
            else:
                Actor.log.warning(f"⚠️ Failed to scrape page {i+1}")
        
        Actor.log.info(f"🏁 Scraping completed!")
        Actor.log.info(f"📊 Pages scraped: {pages_scraped}")
        Actor.log.info(f"📄 PDFs processed: {total_pdfs_processed}")
        
        return pages_scraped

async def main():
    """Main function for Apify Actor with PDF extraction"""
    async with Actor:
        Actor.log.info("🎬 MRPL WEB SCRAPER V4 - WITH PDF TEXT EXTRACTION!")
        Actor.log.info("📄 Extracts text from BOTH web pages AND PDF documents!")
        
        # Get input from Apify
        actor_input = await Actor.get_input() or {}
        
        # Configuration with defaults
        max_pages = actor_input.get('max_pages', 10)  # Reduced default due to PDF processing
        delay = actor_input.get('delay', 3)  # Increased delay for PDF processing
        extract_pdfs = actor_input.get('extract_pdfs', True)
        
        Actor.log.info(f"📥 Input: max_pages={max_pages}, delay={delay}, extract_pdfs={extract_pdfs}")
        
        # Validate input
        if max_pages > 50:
            Actor.log.warning("⚠️ max_pages limited to 50 for PDF processing performance")
            max_pages = 50
        
        if delay < 2:
            Actor.log.warning("⚠️ delay increased to 2 seconds minimum for PDF processing")
            delay = 2
        
        try:
            # Initialize and run scraper
            scraper = MRPLScraperV4_WithPDF(
                max_pages=max_pages, 
                delay=float(delay),
                extract_pdfs=extract_pdfs
            )
            pages_scraped = await scraper.run()
            
            # Log final statistics
            Actor.log.info(f"📈 FINAL STATISTICS:")
            Actor.log.info(f"   • Pages scraped: {pages_scraped}")
            Actor.log.info(f"   • Max pages requested: {max_pages}")
            Actor.log.info(f"   • Delay used: {delay}s")
            Actor.log.info(f"   • PDF extraction: {'Enabled' if extract_pdfs else 'Disabled'}")
            
            if pages_scraped > 0:
                Actor.log.info("✅ SCRAPING WITH PDF EXTRACTION COMPLETED!")
                Actor.log.info("📊 Check your dataset for web content + PDF text data")
            else:
                Actor.log.error("❌ No pages were scraped - check logs for issues")
                
        except Exception as e:
            Actor.log.error(f"💥 Fatal error: {str(e)}")
            import traceback
            Actor.log.error(f"📋 Full traceback: {traceback.format_exc()}")
            raise

if __name__ == '__main__':
    asyncio.run(main())

