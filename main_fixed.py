import asyncio
import os
import sys
from apify import Actor
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MRPLScraper:
    def __init__(self, max_pages=10, delay=2.0):
        self.max_pages = max_pages
        self.delay = delay
        self.session = requests.Session()
        
        # Configure session to handle SSL issues
        self.session.verify = False  # Disable SSL verification
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Set timeouts and retries
        self.session.timeout = 30
    
    async def test_connection(self):
        """Test if we can connect to the website"""
        test_urls = [
            'https://mrpl.co.in/en/',
            'http://mrpl.co.in/en/',  # Try HTTP if HTTPS fails
            'https://mrpl.co.in/',
            'http://mrpl.co.in/'
        ]
        
        for url in test_urls:
            try:
                Actor.log.info(f"🔍 Testing connection to: {url}")
                response = self.session.get(url, timeout=10, verify=False)
                if response.status_code == 200:
                    Actor.log.info(f"✅ Connection successful to: {url}")
                    return url
                else:
                    Actor.log.warning(f"⚠️ Got status {response.status_code} from: {url}")
            except Exception as e:
                Actor.log.warning(f"❌ Connection failed to {url}: {str(e)}")
                continue
        
        return None
    
    async def scrape_page(self, url):
        """Scrape a single page with enhanced error handling"""
        try:
            Actor.log.info(f"🌐 Scraping: {url}")
            
            # Try multiple approaches
            response = None
            
            # Approach 1: HTTPS with SSL disabled
            try:
                response = self.session.get(url, timeout=30, verify=False)
                response.raise_for_status()
            except Exception as e:
                Actor.log.warning(f"HTTPS failed for {url}: {str(e)}")
                
                # Approach 2: Try HTTP instead of HTTPS
                if url.startswith('https://'):
                    http_url = url.replace('https://', 'http://')
                    try:
                        Actor.log.info(f"🔄 Trying HTTP: {http_url}")
                        response = self.session.get(http_url, timeout=30, verify=False)
                        response.raise_for_status()
                        url = http_url  # Update URL for logging
                    except Exception as e2:
                        Actor.log.error(f"HTTP also failed for {http_url}: {str(e2)}")
                        return None
                else:
                    return None
            
            if not response:
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
                '.main-content', '.content', 'main', '.page-content', 'article', 'body'
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
            
            # Limit content length
            content = content[:1500] if len(content) > 1500 else content
            
            # Extract links
            links = []
            pdf_links = []
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href:
                    # Handle relative URLs
                    if href.startswith('/'):
                        base_url = url.split('/')[0] + '//' + url.split('/')[2]
                        absolute_url = base_url + href
                    elif href.startswith('http'):
                        absolute_url = href
                    else:
                        absolute_url = requests.compat.urljoin(url, href)
                    
                    if href.lower().endswith('.pdf'):
                        pdf_links.append(absolute_url)
                    elif 'mrpl.co.in' in absolute_url or href.startswith('/'):
                        links.append(absolute_url)
            
            result = {
                'url': url,
                'title': title,
                'description': description,
                'content': content,
                'links': links[:10],  # Limit to 10 links
                'pdf_links': pdf_links,
                'scraped_at': datetime.now().isoformat(),
                'content_length': len(content),
                'links_count': len(links),
                'status_code': response.status_code
            }
            
            Actor.log.info(f"✅ Successfully scraped: {title[:50]}... (Status: {response.status_code})")
            return result
            
        except Exception as e:
            Actor.log.error(f"❌ Error scraping {url}: {str(e)}")
            return None
    
    async def run(self):
        """Run the scraper with connection testing"""
        Actor.log.info(f"🚀 Starting MRPL scraper with max_pages={self.max_pages}, delay={self.delay}")
        
        # Test connection first
        working_base_url = await self.test_connection()
        if not working_base_url:
            Actor.log.error("❌ Could not establish connection to MRPL website")
            return 0
        
        Actor.log.info(f"🔗 Using base URL: {working_base_url}")
        
        # Determine protocol and base URL
        if working_base_url.startswith('http://'):
            protocol = 'http://'
            base_domain = 'mrpl.co.in'
        else:
            protocol = 'https://'
            base_domain = 'mrpl.co.in'
        
        # Start with main pages - adjust URLs based on working protocol
        start_urls = [
            f"{protocol}{base_domain}/en/",
            f"{protocol}{base_domain}/en/about-us",
            f"{protocol}{base_domain}/en/products",
            f"{protocol}{base_domain}/en/media-center",
            f"{protocol}{base_domain}/en/careers",
            f"{protocol}{base_domain}/en/investors",
            f"{protocol}{base_domain}/en/csr",
            f"{protocol}{base_domain}/en/tenders"
        ]
        
        visited_urls = set()
        to_visit = start_urls.copy()
        pages_scraped = 0
        
        while to_visit and pages_scraped < self.max_pages:
            url = to_visit.pop(0)
            
            if url in visited_urls:
                continue
            
            Actor.log.info(f"📄 Processing page {pages_scraped + 1}/{self.max_pages}")
            
            page_data = await self.scrape_page(url)
            
            if page_data:
                # Push data to Apify dataset
                await Actor.push_data(page_data)
                
                visited_urls.add(url)
                pages_scraped += 1
                
                Actor.log.info(f"📊 Page {pages_scraped} completed. Title: {page_data['title'][:50]}...")
                
                # Add new URLs to visit (from links found)
                for link in page_data.get('links', []):
                    if link not in visited_urls and link not in to_visit and len(to_visit) < 50:
                        to_visit.append(link)
                
                # Delay between requests
                if pages_scraped < self.max_pages:
                    Actor.log.info(f"⏱️ Waiting {self.delay} seconds...")
                    time.sleep(self.delay)
            else:
                Actor.log.warning(f"⚠️ Failed to scrape: {url}")
        
        Actor.log.info(f"🏁 Scraping completed! Total pages scraped: {pages_scraped}")
        return pages_scraped

async def main():
    """Main function for Apify Actor"""
    async with Actor:
        Actor.log.info("🎬 MRPL Web Scraper Actor Starting...")
        
        # Get input from Apify
        actor_input = await Actor.get_input() or {}
        
        # Configuration with defaults
        max_pages = actor_input.get('max_pages', 20)
        delay = actor_input.get('delay', 2)
        
        Actor.log.info(f"📥 Input configuration: max_pages={max_pages}, delay={delay}")
        
        # Validate input
        if max_pages > 200:
            Actor.log.warning("⚠️ max_pages limited to 200 for performance")
            max_pages = 200
        
        if delay < 1:
            Actor.log.warning("⚠️ delay increased to 1 second minimum")
            delay = 1
        
        try:
            # Initialize and run scraper
            scraper = MRPLScraper(max_pages=max_pages, delay=float(delay))
            pages_scraped = await scraper.run()
            
            # Log final statistics
            Actor.log.info(f"📈 Final Statistics:")
            Actor.log.info(f"   • Pages scraped: {pages_scraped}")
            Actor.log.info(f"   • Max pages requested: {max_pages}")
            Actor.log.info(f"   • Delay used: {delay}s")
            
            if pages_scraped > 0:
                Actor.log.info("✅ Scraping completed successfully!")
            else:
                Actor.log.error("❌ No pages were scraped - check connection issues")
                
        except Exception as e:
            Actor.log.error(f"💥 Fatal error: {str(e)}")
            import traceback
            Actor.log.error(f"📋 Traceback: {traceback.format_exc()}")
            raise

if __name__ == '__main__':
    asyncio.run(main())

