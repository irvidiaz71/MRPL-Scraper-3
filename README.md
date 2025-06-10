# MRPL Web Scraper - Apify Actor

A complete Apify Actor for scraping the MRPL (Mangalore Refinery and Petrochemicals Limited) website.

## 🚀 Features

- **Complete website scraping** of MRPL.co.in
- **Intelligent content extraction** (titles, descriptions, content, links)
- **PDF document detection** and cataloging
- **Respectful scraping** with configurable delays
- **Robust error handling** and logging
- **Apify dataset integration** for easy data export

## 📁 Files Included

- `main.py` - Main scraper logic with Apify integration
- `Dockerfile` - Docker configuration for Python environment
- `requirements.txt` - Python dependencies
- `apify.json` - Apify actor configuration
- `INPUT_SCHEMA.json` - Input parameter schema
- `README.md` - This documentation

## 🔧 Configuration

### Input Parameters

- **max_pages** (integer, default: 20): Maximum number of pages to scrape (1-200)
- **delay** (number, default: 2.0): Delay between requests in seconds (1.0-10.0)

### Example Input

```json
{
  "max_pages": 50,
  "delay": 3.0
}
```

## 📊 Output Data

Each scraped page returns:

```json
{
  "url": "https://mrpl.co.in/en/about-us",
  "title": "About Us - MRPL",
  "description": "Learn about MRPL's history and mission",
  "content": "Main page content...",
  "links": ["https://mrpl.co.in/en/careers", "..."],
  "pdf_links": ["https://mrpl.co.in/documents/report.pdf"],
  "scraped_at": "2024-01-01T12:00:00.000Z",
  "content_length": 1500,
  "links_count": 25
}
```

## 🚀 Deployment Instructions

### 1. Upload to GitHub

1. Create a new repository on GitHub
2. Upload all these files to the repository
3. Make sure the repository is public or accessible to Apify

### 2. Create Apify Actor

1. Go to [Apify Console](https://console.apify.com/)
2. Click "Create new" → "Actor"
3. Choose "Import from Git repository"
4. Enter your GitHub repository URL
5. Click "Create"

### 3. Build and Test

1. In your Actor, go to "Build" tab
2. Click "Build" button
3. Wait for build to complete
4. Go to "Console" tab
5. Click "Start" with test input:
   ```json
   {
     "max_pages": 5,
     "delay": 2.0
   }
   ```

### 4. Monitor Results

- Check the "Log" tab for scraping progress
- Check "Storage" → "Datasets" for scraped data
- Data can be exported as JSON, CSV, or Excel

## 🔍 Troubleshooting

### Common Issues

1. **Build fails**: Check that all files are in the root directory
2. **No data scraped**: Check logs for error messages
3. **Timeout errors**: Increase delay parameter
4. **Rate limiting**: Reduce max_pages or increase delay

### Log Messages

- `🎬 MRPL Web Scraper Actor Starting...` - Actor initialized
- `🌐 Scraping: [URL]` - Currently scraping a page
- `✅ Successfully scraped: [Title]` - Page scraped successfully
- `📊 Page X completed` - Page processed and data saved
- `🏁 Scraping completed!` - All pages processed

## 📈 Performance

- **Speed**: ~2-3 pages per minute (with 2s delay)
- **Memory**: ~100MB RAM usage
- **Storage**: ~1-5KB per page scraped
- **Limits**: Max 200 pages per run

## 🛡️ Best Practices

- **Respectful scraping**: Default 2-second delay between requests
- **Error handling**: Continues scraping even if individual pages fail
- **Data validation**: Cleans and validates extracted content
- **Logging**: Comprehensive logging for debugging

## 📞 Support

For issues or questions:
1. Check the Actor logs in Apify Console
2. Verify input parameters are within valid ranges
3. Test with smaller max_pages first (5-10 pages)

## 📄 License

This scraper is designed for educational and research purposes. Please respect MRPL's robots.txt and terms of service.

