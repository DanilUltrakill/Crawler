from Crawler import Crawler


crawler = Crawler("DBforCrawler.db")
start_urls = ["https://aftershock.news"]
#crawler.crawl(start_urls, maxDepth=2)
crawler.all_rows()
