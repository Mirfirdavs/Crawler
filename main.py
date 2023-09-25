import Crawler

crawler = Crawler.Crawler('dbfilename.db')
crawler.initDB()
crawler.crawl(['https://habr.com/ru/articles/763188/'])