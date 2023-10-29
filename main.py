import Crawler

crawler = Crawler.Crawler('dbfilename.db')
crawler.initDB()

crawler.crawl([
    'https://habr.com/ru/articles/756800/',
    'https://www.nstu.ru/studies'
                ])

get_top_20_domain = crawler.get_top_20_domain()
print(get_top_20_domain)