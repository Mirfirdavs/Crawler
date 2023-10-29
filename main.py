import Crawler
from visualization import Visualization as V

crawler = Crawler.Crawler('dbfilename.db')
crawler.initDB()


crawler.crawl([
    'https://habr.com/ru/articles/756800/',
    'https://dzen.ru/news/story/Putin_strany_Zolotogo_milliarda_teryayut_pozicii_vmire_poobektivnym_prichinam--86aeb854930a4a5fea6129e88cae4f1f?lang=ru&rubric=index&fan=1&stid=eUpdggoXpBkDpqYHp4nY&t=1697234708&tt=true&persistent_id=2757851306&story=282d3a7a-9c41-5c39-a282-c3ffe3122442&issue_tld=ru'
    #'https://www.nstu.ru/'
    #'https://www.nstu.ru/studies'
    ])

get_top_20_domain = crawler.get_top_20_domain()
print(get_top_20_domain)

wl = V()

wl.first_graph(crawler.wordList_list)
wl.second_graph(crawler.urlList_list)
wl.third_graph(crawler.wordloc_list)