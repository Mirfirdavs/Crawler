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
#print(crawler.getInfo())
print(get_top_20_domain)
for k, v in get_top_20_domain.items():
    print(v, k)

WL_dict, UL_dict, LBU_dict = crawler.getInfo()

wl = V()

wl.WL_graph(WL_dict)
wl.LBU_graph(UL_dict)
wl.UL_graph(LBU_dict)