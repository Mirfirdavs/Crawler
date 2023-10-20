import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import random


class Crawler:
    # 0. Конструктор Инициализация паука с параметрами БД
    def __init__(self, dbFileName):
        print("Конструктор работает".center(70, '-'))
        self.conn=sqlite3.connect(dbFileName)
        self.wordList_dict = {}
        self.urlList_dct ={}
        self.domainCounter = {}
        
        self.wordList_list  = list()
        self.urlList_list   = list()
        self.wordloc_list   = list()
        
    def getInfo(self):
        
        cursor = self.conn.cursor()
        list_of_keys = cursor.execute("""SELECT fk_FromURL_Id, COUNT(fk_ToURL_Id) AS link_count
                                        FROM linkBetweenURL
                                        GROUP by fk_FromURL_Id;
                                        """).fetchall()
        for key, value in list_of_keys:
            self.urlList_dct[key] = value
        
        WL_dict = self.wordList_dict.copy()
        UL_dict = self.urlList_dct.copy()
        LBU_dict = self.urlList_dct.copy()
        return (WL_dict, UL_dict, LBU_dict)
    
    def get_top_20_domain(self):
        domains = self.domainCounter.copy()
        domains = sorted(domains.items(), key=lambda x: x[1], reverse=True)
        top_20_domain = dict(list(dict(domains).items())[:20])
        return top_20_domain
    
    # 0. Деструктор
    def __del__(self):
        print("Деструктор".center(70, '-'))
        self.conn.close()
    
    # Добавление url в таблицу URLList
    def addUrlToURLList(self, url):
        url = str(url)
        url_parts = url.split('/')
        if len(url_parts)>=2:
            domain = url_parts[2]
            self.domainCounter[domain] = self.domainCounter.get(domain, 0) + 1
        cursor = self.conn.cursor()
        result = cursor.execute("SELECT URL FROM URLList WHERE URL=?; ", (url,)).fetchone()
        if result is None:
            cursor.execute("INSERT INTO URLList (URL) VALUES (?) ", (url,))
        self.conn.commit()
    
    # 1. Индексирование одной страницы
    def addIndex(self, soup, url):
        url = str(url)
        separatedWordList = self.getTextOnly(soup)
        
        cursor = self.conn.cursor()
        result = cursor.execute("SELECT rowId FROM URLList WHERE URL=?", (url,)).fetchone()

        # Добавьте каждое слово в таблицу wordLocation
        for index, word in enumerate(separatedWordList):
            isWordInDB = cursor.execute("SELECT word FROM wordList WHERE word=?", (word,)).fetchone()
            if isWordInDB is None:
                cursor.execute("INSERT INTO wordList (word) VALUES (?)", (word,))
                self.wordList_dict[result[0]] = self.wordList_dict.get(result[0], 0) + 1

            wordId = cursor.execute("SELECT rowId FROM wordList WHERE word=?", (word,)).fetchone()
            cursor.execute("INSERT INTO wordLocation (fk_wordId, fk_URLId, location) VALUES (?, ?, ?)", (wordId[0], result[0], index))
        self.conn.commit()


    # 2. Получение текста страницы
    def getTextOnly(self, soup):
        allTextOnPage = soup.text
        pattern = re.compile(r'\b[a-zA-Zа-яА-Я]+\b')
        separatedWordList = re.findall(pattern, allTextOnPage)
        
        return separatedWordList

    # 3. Разбиение текста на слова
    def separateWords(self, text):
        return text

    # 4. Проиндексирован ли URL (проверка наличия URL в БД)
    def isIndexed(self, url):
        cursor = self.conn.cursor()
        url_id = cursor.execute("SELECT rowId FROM URLList WHERE URL=?", (url, )).fetchone()
        if url_id is None:
            return False
        
        url_id = url_id[0]
        result = cursor.execute("SELECT rowId FROM wordLocation WHERE fk_URLId=?", (url_id, )).fetchone()
        #True если ссылка проиндексирована False если нет
        return not (result is None)

    # 5. Добавление ссылки с одной страницы на другую
    def addLinkRef(self, urlFrom, urlTo):
        cursor = self.conn.cursor()

        # Получите идентификатор URl для страницы, с которой исходит ссылка
        cursor.execute("SELECT rowId FROM URLList WHERE URL=?", (urlFrom,))
        fromId = cursor.fetchone()[0]

        # Получите идентификатор URL для страницы, на которую ведет ссылка
        cursor.execute("SELECT rowId FROM URLList WHERE URL=?", (urlTo,))
        toId = cursor.fetchone()[0]

        # Проверка на повторения
        checkToId = cursor.execute("SELECT fk_ToURL_Id FROM linkBetweenURL WHERE fk_ToURL_Id=?; ", (toId, )).fetchone()
        
        if checkToId is None:
            cursor.execute("INSERT INTO linkBetweenURL (fk_FromURL_Id, fk_ToURL_Id) VALUES (?, ?) ", (fromId, toId))
        self.conn.commit()

    # 6. Непосредственно сам метод сбора данных.
    # Начиная с заданного списка страниц, выполняет поиск в ширину
    # до заданной глубины, индексируя все встречающиеся по пути страницы
    def crawl(self, urlList, maxDepth=15):
        print("6. Обход страниц")
        COUNTER_PARSED_ULR = 0
        for currDepth in range(0, maxDepth):
            # Создаем список для хранения URL следующей глубины
            nextDepthURLs = []
            # Вар.1. обход каждого url на текущей глубине
            for url in  urlList:
                cursor = self.conn.cursor()
                if url.endswith('/'):
                    url = url[:-1]
                try:
                    check = requests.get(url)
                    if check.status_code != 200:
                        continue
                except:
                    continue
                self.addUrlToURLList(url)
                
                print(f"Индексируем  {url}")
                if self.isIndexed(url):
                    print("Ссылка уже проиндексирована")
                    continue
                try:
                    
                    # получить HTML-код страницы по текущему url
                    http_answer = requests.get(url)
                    if http_answer.status_code != 200:
                        continue
                    
                    html_doc = http_answer.text
                    # использовать парсер для работа тегов
                    soup = BeautifulSoup(html_doc, "html.parser")
                    
                    # Вызвать функцию класса Crawler для добавления содержимого в индекс
                    self.addIndex(soup, url)
                    
                    listUnwantedItems = ['script', 'style', 'noscript']
                    for script in soup.find_all(listUnwantedItems):
                        script.decompose()
                    
                    # Индексирование аттр. Value
                    input_tags = soup.find_all('input', {'value': True})
                    self.addToAttrValue(input_tags, url)
                    
                    # получить список тэгов <a> с текущей страницы
                    foundLinks = soup.find_all('a')

                    for link in foundLinks:
                        href = link.get('href')
                        if href and not href.startswith(('#', 'mailto:')) and href not in ('/ru', '/ru/') and not href.endswith(('.pdf', '.jpg', '.png')):
                            # Преобразовать относительные URL в абсолютные
                            if not href.startswith('http'):
                                href = url + href
                            # Удаление не нужных якорей
                            if '#' in href:
                                href = href.split('#')[0]
                            if '?' in href:
                                href = href.split('?')[0]
                            
                            if href.endswith('/'):
                                href = href[:-1]
                            
                            # Добавить ссылку в список следующей глубины
                            nextDepthURLs.append(href)
                            # Добавление в табоицу URLList
                            self.addUrlToURLList(href)

                            # Извлечь текст из тега <a>
                            linkText = link.get_text()

                            # Извлечь текст
                            # Добавить ссылку в таблицу linkBetweenURL в базе данных
                            self.addLinkRef(url, href)
                            self.addToLinkWord(url, href, linkText)
                    # поличтить кол-во строк таблицы БД
                    
                    COUNTER_PARSED_ULR +=1
                    
                    resultwordLoc = cursor.execute("SELECT  count(*) FROM wordLocation;").fetchone()[0]
                    resulturlList = cursor.execute("SELECT  count(*) FROM URLList;").fetchone()[0]
                    resultwordList = cursor.execute("SELECT count(*) FROM wordList;").fetchone()[0]
                    self.wordloc_list.append(  resultwordLoc) 
                    self.urlList_list.append(  resulturlList) 
                    self.wordList_list.append( resultwordList) 
                    
                except Exception as e:
                    print(f"Ошибка при обработке {url}: {str(e)}")
                print('Переход  НА СЛЕД url ')
                
                
            print(' Переход к следующей глубине')
            nextDepthURLs = list(set(nextDepthURLs))
            if len(nextDepthURLs) >= (currDepth+2) * 2:
                urlList = random.choices(nextDepthURLs, k=(currDepth+2)*2)
            else:
                urlList = nextDepthURLs[:]
            print(f"Глубина: {currDepth+2}   Список:  {urlList}")
            nextDepthURLs.clear()
        print("Завершено.")
        print('Количество проиндексированных страниц: ', COUNTER_PARSED_ULR)

    # 7. Инициализация таблиц в БД
    def initDB(self, ):
        print("Создаем пустые таблицы с необходимой структурой")
        # Устанавливаем соединение с базой данных
        
        # удаляем если уже  есть таблицы
        cursor = self.conn.cursor()
        cursor.execute("""DROP TABLE IF EXISTS wordList""")
        cursor.execute("""DROP TABLE IF EXISTS URLList""")
        cursor.execute("""DROP TABLE IF EXISTS wordLocation""")
        cursor.execute("""DROP TABLE IF EXISTS linkBetweenURL""")
        cursor.execute("""DROP TABLE IF EXISTS linkWord""")
        cursor.execute("""DROP TABLE IF EXISTS attrValue""")

        # Создаем таблицы
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS wordList (
            rowId INTEGER PRIMARY KEY,
            word TEXT,
            isFiltred INTEGER
            );
        ''')

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS URLList (
            rowId INTEGER PRIMARY KEY,
            URL TEXT
            );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS wordLocation (
            rowId INTEGER PRIMARY KEY,
            fk_wordId INTEGER,
            fk_URLId INTEGER,
            location INTEGER,
            FOREIGN KEY (fk_wordId) REFERENCES wordList (rowId),
            FOREIGN KEY (fk_URLId) REFERENCES URLList (rowId)
            );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS linkBetweenURL (
            rowId INTEGER PRIMARY KEY,
            fk_FromURL_Id INTEGER,
            fk_ToURL_Id INTEGER,
            FOREIGN KEY (fk_FromURL_Id) REFERENCES URLList (rowId),
            FOREIGN KEY (fk_ToURL_Id) REFERENCES URLList (rowId)
            );
        """)

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS linkWord (
            rowId INTEGER PRIMARY KEY,
            fk_wordId INTEGER,
            fk_linkId INTEGER,
            FOREIGN KEY(fk_wordId) REFERENCES wordList (rowId),
            FOREIGN KEY(fk_linkId) REFERENCES linkBetweenURL (rowId)
            );
        ''')
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS attrValue (
            rowId INTEGER PRIMARY KEY,
            fk_wordId INTEGER,
            fk_URLId INTEGER,
            FOREIGN KEY(fk_wordId) REFERENCES wordList (rowId),
            FOREIGN KEY (fk_URLId) REFERENCES URLList (rowId)
            );
        """)
        self.conn.commit()

    # 8. Функция для добавления слов в таблицу linkWord
    def addToLinkWord(self, url, href, linkText):
        linkText = linkText.split()
        cursor = self.conn.cursor()
        
        cursor.execute("SELECT rowId FROM URLList WHERE URL=?; ", (url,))
        rowId_url = cursor.fetchone()[0]
        
        cursor.execute("SELECT rowId FROM URLList WHERE URL=?; ", (href,))
        rowId_href = cursor.fetchone()[0]
        
        cursor.execute("SELECT rowId FROM linkBetweenURL WHERE fk_FromURL_Id=? AND fk_ToURL_Id=?", (rowId_url, rowId_href))
        fk_linkId = cursor.fetchone()
        for word in linkText:
            cursor.execute("SELECT rowId FROM wordList WHERE word=?", (word, ))
            word_id = cursor.fetchone()
            if fk_linkId is not None and word_id is not None:
                cursor.execute("INSERT INTO linkWord (fk_wordId, fk_linkId) VALUES (?, ?)", (word_id[0], fk_linkId[0]))
        self.conn.commit()

    
    def addToAttrValue(self, input_tags, url):
        cursor = self.conn.cursor()
        cursor.execute("SELECT rowId FROM URLList WHERE URL=?; ", (url,))
        rowId_url = cursor.fetchone()
        
        for input_tag in input_tags:
            value = input_tag['value']
            isWordInDB = cursor.execute("SELECT word FROM wordList WHERE word=?", (value,)).fetchone()
            if isWordInDB is None:
                cursor.execute("INSERT INTO wordList (word) VALUES (?)", (value,))
            
                rowId = cursor.execute("SELECT rowId FROM wordList WHERE word=?", (value,)).fetchone()
                cursor.execute("INSERT INTO attrValue (fk_wordId, fk_URLId) VALUES (?, ?)", (rowId[0], rowId_url[0]))
        self.conn.commit()