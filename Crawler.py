import requests
from bs4 import BeautifulSoup
import sqlite3

class Crawler:
    # 0. Конструктор Инициализация паука с параметрами БД
    def __init__(self, dbFileName):
        print("Конструктор работает".center(70, '-'))
        
        self.conn=sqlite3.connect(dbFileName)

    # 0. Деструктор
    def __del__(self):
        print("Деструктор".center(70, '-'))
        self.conn.close()
    
    # 1. Индексирование одной страницы
    def addIndex(self, soup, url):
        cursor = self.conn.cursor()

        #Получените текст страницы (вы сможете использовать ваш метод getTextOnly)
        pageText = self.getTextOnly(soup)
        
        #Разделите текст на слова (вы можете использовать ваш метод separateWords
        words = self.separateWords(pageText)

        #Получите идентификатор URL из таблицы URLList или добавьте новый URL
        cursor.execute("SELECT rowId FROM URLList WHERE URL=?", (url,))
        row = cursor.fetchone()

        if row is None:
            cursor.execute("INSERT INTO URLList (URL) VALUES (?)", (url,))
            self.conn.commit()
            urlId = cursor.lastrowid
        else:
            urlId = row[0]

        # Добавьте каждое слово в таблицу wordLocation
        for i, word in enumerate(words):
            cursor.execute("INSERT INTO wordList (word, isFiltred) VALUES (?, 0)", (word,))
            self.conn.commit()
            wordId = cursor.lastrowid
            cursor.execute("INSERT INTO wordLocation (fk_wordId, fk_URLId, location) VALUES(?, ?, ?)", (wordId, urlId, i))

        self.conn.commit()


    # 2. Получение текста страницы
    def getTextOnly(self, text):
        return text

    # 3. Разбиение текста на слова
    def separateWords(self, text):
        return [word.lower() for word in text.split()]

    # 4. Проиндексирован ли URL (проверка наличия URL в БД)
    def isIndexed(self, url):
        cursor = self.conn.cursor()
        cursor.execute("SELECT rowId FROM URLList WHERE URL=?", (url,))
        row = cursor.fetchone()
        return row is not None

    # 5. Добавление ссылки с одной страницы на другую
    def addLinkRef(self, urlFrom, urlTo, linkText):
        cursor = self.conn.cursor()

        # Получите идентификатор URl для страницы, с которой исходит ссылка
        cursor.execute("SELECT rowId FROM URLList WHERE URL=?", (urlFrom,))
        fromId = cursor.fetchone()[0]

        # Получите идентификатор URL для страницы, на которую ведет ссылка
        cursor.execute("SELECT rowId FROM URLList WHERE URL=?", (urlTo,))
        toId = cursor.fetchone()[0]

        # Добавьте запись о ссылке
        cursor.execute("SELECT fk_ToURL_Id FROM linkBetweenURL WHERE fk_ToURL_Id=?; ", (toId, ))
        result = cursor.fetchone()
        if result is None:
            cursor.execute("INSERT INTO linkBetweenURL (fk_FromURL_Id, fk_ToURL_Id) VALUES (?, ?)", (fromId, toId))
        self.conn.commit()


    # 6. Непосредственно сам метод сбора данных.
    # Начиная с заданного списка страниц, выполняет поиск в ширину
    # до заданной глубины, индексируя все встречающиеся по пути страницы
    def crawl(self, urlList, maxDepth=1):
        print("6. Обход страниц")
        
        for currDepth in range(0, maxDepth):
            # Создаем список для хранения URL следующей глубины
            nextDepthURLs = []

            # Вар.1. обход каждого url на текущей глубине
            for url in  urlList:
                try:
                    if url.endswith('/'):
                        url = url[:-1]
                    self.addUrlToURLList(url)
                    
                    # получить HTML-код страницы по текущему url
                    html_doc = requests.get(url).text
                    # использовать парсер для работа тегов
                    soup = BeautifulSoup(html_doc, "html.parser")
                    
                    listUnwantedItems = ['script', 'style', 'noscript']
                    for script in soup.find_all(listUnwantedItems):
                        script.decompose()

                    # получить список тэгов <a> с текущей страницы
                    foundLinks = soup.find_all('a')

                    for link in foundLinks:
                        href = link.get('href')
                        if href and not href.startswith('#') and not href.startswith('mailto:'):
                            # Преобразовать относительные URL в абсолютные
                            if not href.startswith('http'):
                                href = url + href[1:]
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

                            # Добавить ссылку в таблицу linkBetweenURL в базе данных
                            self.addLinkRef(url, href, linkText)

                    # Вызвать функцию класса Crawler для добавления содержимого в индекс
                    self.addIndex(soup, url)
                except Exception as e:
                    print(f"Ошибка при обработке {url}: {str(e)}")
                    
                # Переход к следующей глубине
            urlList = nextDepthURLs

        print("Завершено.")

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
        self.conn.commit()

    # 8. Вспомогательная функция для получения идентификатора и
    # добавления записи, если такой еще нет
    def getEntryId(self, tableName, fieldName, value):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT rowId FROM {tableName} WHERE {fieldName}=?", (value,))
        row = cursor.fetchone()

        if row is not None:
            return row[0]
        else:
            cursor.execute(f"INSERT INTO {tableName} ({fieldName}) VALUES (?)", (value,))
            self.conn.commit()
            return cursor.lastrowid

    # Добавление url в таблицу URLList
    def addUrlToURLList(self, url):
        url = str(url)
        cursor = self.conn.cursor()
        result = cursor.execute("SELECT URL FROM URLList WHERE URL=?; ", (url,)).fetchall()
        
        if len(result) == 0:
            cursor.execute("INSERT INTO URLList (URL) VALUES (?) ", (url,))
        self.conn.commit()
