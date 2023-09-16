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
        pass

    # 2. Получение текста страницы
    def getTextOnly(self, text):
        pass

    # 3. Разбиение текста на слова
    def separateWords(self, text):
        return text.split()

    # 4. Проиндексирован ли URL (проверка наличия URL в БД)
    def isIndexed(self, url):
        return False

    # 5. Добавление ссылки с одной страницы на другую
    def addLinkRef(self, urlFrom, urlTo, linkText):
        pass


    # 6. Непосредственно сам метод сбора данных.
    # Начиная с заданного списка страниц, выполняет поиск в ширину
    # до заданной глубины, индексируя все встречающиеся по пути страницы
    def crawl(self, urlList, maxDepth=2):
        print("6. Обход страниц")
        
        for currDepth in range(0, maxDepth):

            # Вар.1. обход каждого url на текущей глубине
            for url in  urlList:

                # получить HTML-код страницы по текущему url
                html_doc = requests.get(url).text

                # использовать парсер для работа тегов
                soup = BeautifulSoup(html_doc, "html.parser")
                
                listUnwantedItems = ['script', 'style']
                for script in soup.find_all(listUnwantedItems):
                    script.decompose()

                # получить список тэгов <a> с текущей страницы
                foundLinks = soup.find_all('a')
                for link in foundLinks:
                    if link['href'] and link['href'] == '#':
                        continue
                    if link['href'] in urlList:
                        continue
                    if link['href'] in links:
                        continue
                    if link['href'].startswith('/'):
                        pass
                    else:
                        pass
                # обработать каждый тэг <a>
                #   проверить наличие атрибута 'href'
                #   убрать пустые ссылки, вырезать якоря из ссылок, и т.д.
                #   выделить ссылку 
                #   добавить ссылку в список следующих на обход
                #   извлечь из тэг <a> текст linkText
                #   добавить в таблицу linkbeetwenurl БД ссылку с одной страницы на другую

                # вызвать функцию класса Crawler для добавления содержимого в индекс
                self.addToIndex (soup, url)

                # конец обработки текущ url
                pass

            # конец обработки всех URL на данной глубине
            pass
        pass  


    # 7. Инициализация таблиц в БД
    def initDB(self, ):
        print("Создаем пустые таблицы с необходимой структурой")
        # Устанавливаем соединение с базой данных
        cursor = self.conn.cursor()

        # Создаем таблицы
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Users (
            rowId INTEGER PRIMARY KEY,
            word TEXT,
            isFiltred INT);
        ''')

        cursor.execute("""CREATE TABLE IF NOT EXISTS URLList (
            rowID INT PRIMARY KEY,
            URL TEXT);
        """)

        cursor.execute("""CREATE TABLE IF NOT EXISTS wordLocation (
            rowID INT PRIMARY KEY,
            fk_wordId INT,
            fk_URLId INT,
            location INT);
        """)

        cursor.execute("""CREATE TABLE IF NOT EXISTS linkBetweenURL (
            rowID INT PRIMARY KEY,
            fk_FromURL_Id INT,
            fk_ToURL_id INT);
        """)

        cursor.execute('''CREATE TABLE IF NOT EXISTS linkWord (
            rowId INT PRIMARY KEY,
            fk_wordId INT,
            fk_linkId INT);
        ''')
        self.conn.commit()

    # 8. Вспомогательная функция для получения идентификатора и
    # добавления записи, если такой еще нет
    def getEntryId(self, tableName, fieldName, value):
        return 1
