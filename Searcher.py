import sqlite3
import itertools
import re


class Searcher:

    def dbcommit(self):
        """ Зафиксировать изменения в БД """
        self.conn.commit()


    def __init__(self, dbFileName):
        """  0. Конструктор """
        # открыть "соединение" получить объект "сonnection" для работы с БД
        self.conn = sqlite3.connect(dbFileName)


    def __del__(self):
        """ 0. Деструктор  """
        # закрыть соединение с БД
        self.conn.close()


    def getWordsIds(self, queryString: str) -> list[int]:
        print("getWordsIds starting")
        """
        Получение идентификаторов для каждого слова в queryString
        :param queryString: поисковый запрос пользователя
        :return: список wordlist.rowid искомых слов
        """

        queryWordsList = queryString.split()
        rowidList = list()

        # Для каждого искомого слова
        for word in queryWordsList:
            sql = "SELECT rowId FROM wordlist WHERE word =\"{}\" LIMIT 1; ".format(word)
            result_row = self.conn.execute(sql ).fetchone()
            
            if result_row != None:
                word_rowid = result_row[0]
                
                rowidList.append(word_rowid)
                print("  ", word, word_rowid)
            else:
                raise Exception("Одно из слов поискового запроса не найдено:" + word)
        # вернуть список идентификаторов
        print("getWordsIds done")
        return rowidList


    def getMatchRows(self, queryString: str) ->tuple[list[tuple[int, int, int]], list[int]]:
        print("getMatchRows starting")
        """
        Поиск комбинаций из всезх искомых слов в проиндексированных url-адресах
        :param queryString: поисковый запрос пользователя
        :return: 1) список вхождений формата (urlId, loc_q1, loc_q2, ...) loc_qN позиция на странице Nго слова из поискового запроса  "q1 q2 ..."
        """
        
        wordsList = queryString.split()
        # получить идентификаторы искомых слов
        wordsidList = self.getWordsIds(queryString)

        #Созать переменную для полного SQL-запроса
        sqlFullQuery = """"""
        # Созать объекты-списки для дополнений SQL-запроса
        sqlpart_Name = list() # имена столбцов
        sqlpart_Join = list() # INNER JOIN
        sqlpart_Condition = list() # условия WHERE

        #Конструктор SQL-запроса (заполнение обязательной и дополнительных частей)
        #обход в цикле каждого искомого слова и добавлене в SQL-запрос соответствующих частей
        for wordIndex in range(0, len(wordsList)):
            # Получить идентификатор слова
            wordID = wordsidList[wordIndex]

            if wordIndex ==0:
                # обязательная часть для первого слова
                sqlpart_Name.append("""w0.fk_URLId    fk_URLId  --идентификатор url-адреса""")
                sqlpart_Name.append("""   , w0.location w0_loc --положение первого искомого слова""")

                sqlpart_Condition.append("""WHERE w0.fk_wordId={}     -- совпадение w0 с первым словом """.format(wordID))

            else:
                # Дополнительная часть для 2,3,.. искомых слов
                if len(wordsList)>=2:
                    # Проверка, если текущее слово - второе и более
                    # Добавить в имена столбцов
                    sqlpart_Name.append(""" , w{}.location w{}_loc --положение следующего искомого слова""".format(wordIndex, wordIndex))

                    #Добавить в sql INNER JOIN
                    sqlpart_Join.append("""INNER JOIN wordLocation w{}  -- назначим псевдоним w{} для второй из соединяемых таблиц
                        on w0.fk_URLId=w{}.fk_URLId -- условие объединения""".format(wordIndex, wordIndex, wordIndex))
                    # Добавить в sql ограничивающее условие
                    sqlpart_Condition.append("""  AND w{}.fk_wordId={} -- совпадение w{}... с cоответсвующим словом """.format(wordIndex, wordID, wordIndex ))
                    

        # Объеднение запроса из отдельных частей
        sqlFullQuery += "SELECT "

        #Все имена столбцов для вывода
        for sqlpart in sqlpart_Name:
            sqlFullQuery+="\n"
            sqlFullQuery += sqlpart

        # обязательная часть таблица-источник
        sqlFullQuery += "\n"
        sqlFullQuery += "FROM wordLocation w0 "

        #часть для объединения таблицы INNER JOIN
        for sqlpart in sqlpart_Join:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        #обязательная часть и дополнения для блока WHERE
        for sqlpart in sqlpart_Condition:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        # Выполнить SQL-запроса и извлеч ответ от БД
        #print(sqlFullQuery)
        cur = self.conn.execute(sqlFullQuery).fetchall()
        rows = [row for row in cur]
        print("getMatchRows done")
        return rows, wordsidList


    def normalizeScores(self, scores: dict[int, int], smallIsBetter = 0) -> dict[int, float]:
        print("normalizeScores starting")
        resultDict = dict() # словарь с результатом

        vsmall = 0.00001  # создать переменную vsmall - малая величина, вместо деления на 0
        minscore = min(scores.values())  # получить минимум
        maxscore = max(scores.values())  # получить максимум
        # перебор каждой пары ключ значение
        for (key, val) in scores.items():

            if smallIsBetter:
                # Режим МЕНЬШЕ вх. значение => ЛУЧШЕ
                # ранг нормализованный = мин. / (тек.значение  или малую величину)
                resultDict[key] = float(minscore) / max(vsmall, val)
            else:
                # Режим БОЛЬШЕ  вх. значение => ЛУЧШЕ вычислить макс и разделить каждое на макс
                # вычисление ранга как доли от макс.
                # ранг нормализованный = тек. значения / макс.
                resultDict[key] = float(val) / maxscore
        print("normalizeScores done")
        return resultDict

    # Ранжирование. Содержимое. 2. Расположение в документе.
    def locationScore(self, rowsLoc: list[tuple[int, int, int]]) -> dict[int, float]:
        print("locationScore starting")
        """
        Расчет минимального расстояния от начала страницы у комбинации искомых слов
        :param rows: Список вхождений: urlId, loc_q1, loc_q2, .. слов из поискового запроса "q1 q2 ..." (на основе результата getmatchrows ())
        :return: словарь {UrlId1: мин. расстояния от начала для комбинации, UrlId2: мин. расстояния от начала для комбинации, }
        """

        # Создать locationsDict - словарь с расположением от начала страницы упоминаний/комбинаций искомых слов
        locationsDict = dict()

        for row in rowsLoc:
            urlId = row[0]     # Извлечение urlId из строки
            locations = row[1:]
            locationsDict.setdefault(urlId, 1_000_000)
            
            #Вычисление суммы дистанций каждого слова от начала страницы
            total_distance = sum(locations)
            
            locationsDict[urlId] = min(total_distance, locationsDict[urlId])
        print("locationScore done")
        return self.normalizeScores(locationsDict, smallIsBetter=1)

    def geturlname(self, id: int) -> str:
        """
        Получает из БД текстовое поле url-адреса по указанному urlid
        :param id: целочисленный urlid
        :return: строка с соответствующим url
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT url FROM urlList WHERE rowId=?", (id, ))
        url = cursor.fetchone()
        
        if url is not None:
            return url[0]
        return 'example.com'

    def getSortedList(self, queryString: str) -> dict[int: list[int, int, int, str]]: # type: ignore
        print("getSortedList starting")
        """
        На поисковый запрос формирует список URL, вычисляет ранги, выводит в отсортированном порядке
        :param queryString:  поисковый запрос
        :return:
        """
        rowsLoc, wordids = self.getMatchRows(queryString=queryString)


        m1Scores = self.locationScore(rowsLoc)
        m2Scores = self.pagerankScore(rowsLoc)
        
        m3Scores: dict[int, float] = dict()
        for key in m1Scores.keys():
            m3Scores[key] = (m1Scores[key] + m2Scores[key]) / 2
                
        m3Scores_sorted = dict(sorted(m3Scores.items(), reverse=True, key=lambda item: item[1]))

        sorted_urls_dct = dict()
        for k in m3Scores_sorted.keys():
            id = k
            m1 = m1Scores[k]
            m2 = m2Scores[k]
            m3 = m3Scores_sorted[k]
            url = self.geturlname(k)
            sorted_urls_dct[id] = [m1, m2, m3, url]
    
        return sorted_urls_dct

    def calculatePageRank(self, iterations=5) -> None:
        print("calculatePageRank starting")
        # Подготовка БД ------------------------------------------
        # стираем текущее содержимое таблицы PageRank
        self.conn.execute('DROP TABLE IF EXISTS pageRank')
        self.conn.execute("""CREATE TABLE  IF NOT EXISTS  pageRank(
                                rowId INTEGER PRIMARY KEY AUTOINCREMENT,
                                urlId INTEGER,
                                score REAL
                            );""")

        # Для некоторых столбцов в таблицах БД укажем команду создания объекта "INDEX" для ускорения поиска в БД
        self.conn.execute("DROP INDEX   IF EXISTS word_Idx;")
        self.conn.execute("DROP INDEX   IF EXISTS url_Idx;")
        self.conn.execute("DROP INDEX   IF EXISTS word_Url_Idx;")
        self.conn.execute("DROP INDEX   IF EXISTS url_To_Idx;")
        self.conn.execute("DROP INDEX   IF EXISTS url_From_Idx;")
        self.conn.execute('CREATE INDEX IF NOT EXISTS word_Idx       ON wordList(word)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS url_Idx        ON URLList(URL)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS word_Url_Idx    ON wordLocation(fk_wordId)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS url_To_Idx      ON linkBetweenURL(fk_ToURL_Id)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS url_From_Idx    ON linkBetweenURL(fk_FromURL_Id)')
        self.conn.execute("DROP INDEX   IF EXISTS rank_Url_IdIdx;")
        self.conn.execute('CREATE INDEX IF NOT EXISTS rank_Url_IdIdx  ON pagerank(urlId)')
        self.conn.execute("REINDEX word_Idx;")
        self.conn.execute("REINDEX url_Idx;")
        self.conn.execute("REINDEX word_Url_Idx;")
        self.conn.execute("REINDEX url_To_Idx;")
        self.conn.execute("REINDEX url_From_Idx;")
        self.conn.execute("REINDEX rank_Url_IdIdx;")

        self.conn.execute('INSERT INTO pageRank (urlId, score) SELECT rowId, 1.0 FROM URLList')
        self.dbcommit()

        print("Page rank:")
        for i in range(iterations):
            print(f"Iteration {i}")

            all_url_ids = self.conn.execute('SELECT rowId FROM URLList').fetchall()
            new_ranks = {}

            for (url_id,) in all_url_ids:
                page_rank = 0.15

                links_to_page = self.conn.execute('SELECT DISTINCT fk_FromURL_Id FROM linkBetweenURL WHERE fk_ToURL_Id = ?', (url_id,)).fetchall()

                for (linking_id,) in links_to_page:
                    linking_page_rank = self.conn.execute('SELECT score FROM pageRank WHERE urlId = ?', (linking_id,)).fetchone()[0]
                    linking_total_links = self.conn.execute('SELECT count(*) FROM linkBetweenURL WHERE fk_FromURL_Id = ?', (linking_id,)).fetchone()[0]
                    page_rank += 0.85 * (linking_page_rank / linking_total_links)

                new_ranks[url_id] = page_rank

            for url_id, page_rank in new_ranks.items():
                self.conn.execute('UPDATE pageRank SET score = ? WHERE urlId = ?', (page_rank, url_id))

            self.conn.commit()
        print("calculatePageRank done")

    def pagerankScore(self, rows: list[tuple[int, int, int]]) -> dict[int, float]:
        print("pagerankScore starting")
        page_ranks = []
        for row in rows:
            score = self.conn.execute('SELECT urlId, score FROM pageRank WHERE urlId = ?', (row[0], )).fetchone()
            if score is not None:
                score = score
                page_ranks.append(score)
                
        #page_ranks = [self.conn.execute('SELECT score FROM pageRank WHERE urlId = ?', (row,)).fetchone()[0] for row in rows]
        print("pagerankScore done")
        return self.normalizeScores(dict(page_ranks), smallIsBetter=0)
    
    def createMarkedHtmlFile(self, markedHTMLFilename: str, testText: list[str], testQueryList: list[str]) -> None:
        print("createMarkedHtmlFile starting")
        #Приобразование текста к нижнему регистру
        for i in range(0, len(testQueryList)):
            testQueryList[i] = testQueryList[i].lower()

        #Получить html-код с маркировкой искомых слов
        htmlCode = self.getMarkedHTML(testText, testQueryList)
        #print(htmlCode)

        #сохранить html-код в файл с указанным именем
        file = open(markedHTMLFilename, 'w', encoding="utf-8")
        file.write(htmlCode)
        file.close()
        print("createMarkedHtmlFile done")


    def getMarkedHTML(self, wordList: list, queryList: list[str]) -> str:
        print("getMarkedHTML starting")
        """Генерировть html-код с макркировкой указанных слов цветом
        wordList - список отдельных слов исходного текста
        queryList - список отдельных искомых слов,
        """

        resultHTML = "<html><body><p>"

        queryList = [word.lower() for word in queryList]

        for word in wordList:
            if word.lower() in queryList:
                # If the word is in the query list, mark it with a specific color
                resultHTML += '<span style="background-color: red;">{}</span> '.format(word)
            else:
                resultHTML += word + ' '

        resultHTML += "</p></body></html>"
        print("getMarkedHTML done")
        return resultHTML



# ------------------------------------------
def main():
    """ основная функция main() """
    mySearcher = Searcher("dbfilename.db")
    
    mySearchQuery = "of for"
    rowsLoc, wordsidList = mySearcher.getMatchRows(mySearchQuery)

    print("-----------------------")
    
    m1dict = dict(mySearcher.locationScore(rowsLoc))

    m2dict = mySearcher.pagerankScore(rowsLoc)
    m3dict = dict()
    
    for key in m1dict.keys():
        m3dict[key] = (m1dict[key] + m2dict[key]) / 2
    m3dict_sorted = dict(sorted(m3dict.items(), reverse=True, key=lambda item: item[1]))
    
    urls_3 = dict(itertools.islice(m3dict_sorted.items(), 3))
    sql = """select wordList.word
from wordList
join wordLocation on wordlist.rowId = wordlocation.fk_wordId
where wordlocation.fk_URLId = ?
ORDER BY wordlocation.location"""
    text = []
    for k, v in urls_3.items():
        words = mySearcher.conn.execute(sql, (k,)).fetchall()
        words = [item[0] for item in words]
        mySearcher.createMarkedHtmlFile(f"page{k}.html", words, mySearchQuery.split())
    
# ------------------------------------------
main()
