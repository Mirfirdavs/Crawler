import sqlite3

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


    def getWordsIds(self, queryString):
        """
        Получение идентификаторов для каждого слова в queryString
        :param queryString: поисковый запрос пользователя
        :return: список wordlist.rowid искомых слов
        """

        queryWordsList = queryString.split()
        rowidList = list()

        # Для каждого искомого слова
        for word in queryWordsList:
            sql = "SELECT rowid FROM wordlist WHERE word =\"{}\" LIMIT 1; ".format(word)
            result_row = self.conn.execute(sql ).fetchone()
            
            if result_row != None:
                word_rowid = result_row[0]
                
                rowidList.append(word_rowid)
                print("  ", word, word_rowid)
            else:
                raise Exception("Одно из слов поискового запроса не найдено:" + word)
        # вернуть список идентификаторов
        return rowidList


    def getMatchRows(self, queryString):
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
                    sqlpart_Join.append("""INNER JOIN wordlocation w{}  -- назначим псевдоним w{} для второй из соединяемых таблиц
                        on w0.fk_URLId=w{}.fk_URLId -- условие объединения""".format(wordIndex, wordIndex, wordIndex))
                    # Добавить в sql ограничивающее условие
                    sqlpart_Condition.append("""  AND w{}.fk_wordId={} -- совпадение w{}... с cоответсвующим словом """.format(wordIndex, wordID, wordIndex ))
                    pass
            pass

        # Объеднение запроса из отдельных частей
        sqlFullQuery += "SELECT "

        #Все имена столбцов для вывода
        for sqlpart in sqlpart_Name:
            sqlFullQuery+="\n"
            sqlFullQuery += sqlpart

        # обязательная часть таблица-источник
        sqlFullQuery += "\n"
        sqlFullQuery += "FROM wordlocation w0 "

        #часть для объединения таблицы INNER JOIN
        for sqlpart in sqlpart_Join:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        #обязательная часть и дополнения для блока WHERE
        for sqlpart in sqlpart_Condition:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        # Выполнить SQL-запроса и извлеч ответ от БД
        print(sqlFullQuery)
        cur = self.conn.execute(sqlFullQuery)
        rows = [row for row in cur]
        return rows, wordsidList


    def normalizeScores(self, scores, smallIsBetter=0):
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

        return resultDict

    # Ранжирование. Содержимое. 2. Расположение в документе.
    def locationScore(self, rowsLoc):
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
        
        return self.normalizeScores(locationsDict, smallIsBetter=0)

    def geturlname(self, id):
        """
        Получает из БД текстовое поле url-адреса по указанному urlid
        :param id: целочисленный urlid
        :return: строка с соответствующим url
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT url FROM urlList WHERE rowid=?", (id, ))
        url = cursor.fetchone()
        
        if url is not None:
            return url[0]
        return None

    def getSortedList(self, queryString):
        """
        На поисковый запрос формирует список URL, вычисляет ранги, выводит в отсортированном порядке
        :param queryString:  поисковый запрос
        :return:
        """
        rowsLoc, wordids = self.getMatchRows(queryString=queryString)
        # получить rowsLoc и wordids от getMatchRows(queryString)
        # rowsLoc - Список вхождений: urlId, loc_q1, loc_q2, .. слов из поискового запроса "q1 q2 ..."
        # wordids - Список wordids.rowid слов поискового запроса

        m1Scores = self.locationScore(rowsLoc)
        # Получить m1Scores - словарь {id URL страниц где встретились искомые слова: вычисленный нормализованный РАНГ}
        # как результат вычисления одной из метрик

        #Создать список для последующей сортировки рангов и url-адресов
        rankedScoresList = list()
        for url, score in m1Scores.items():
            pair = (score, url)
            rankedScoresList.append( pair )

        # Сортировка из словаря по убыванию
        rankedScoresList.sort(reverse=True)

        # Вывод первых N Результатов
        print("score, urlid, geturlname")
        for (score, urlid) in rankedScoresList[0:10]:
            print ( "{:.2f} {:>5}  {}".format ( score, urlid, self.geturlname(urlid)))

    def calculatePageRank(self, iterations=5):
        # Подготовка БД ------------------------------------------
        # стираем текущее содержимое таблицы PageRank
        self.con.execute('DROP TABLE IF EXISTS pagerank')
        self.con.execute("""CREATE TABLE  IF NOT EXISTS  pagerank(
                                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                urlid INTEGER,
                                score REAL
                            );""")


        # Для некоторых столбцов в таблицах БД укажем команду создания объекта "INDEX" для ускорения поиска в БД
        self.con.execute("DROP INDEX   IF EXISTS wordidx;")
        self.con.execute("DROP INDEX   IF EXISTS urlidx;")
        self.con.execute("DROP INDEX   IF EXISTS wordurlidx;")
        self.con.execute("DROP INDEX   IF EXISTS urltoidx;")
        self.con.execute("DROP INDEX   IF EXISTS urlfromidx;")
        self.con.execute('CREATE INDEX IF NOT EXISTS wordidx       ON wordlist(word)')
        self.con.execute('CREATE INDEX IF NOT EXISTS urlidx        ON urllist(url)')
        self.con.execute('CREATE INDEX IF NOT EXISTS wordurlidx    ON wordlocation(wordid)')
        self.con.execute('CREATE INDEX IF NOT EXISTS urltoidx      ON linkbeetwenurl(toid)')
        self.con.execute('CREATE INDEX IF NOT EXISTS urlfromidx    ON linkbeetwenurl(fromid)')
        self.con.execute("DROP INDEX   IF EXISTS rankurlididx;")
        self.con.execute('CREATE INDEX IF NOT EXISTS rankurlididx  ON pagerank(urlid)')
        self.con.execute("REINDEX wordidx;")
        self.con.execute("REINDEX urlidx;")
        self.con.execute("REINDEX wordurlidx;")
        self.con.execute("REINDEX urltoidx;")
        self.con.execute("REINDEX urlfromidx;")
        self.con.execute("REINDEX rankurlididx;")



        # в начальный момент ранг для каждого URL равен 1
        self.con.execute('INSERT INTO pagerank (urlid, score) SELECT rowid, 1.0 FROM urllist')
        self.dbcommit()


        # Цикл Вычисление PageRank в несколько итераций  
        for i in range(iterations):
            print("Итерация %d" % (i))


            # Цикл для обхода каждого  urlid адреса в urllist БД
            
                #назначить коэфф pr = 0.15
                

                # В цикле обходим все страницы, ссылающиеся на данную urlid
                # SELECT DISTINCT fromid FROM linkbeetwenurl  -- DISTINCT выбрать уникальные значения fromid
                

                    # Находим ранг ссылающейся страницы linkingpr. выполнить SQL-зарпрос
                    

                    # Находим общее число ссылок на ссылающейся странице linkingcount. выполнить SQL-зарпрос
        #SELECT count (*) 
        #FROM linkbeetwenurl
        #WHERE  fromid = 502
#
                    
                    
                    # Придавить к pr вычесленный результат для текущего узла
                    
                # выполнить SQL-зарпрос для обновления значения  score в таблице pagerank БД
        self.con.execute('UPDATE pagerank SET score=%f WHERE urlid=%d' % (pr, urlid))

        self.dbcommit()

    # Вывод результата pagerank
    def pagerankScore(self, rows):
        return None
        # получить значения pagerank
        # нормализовать отностительно максимума
        return normalizedscores

# ------------------------------------------
def main():
    """ основная функция main() """
    mySearcher = Searcher("dbfilename.db")
    
    mySearchQuery = "of for"
    rowsLoc, wordsidList = mySearcher.getMatchRows(mySearchQuery)

    print("-----------------------")
    #print (mySearchQuery)
    #print (wordsidList)
    #for location in rowsLoc:
    #    print(location)
    
    #print(mySearcher.locationScore(rowsLoc=rowsLoc))
    print(mySearcher.getSortedList(mySearchQuery))


# ------------------------------------------
main()
