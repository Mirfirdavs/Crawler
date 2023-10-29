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

        # Разделить на отдельные искомые слова
        queryWordsList = queryString.split()

        # список для хранения результата
        rowidList = list()

        # Для каждого искомого слова
        for word in queryWordsList:
            # Сформировать sql-запрос для получения rowid слова, указано ограничение на кол-во возвращаемых результатов (LIMIT 1)
            sql = "SELECT rowid FROM wordlist WHERE word =\"{}\" LIMIT 1; ".format(word)

            # Выполнить sql-запрос. В качестве результата ожидаем строки содержащие целочисленный идентификатор rowid
            result_row = self.conn.execute(sql ).fetchone()


            # Если слово было найдено и rowid получен
            if result_row != None:
                # Искомое rowid является элементом строки ответа от БД (особенность получаемого результата)
                word_rowid = result_row[0]

                # поместить rowid в список результата
                rowidList.append(word_rowid)
                print("  ", word, word_rowid)
            else:
                # в случае, если слово не найдено приостановить работу (генерация исключения)
                raise Exception("Одно из слов поискового запроса не найдено:" + word)

        # вернуть список идентификаторов
        return rowidList

    def getMatchRows(self, queryString):
        """
        Поиск комбинаций из всезх искомых слов в проиндексированных url-адресах
        :param queryString: поисковый запрос пользователя
        :return: 1) список вхождений формата (urlId, loc_q1, loc_q2, ...) loc_qN позиция на странице Nго слова из поискового запроса  "q1 q2 ..."
        """

        # Разбить поисковый запрос на слова по пробелам
        queryString = queryString.lower()
        wordsList = queryString.split(' ')

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
        for wordIndex in range(0,len(wordsList)):
        
            # Получить идентификатор слова
            wordID = wordsidList[wordIndex]

            if wordIndex ==0:
                # обязательная часть для первого слова
                sqlpart_Name.append("""w0.urlid    urlid  --идентификатор url-адреса""")
                sqlpart_Name.append("""   , w0.location w0_loc --положение первого искомого слова""")

                sqlpart_Condition.append("""WHERE w0.wordid={}     -- совпадение w0 с первым словом """.format(wordID))

            else:
                # Дополнительная часть для 2,3,.. искомых слов

                if len(wordsList)>=2:
                    # Проверка, если текущее слово - второе и более

                    # Добавить в имена столбцов
                    sqlpart_Name.append(""" , w{}.location w{}_loc --положение следующего искомого слова""".format(wordIndex,wordIndex))

                    #Добавить в sql INNER JOIN
                    sqlpart_Join.append("""INNER JOIN wordlocation w{}  -- назначим псевдоним w{} для второй из соединяемых таблиц
                        on w0.urlid=w{}.urlid -- условие объединения""".format(wordIndex, wordIndex, wordIndex))
                    # Добавить в sql ограничивающее условие
                    sqlpart_Condition.append("""  AND w{}.wordid={} -- совпадение w{}... с cоответсвующим словом """.format(wordIndex, wordID, wordIndex ))
                    pass
            pass
        
        
        # Объеднение запроса из отдельных частей

        #Команда SELECT
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

# ------------------------------------------
def main():
    """ основная функция main() """
    mySeacher = Seacher("dbfilename.db")
    
    mySearchQuery = "на мы"
    rowsLoc, wordsidList = \
        mySeacher.getMatchRows(mySearchQuery)

    print("-----------------------")
    print (mySearchQuery)
    print (wordsidList)
    for location in rowsLoc:
        print(location)


# ------------------------------------------
main()
