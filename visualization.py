import matplotlib.pyplot as plt


class Visualization():
    def __init__(self):
        pass
    
    def WL_graph(self, dct):
        title = "Зависимость добавление уникальных слов от кол-ва ссылок"
        x = list(dct.keys())
        x = [i for i in range(1, len(x)+1)]
        y = list(dct.values())
        x.insert(0, 0)
        y.insert(0, 0)
        plt.title(title) # заголовок
        plt.xlabel("Количество ссылок") # ось абсцисс
        plt.ylabel("Количество уникальных слов") # ось ординат
        plt.grid()      # включение отображение сетки
        plt.plot(x, y)  # построение графика
        plt.show()
    
    def LBU_graph(self, dct):
        title = "Содержание ссылок на другие сайты"
        x = list(dct.keys())
        y = list(dct.values())
        x = [i for i in range(1, len(x)+1)]
        plt.figure()
        plt.title(title)
        plt.bar(x, y)
        plt.xlabel("Id ссылки")
        plt.ylabel("Количество ссылок")
        plt.show()
    
    def UL_graph(self, dct):
        title = "UrlList"
        x_temp = list(dct.keys())
        y_temp = list(dct.values())
        x, y = [], []
        for index, item in enumerate(y_temp, start=1):
            x.append(index)
            y.append(sum(y_temp[:index]))
        
        x.insert(0, 0)
        y.insert(0, 0)
        plt.title(title) # заголовок
        plt.xlabel("Количество индексированных страниц") # ось абсцисс
        plt.ylabel("Количество URL") # ось ординат
        plt.grid()      # включение отображение сетки
        plt.plot(x, y)  # построение графика
        plt.show()

