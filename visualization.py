import matplotlib.pyplot as plt


class Visualization():
    def __init__(self):
        pass
    
    def first_graph(self, lst):
        title = "график 1"
        x = list(range(1, len(lst) +1))
        y = lst
        plt.title(title) # заголовок
        plt.xlabel("Количество индексированных страниц") # ось абсцисс
        plt.ylabel("Количество уникальных слов") # ось ординат
        plt.grid()      # включение отображение сетки
        plt.plot(x, y,
                #'o-'
                )  # построение графика
        plt.show()
    
    def second_graph(self, lst):
        title = "график 2"
        x = list(range(1, len(lst) +1))
        y = lst
        plt.title(title) # заголовок
        plt.xlabel("Количество индексированных страниц") # ось абсцисс
        plt.ylabel("Количество URL") # ось ординат
        plt.grid()      # включение отображение сетки
        plt.plot(x, y,
                #'o-'
                )  # построение графика
        plt.show()
    
    def third_graph(self, lst):
        title = "график 3"
        x = list(range(1, len(lst) +1))
        y = lst
        plt.title(title) # заголовок
        plt.xlabel("Количество индексированных страниц") # ось абсцисс
        plt.ylabel("Количество слов") # ось ординат
        plt.grid()      # включение отображение сетки
        plt.plot(x, y, 
                #'o-'
                )  # построение графика
        plt.show()

