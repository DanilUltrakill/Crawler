class Crawler:

    # 0. Конструктор Инициализация паука с параметрами БД
    def __init__(self, dbFileName):
        print("Конструктор")
        pass

    # 0. Деструктор
    def __del__(self):
        print("Деструктор")
        pass

    # 1. Индексирование одной страницы
    def addIndex(self, soup, url):
        pass

    # 2. Получение текста страницы
    def getTextOnly(self, text):
        return ""

    # 3. Разбиение текста на слова
    def separateWords(self, text):
        return ""

    # 4. Проиндексирован ли URL (проверка наличия URL в БД)
    def isIndexed(self, url):
        return False
 
    # 5. Добавление ссылки с одной страницы на другую
    def addLinkRef(self, urlFrom, urlTo, linkText):
        pass

    # 6. Непосредственно сам метод сбора данных.
    # Начиная с заданного списка страниц, выполняет поиск в ширину
    # до заданной глубины, индексируя все встречающиеся по пути страницы
    def crawl(self, urlList, maxDepth=1):
        pass
 
    # 7. Инициализация таблиц в БД
    def initDB(self):
        print("Создать пустые таблицы с необходимой структурой")
