import sqlite3
import requests
from bs4 import BeautifulSoup

class Crawler:
    def __init__(self, dbFileName):
        self.conn = sqlite3.connect(dbFileName)
        self.cursor = self.conn.cursor()
        self.initDB()


    def __del__(self):
        self.conn.close()


    def addIndex(self, soup, url):
        # Индексирование одной страницы
        page_content = soup.get_text()
        self.cursor.execute("INSERT INTO URLList (url) VALUES (?)", (url))
        self.cursor.execute("INSERT INTO wordList (word, isFiltred) VALUES (?, &)", (page_content))
        self.conn.commit()


    def getTextOnly(self, text):
        # Получение текста страницы
        # Здесь вы можете добавить логику для извлечения только текста
        return text


    def separateWords(self, text):
        # Разбиение текста на слова
        # Здесь вы можете добавить логику для разделения текста на слова
        return text.split()


    def isIndexed(self, url):
        # Проверка наличия URL в БД
        self.cursor.execute("SELECT id FROM URLList WHERE url=?", (url,))
        result = self.cursor.fetchone()
        return result is not None


    def addLinkRef(self, urlFrom, urlTo, linkText):
        # Добавление ссылки с одной страницы на другую
        # Здесь вы можете добавить логику для добавления ссылки в базу данных
        pass


    def crawl(self, urlList, maxDepth=1):
        # Метод сбора данных
        # Начиная с заданного списка страниц, выполняет поиск в ширину
        # до заданной глубины, индексируя все встречающиеся по пути страницы
        pages_to_crawl = urlList.copy()
        visited_pages = set()

        for _ in range(maxDepth):
            new_pages = []
            for page_url in pages_to_crawl:
                if page_url not in visited_pages:
                    try:
                        response = requests.get(page_url)
                        response.raise_for_status()
                        soup = BeautifulSoup(response.text, "html.parser")
                        print(soup.get_text().encode('utf-8'))
                        visited_pages.add(page_url)

                        # Здесь добавьте код для извлечения ссылок со страницы и добавления их в new_pages
                        # Вы можете использовать soup.find_all('a') и обрабатывать найденные ссылки

                        self.addIndex(soup, page_url)

                    except Exception as e:
                        print(f"Ошибка индексации страницы {page_url}: {e}")

            pages_to_crawl = new_pages


    def initDB(self):
        self.cursor.execute('create table wordList (rowId serial PRIMARY KEY,\
                                      word text,\
                                      isFiltred integer);')
        
        
        self.cursor.execute('create table URLList (rowId serial PRIMARY KEY,\
					                 url text);')
        
        self.cursor.execute('create table wordLocation (rowId serial PRIMARY KEY,\
                                          fk_wordId integer REFERENCES wordList(rowId),\
                                          fk_URLId integer REFERENCES URLList(rowId),\
                                          wordLocation integer);')
        
        self.cursor.execute('create table linkBetweenURL (rowId serial PRIMARY KEY,\
					                        fk_FromURL_Id integer REFERENCES URLList(rowId),\
						                    fk_ToURL_Id integer REFERENCES URLList(rowId));')
        
        self.cursor.execute('create table linkWord (rowId serial PRIMARY KEY,\
                                      fk_wordId integer REFERENCES wordList(rowId),\
                                      fk_linkId integer REFERENCES linkBetweenURL(rowId));')
        
        self.conn.commit()
        

    def getEntryId(self, tableName, fieldName, value):
        # Вспомогательная функция для получения идентификатора и добавления записи, если такой еще нет
        self.cursor.execute(f"SELECT id FROM {tableName} WHERE {fieldName}=?", (value,))
        result = self.cursor.fetchone()
        if result is None:
            self.cursor.execute(f"INSERT INTO {tableName} ({fieldName}) VALUES (?)", (value,))
            self.conn.commit()
            return self.getEntryId(tableName, fieldName, value)
        return result[0]


