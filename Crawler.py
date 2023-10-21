import re
import sqlite3
import requests
from bs4 import BeautifulSoup

class Crawler:
    def __init__(self, dbFileName):
        self.conn = sqlite3.connect(dbFileName)
        self.conn.text_factory = lambda x: str(x, 'utf-8', 'ignore')
        self.cursor = self.conn.cursor()
        self.initDB()


    def __del__(self):
        self.conn.close()


    def addIndex(self, soup, url):
        # Индексирование одной страницы
        if (self.isIndexed(url)):
            return
    
        only_text = self.getTextOnly(soup)
        separated_words = self.separateWords(only_text)
        print(separated_words)
        url_id = self.getEntryId("URLList", "url", url)

        cur_index = 0
        for word in separated_words:
            word_id = self.getEntryId("wordList", "word", word)
            self.cursor.execute("INSERT INTO wordLocation (fk_wordId, fk_URLId, location) VALUES (?, ?, ?)", (word_id, url_id, cur_index,))
            cur_index += 1
        self.conn.commit()


    def getTextOnly(self, soup):
        # Получение текста страницы
        # Здесь вы можете добавить логику для извлечения только текста
        if soup is not None:
            final_text = soup.get_text()
            try:
                images = soup.find_all('img')
                for image in images:
                    try:
                        attr_alt = image.get("alt")
                        if attr_alt != " " and attr_alt is not None:                            
                            print(f"Аттрибут alt: {attr_alt}")
                            final_text += attr_alt
                    except Exception as e:
                        continue
            except Exception as e:
                pass
            return final_text
        return ""


    def separateWords(self, text):
        # Разбиение текста на слова
        return re.findall(r'[a-zA-Zа-яА-Я]+', text)


    def isIndexed(self, url):
        # Проверка наличия URL в БД
        self.cursor.execute("SELECT id FROM URLList WHERE url=?", (url,))
        result = self.cursor.fetchone()
        if result == None:
            return False
        else:
            self.cursor.execute("SELECT id FROM wordLocation WHERE fk_URLId=?", (result[0],))
            result = self.cursor.fetchone()
            if result is None:
                return False
            else:
                return True


    def addLinkRef(self, urlFrom, urlTo, linktext):
        urlFromId = self.getEntryId("URLList", "url", urlFrom)
        urlToId = self.getEntryId("URLList", "url", urlTo)
        self.cursor.execute("INSERT INTO linkBetweenURL (fk_FromURL_Id, fk_ToURL_Id) VALUES (?, ?)", (urlFromId, urlToId))
        linkId = self.cursor.lastrowid  # Получаем идентификатор добавленной ссылки

        if linktext:
            separated_words = self.separateWords(linktext)
            for word in separated_words:
                wordId = self.getEntryId("wordList", "word", word)
                self.cursor.execute("INSERT INTO linkWord (fk_wordId, fk_linkId) VALUES (?, ?)", (wordId, linkId))

        self.conn.commit()


    def crawl(self, urlList, maxDepth=1):
        # Метод сбора данных
        # Начиная с заданного списка страниц, выполняет поиск в ширину
        # до заданной глубины, индексируя все встречающиеся по пути страницы
        pages_to_crawl = urlList.copy()
        visited_pages = set()

        for _ in range(maxDepth):
            new_pages = []
            for page_url in pages_to_crawl:
                if page_url not in visited_pages and len(visited_pages) < 100:
                    try:
                        response = requests.get(page_url)
                        response.raise_for_status()
                        response.encoding = 'utf-8'
                        soup = BeautifulSoup(response.text, "html.parser")

                        visited_pages.add(page_url)
                        
                        # Здесь добавьте код для извлечения ссылок со страницы и добавления их в new_pages
                        # Вы можете использовать soup.find_all('a') и обрабатывать найденные ссылки
                        
                        self.addIndex(soup, page_url)
                        print(f"Проиндексирована: {page_url}")
                        try:
                            links = soup.find_all('a')
                            for link in links:
                                try:
                                    attr_href = link.get("href")
                                    if attr_href != " " and attr_href != "/" and attr_href != "" and attr_href != "./":
                                        new_url = attr_href
                                        if new_url.find("http") == -1:    
                                            new_url = f"https://aftershock.news{attr_href}"   

                                            if new_url != "https://aftershock.news/?q=":
                                                
                                                new_pages.append(new_url)

                                                link_text = link.text
                                                self.addLinkRef(page_url, new_url, link_text)
                                        elif new_url.startswith('/') == False:
                                            new_pages.append(new_url)

                                            link_text = link.text
                                            self.addLinkRef(page_url, new_url, link_text)
                                        print(f"Найдена: {new_url}")
                                except Exception as e:
                                    continue
                        except Exception as e:
                            print(f"Ошибка поиска тэгов <a> {page_url}: {e}")
                        
                    except Exception as e:
                        print(f"Ошибка индексации страницы {page_url}: {e}")
 
            pages_to_crawl = new_pages
            


    def initDB(self):
        self.cursor.execute('create table if not exists wordList (id INTEGER PRIMARY KEY,\
                                      word text,\
                                      isFiltred integer default 0);')
                
        self.cursor.execute('create table if not exists URLList (id INTEGER PRIMARY KEY,\
					                 url text);')
        
        self.cursor.execute('create table if not exists wordLocation (id INTEGER PRIMARY KEY,\
                                          fk_wordId integer REFERENCES wordList(id),\
                                          fk_URLId integer REFERENCES URLList(id),\
                                          location integer);')
        
        self.cursor.execute('create table if not exists linkBetweenURL (id INTEGER PRIMARY KEY,\
					                        fk_FromURL_Id integer REFERENCES URLList(id),\
						                    fk_ToURL_Id integer REFERENCES URLList(id));')
        
        self.cursor.execute('create table if not exists linkWord (id INTEGER PRIMARY KEY,\
                                      fk_wordId integer REFERENCES wordList(id),\
                                      fk_linkId integer REFERENCES linkBetweenURL(id));')
        
        self.conn.commit()
        

    def getEntryId(self, tableName, fieldName, value):
        # Вспомогательная функция для получения идентификатора и добавления записи, если такой еще нет
        try:
            self.cursor.execute(f"SELECT id FROM {tableName} WHERE {fieldName} = ?", (value,))
            result = self.cursor.fetchone()
            if result is None:
                self.cursor.execute(f"INSERT INTO {tableName} ({fieldName}) VALUES (?)", (value,))
                self.conn.commit()
                return self.getEntryId(tableName, fieldName, value,)
            return result[0]
        except Exception as e:
            print(f"Ошибка поиска или создания записи в бд: {e}")


    def all_rows(self):
        # Получаем список всех таблиц в базе данных
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()

        # Создаем словарь для хранения количества записей в каждой таблице
        table_record_counts = {}

        # Перебираем каждую таблицу и получаем количество записей
        for table in tables:
            table_name = table[0]
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = self.cursor.fetchone()[0]
            table_record_counts[table_name] = count

        for table, count in table_record_counts.items():
            print(f"Таблица '{table}' содержит {count} записей.")

