import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class export_ans:

    def __int__(self):
       print("inicia execução do modulo!")

    def download_arquivo(self,  url):
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
            self.driver.get(url)
            time.sleep(60)

    def fecha_navegador(self):
            self.driver.close()
            self.driver.quit()