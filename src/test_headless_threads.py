from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions

def open_browser():
    options = FirefoxOptions()
    options.add_argument('--headless')
    driver = webdriver.Firefox(options=options)
    driver.get('https://www.google.com')
    print(driver.title)
    driver.quit()

open_browser()
