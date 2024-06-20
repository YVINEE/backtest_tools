# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import undetected_chromedriver as uc
import traceback
from time import gmtime, strftime
import datetime
import time
import pyperclip
import os
import re
import sqlite3
import psutil
import signal
script_directory = os.path.abspath('')

# %%
database_path = os.path.join('/home/doku/envelope', 'database')

config_database_path = os.path.join(database_path, 'config.db3')
with sqlite3.connect(config_database_path, 10, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as con:      
    cur = con.cursor()  
    cur.execute("PRAGMA read_uncommitted = true;");     
    
    sql = "SELECT coin FROM config ORDER BY coin"
    cur.execute(sql)
    rows = cur.fetchall()
    pair_list_in_config = [f'{row[0]}USDT' for row in rows]      
    cur.close()  

# %%
print(pair_list_in_config)

# %%
pine_script_file_name = '/home/doku/envelope/database/pinescript.txt'
f = open(pine_script_file_name)
GetBotConfig = f.read()    
f.close()

# %%
pattern = r"'(\w+USDT)'"
matches = re.findall(pattern, GetBotConfig)

pair_list_to_study = sorted(list(set(matches) - set(pair_list_in_config)))
print(pair_list_to_study)

# %%
try:
    options = uc.ChromeOptions()
    options.add_argument("--password-store=basic")
    options.add_experimental_option(
        "prefs",    
        {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        },
    )
    # options.add_argument('--no-sandbox')
    # options.add_argument('--disable-dev-shm-usage')
    # options.add_argument('--headless')      

    browser = uc.Chrome(headless=False,use_subprocess=False,options=options)
    url = 'https://www.tradingview.com/accounts/signin/'
    browser.get(url)

    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.NAME, 'Email'))).click()
    
    WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.ID, 'id_username'))).send_keys('yvinee@protonmail.com')

    WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.ID, 'id_password'))).send_keys('Uar@ulnty86330')

    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-overflow-tooltip-text='Sign in']"))).click()

    
    time.sleep(10)
    url='https://www.tradingview.com/chart'
    browser.get(url)
    
    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-overflow-tooltip-text='Accept all ']"))).click() 

    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[aria-label='Remove objects']"))).click()

    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='remove-studies']"))).click()

    xpath = "//button[@aria-label = 'Open Pine Editor'][@data-active='false']"
    open_pine_editor_elements = browser.find_elements(By.XPATH, xpath);
    if len(open_pine_editor_elements) > 0:
        open_pine_editor_elements[0].click()
        print("Open Pine Editor is found")
    else:
        print("Open Pine Editor is NOT found")

    
    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".view-lines.monaco-mouse-cursor-text"))).click()
    time.sleep(2)
    ActionChains(browser).key_down(Keys.CONTROL).send_keys('o').key_up(Keys.CONTROL).perform()
    time.sleep(2)
    ActionChains(browser).send_keys(Keys.DOWN).perform()
    time.sleep(2)
    ActionChains(browser).send_keys(Keys.RETURN).perform()
    time.sleep(4)

    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".view-lines.monaco-mouse-cursor-text"))).click()

    editor = browser.switch_to.active_element

    time.sleep(2)
    editor.send_keys(Keys.CONTROL + 'a')
    time.sleep(2)
    editor.send_keys(Keys.CONTROL + 'c')    
    script = pyperclip.paste()

    update_date = "//" + datetime.datetime.now().strftime('%A %d %B %X')
    new_script = re.sub('(<GetBotConfig>).*(//</GetBotConfig>)', rf'\1\n{update_date}\n{GetBotConfig}\2', script,  flags=re.DOTALL)

    pyperclip.copy(new_script)
    editor.send_keys(Keys.DELETE)

    time.sleep(2)
    editor.send_keys(Keys.CONTROL + 'v')

    time.sleep(2)
    editor.send_keys(Keys.CONTROL + 's')
    time.sleep(2)

    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='add-script-to-chart']"))).click()
    time.sleep(10)  

    
    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='settings-button']"))).click()

    time.sleep(4)
    clear_list_elements = browser.find_elements(By.XPATH, "//*[contains(text(), 'Clear list')]")
    if len(clear_list_elements) > 0:
        clear_list_elements[0].click()
        print("Clear List is found")
        time.sleep(2)
        ActionChains(browser).send_keys(Keys.RETURN).perform()

        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='add-symbol-button']"))).click()
        time.sleep(2)
        ActionChains(browser).send_keys("BITGET:BTCUSDT").perform()
        time.sleep(2)
        ActionChains(browser).send_keys(Keys.RETURN).perform()        
        time.sleep(2)
        ActionChains(browser).send_keys(Keys.ESCAPE).perform()
        time.sleep(2)

        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='settings-button']"))).click()
        add_section_elements = browser.find_elements(By.XPATH, "//*[contains(text(), 'Add section')]")
        time.sleep(2)
        if len(add_section_elements) > 0:
            add_section_elements[0].click()
        
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='add-symbol-button']"))).click()
        time.sleep(2)        

        for pair in pair_list_in_config:
            print(pair)
            ActionChains(browser).send_keys(f"BITGET:{pair}").perform()
            time.sleep(1)
            ActionChains(browser).send_keys(Keys.RETURN).perform()        
            time.sleep(1)  

        pair = pair_list_to_study[0]
        ActionChains(browser).send_keys(f"BITGET:{pair}").perform()
        time.sleep(1)
        ActionChains(browser).send_keys(Keys.RETURN).perform()        
        time.sleep(1)          

        time.sleep(2)
        ActionChains(browser).send_keys(Keys.ESCAPE).perform()
        time.sleep(10)
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, f"[data-symbol-full='BITGET:{pair}']"))).click()
            
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='settings-button']"))).click()
        time.sleep(2)
        add_section_elements = browser.find_elements(By.XPATH, "//*[contains(text(), 'Add section')]")
        if len(add_section_elements) > 0:
            add_section_elements[0].click()

        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='add-symbol-button']"))).click()
        time.sleep(2)               
        
        for pair in pair_list_to_study[1:]:
            print(pair)
            ActionChains(browser).send_keys(f"BITGET:{pair}").perform()
            time.sleep(1)
            ActionChains(browser).send_keys(Keys.RETURN).perform()        
            time.sleep(1)        

        ActionChains(browser).send_keys(Keys.ESCAPE).perform()       

        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='backtesting']"))).click() 

    else:
        print("Clear List is NOT found")

    #browser.close()

except Exception:
    print(traceback.format_exc())
    #browser.close()
    pass


