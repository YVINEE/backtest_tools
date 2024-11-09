# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
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
import mailtrap as mt
from datetime import datetime, timedelta

script_directory = os.path.abspath('')
if os.name == 'nt':
    node_path = r"C:\Program Files\nodejs\node.exe"
    envelope_db_path = r"D:\Git\envelope\database"

else :
    node_path = "/home/doku/.nvm/versions/node/v20.11.0/bin/node"
    envelope_db_path = "/home/doku/envelope/database"
backtest_path = os.path.join(envelope_db_path, 'week_backtesting100.db3')
config_database_path = os.path.join(envelope_db_path, 'config.db3')
pinescript_path = os.path.join(envelope_db_path, 'pinescript.txt')

try:
    with sqlite3.connect(config_database_path, 10, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as con:      
        cur = con.cursor()  
        cur.execute("PRAGMA read_uncommitted = true;");     
        
        sql = "SELECT coin FROM config ORDER BY coin"
        cur.execute(sql)
        rows = cur.fetchall()
        pair_list_in_config = []
        for row in rows:
            if row[0].endswith(".P"):
                # Si le coin se termine par ".P", on l'enlève et on ajoute ":USDT" après "/USDT"
                coin = row[0][:-2]  # Enlève les deux derniers caractères (".P")
                pair_list_in_config.append(f"{coin}/USDT:USDT")
            else:
                # Si le coin ne se termine pas par ".P", on l'ajoute tel quel avec "/USDT"
                pair_list_in_config.append(f"{row[0]}/USDT")  
        cur.close()  

    print(pair_list_in_config)
    sql_in_config = '(' + ', '.join("'"+pair+"'" for pair in pair_list_in_config) + ')'
    print(sql_in_config)

    print('>>> generate pinescript')
    pattern = "    if (pair == '{pair}')"
    pattern += "\n        result := config.new('{source_name}', {env_perc}, {coef_on_btc_rsi}, {coef_on_stoch_rsi}, {fibo_level})\n"""  

    pinescript = "GetBotConfig(string pair) =>"
    pinescript += '\n    config result = na\n'        

    with sqlite3.connect(backtest_path, 10, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as con:    
        cur = con.cursor()  
        cur.execute("PRAGMA read_uncommitted = true;");     
        
        sql = f"""SELECT pair, source_name, env_perc, coef_on_btc_rsi, coef_on_stoch_rsi, fibo_level, last_volume_usdt FROM backtesting WHERE pair IN {sql_in_config}
            UNION SELECT pair, source_name, env_perc, coef_on_btc_rsi, coef_on_stoch_rsi, fibo_level, last_volume_usdt FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY usd_per_day DESC) AS rn FROM backtesting WHERE pair NOT IN {sql_in_config} AND score IS NOT NULL) t WHERE rn <= 15"""

        print(sql)
        res = cur.execute(sql)
        rows = cur.fetchall()
        rows_list = [list(row) for row in rows]
        cur.close()


    pair_list_in_pinescript = []
    pair_list_not_enough_volume = []
    for row in rows_list:
        pair_list_in_pinescript.append(f"'{row[0]}'")
        pair = row[0].replace('/','').replace('$','').replace(':USDT','.P')
        pinescript += pattern.format(pair=pair, source_name=row[1], env_perc=row[2], coef_on_btc_rsi=row[3], coef_on_stoch_rsi=row[4], fibo_level=row[5])
        if row[6] < 1000 :
          pair_list_not_enough_volume.append(row[0] + f'({row[6]})')

    pinescript += '\n    result\n'
    #print(pinescript)

    text_file = open(pinescript_path, "w")
    text_file.write(pinescript)
    text_file.close()  
    
    if len(pair_list_not_enough_volume) > 0:    
        mail = mt.Mail(
            sender=mt.Address(email="mailtrap@demomailtrap.com", name="Bot"),
            to=[mt.Address(email="mere.doku@gmail.com")],
            subject='Volume insuffisant',
            text=', '.join(pair_list_not_enough_volume),
            category="Integration Test",
        )
        client = mt.MailtrapClient(token="d27ebfce94cfc54d8cf27a95cb073b36")
        client.send(mail)                           
                          
    print('>>> update tradingview')
    pair_list_in_config = [pair.replace('/','').replace('$','').replace(':USDT','.P') for pair in pair_list_in_config] 
    GetBotConfig = pinescript
    pattern = r"'(\w+USDT(?:\.P)?)'"
    matches = re.findall(pattern, GetBotConfig)
    print(matches)


    pair_list_to_study = sorted(list(set(matches) - set(pair_list_in_config)))
    for proc in psutil.process_iter(['name', 'cmdline']):
        try:
            if proc.info['cmdline'] != None:
                cmdline = ' '.join(map(str, proc.info['cmdline']))  # Convert each argument to string using map()
                #print(cmdline)
                if 'chrome' in cmdline :
                    proc.send_signal(signal.SIGTERM) # Terminate the process
                    print(f"Terminated process: {proc.pid}")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass    

    options = webdriver.ChromeOptions()
    options.add_argument("--password-store=basic")
    options.add_argument("--start-maximized")
    options.add_experimental_option(
        "prefs",    
        {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        },
    )    

    browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
    url = 'https://www.tradingview.com/accounts/signin/'
    browser.get(url)

    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.NAME, 'Email'))).click()
    
    WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.ID, 'id_username'))).send_keys('yvinee@protonmail.com')

    WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.ID, 'id_password'))).send_keys('Uar@ulnty86330')

    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-overflow-tooltip-text='Sign in']"))).click()

    
    time.sleep(30)
    url='https://www.tradingview.com/chart'
    browser.get(url)
    
    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-overflow-tooltip-text='Accept all ']"))).click() 

    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[aria-label='Remove objects']"))).click()

    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='remove-studies']"))).click()

    xpath = "//button[@aria-label = 'Open Pine Editor'][@data-active='false']"
    open_pine_editor_elements = browser.find_elements(By.XPATH, xpath);
    if len(open_pine_editor_elements) > 0:
        #open_pine_editor_elements[0].click()
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, xpath))).click()
        print("Open Pine Editor is found")
    else:
        print("Open Pine Editor is NOT found")

    
    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".view-lines.monaco-mouse-cursor-text"))).click()
    time.sleep(2)
    editor = browser.switch_to.active_element
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

    today = datetime.now()
    one_week_ago = today - timedelta(weeks=1)
    date_one_week_ago = one_week_ago.strftime("%Y-%m-%d")

    update_date = "update_date = '" + today.strftime('%A %d %b %X') + "'"
    new_script = re.sub('(<GetBotConfig>).*(//</GetBotConfig>)', rf'\1\n{update_date}\n{GetBotConfig}\2', script,  flags=re.DOTALL)

    # Pattern to match the line starting with "startDate = input.time(timestamp("
    start_date_pattern = r'^startDate = input\.time\(timestamp\("(\d{4}-\d{2}-\d{2})"\),'

    # Split the original string into lines
    lines = new_script.split('\n')

    # Replace the line matching the pattern
    for i, line in enumerate(lines):
        match = re.match(start_date_pattern, line)
        if match:
            old_date = match.group(1)
            print("old_date", old_date)
            new_line = re.sub(old_date, date_one_week_ago, line)
            lines[i] = new_line

    # Join the lines back into a string
    new_script = '\n'.join(lines)    

    pyperclip.copy(new_script)
    editor.send_keys(Keys.DELETE)

    time.sleep(2)
    editor.send_keys(Keys.CONTROL + 'v')

    time.sleep(2)
    editor.send_keys(Keys.CONTROL + 's')
    time.sleep(10)

    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='add-script-to-chart']"))).click()
    time.sleep(10)      

    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='watchlists-button']"))).click()

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

        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='watchlists-button']"))).click()
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
            
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='watchlists-button']"))).click()
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
    
    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[aria-label='Remove objects']"))).click()
    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='remove-studies']"))).click()    
    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.ID, "header-toolbar-save-load"))).click()
    time.sleep(30)
    browser.close()

except Exception:
    print(traceback.format_exc())
    browser.close()
    pass


