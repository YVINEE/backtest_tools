from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementClickInterceptedException
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
from datetime import datetime, timedelta, timezone
import socket
import requests

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

def check_chrome_debugging_session():
    # Vérifier si le port 9222 est ouvert
    def is_port_open(port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', port))
        sock.close()
        return result == 0

    # Vérifier si un processus Chrome avec l'option de débogage est en cours d'exécution
    def chrome_process_running():
        for proc in psutil.process_iter(['name', 'cmdline']):
            if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                if proc.info['cmdline'] and '--remote-debugging-port=9222' in proc.info['cmdline']:
                    return True
        return False

    # Tenter de se connecter à l'API de débogage
    def test_debugging_connection():
        try:
            response = requests.get('http://localhost:9222/json/version', timeout=2)
            return response.status_code == 200
        except requests.RequestException:
            return False

    port_open = is_port_open(9222)
    process_running = chrome_process_running()
    connection_successful = test_debugging_connection()

    print(f"Port 9222 ouvert : {port_open}")
    print(f"Processus Chrome avec option de débogage en cours : {process_running}")
    print(f"Connexion à l'API de débogage réussie : {connection_successful}")

    return port_open and process_running and connection_successful

try:
    with sqlite3.connect(config_database_path, 10, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as con:      
        cur = con.cursor()  
        cur.execute("PRAGMA read_uncommitted = true;");     
        
        sql = "SELECT coin, sma_source, envelope_percent, coef_on_btc_rsi, coef_on_stoch_rsi, fibo_level FROM config ORDER BY coin"
        cur.execute(sql)
        rows = cur.fetchall()
        #pair_list_in_config = []
        dico_pair_in_config = {}

        for row in rows:
            pair = f"{row[0]}/USDT"
            if row[0].endswith(".P"):
                # Si le coin se termine par ".P", on l'enlève et on ajoute ":USDT" après "/USDT"
                coin = row[0][:-2]  # Enlève les deux derniers caractères (".P")
                pair = f"{coin}/USDT:USDT"
            dico_pair_in_config[pair] = [row[1], row[2], row[3], row[4], row[5]]  
        cur.close()  

    #print(dico_pair_in_config.keys())
    sql_in_config = '(' + ', '.join("'"+pair+"'" for pair in dico_pair_in_config.keys()) + ')'
    #print(sql_in_config)

    print('>>> generate pinescript')
    #example: 
    # src_0X0 = input.source(hl2, title = 'Source', inline = 'line1', group = '0X0USDT')
    # envelope_pct_0X0 = input.float(0.023, 'Env', step = 0.001, inline = 'line1', group = '0X0USDT')
    # fibo_level_0X0 = input.float(0, title = 'Fibo', options = [0, 0.236, 0.382, 0.5, 0.618, 0.7, 1], inline = 'line2', group = '0X0USDT')
    # coef_on_btc_rsi_0X0 = input.float(1.6, 'BTC RSI', step = 0.1, inline = 'line2', group = '0X0USDT')
    # coef_on_stoch_rsi_0X0 = input.float(1.3, 'Stoch RSI', step = 0.1, inline = 'line2', group = '0X0USDT')    
    inputs_pattern = "src_{pair} = input.source({source_name}, title = 'Source', inline = 'line1', group = '{pair}', display = display.none)\n"
    inputs_pattern += "envelope_pct_{pair} = input.float({env_perc}, 'Env', step = 0.001, inline = 'line1', group = '{pair}', display = display.none)\n"
    inputs_pattern += "fibo_level_{pair} = input.float({fibo_level}, title = 'Fibo', options = [0, 0.236, 0.382, 0.5, 0.618, 0.7, 1], inline = 'line2', group = '{pair}', display = display.none)\n"
    inputs_pattern += "coef_on_btc_rsi_{pair} = input.float({coef_on_btc_rsi}, 'BTC RSI', step = 0.1, inline = 'line2', group = '{pair}', display = display.none)\n"
    inputs_pattern += "coef_on_stoch_rsi_{pair} = input.float({coef_on_stoch_rsi}, 'Stoch RSI', step = 0.1, inline = 'line2', group = '{pair}', display = display.none)\n\n"

    #example: '0X0USDT' => config.new(GetSourceName(src_0X0), envelope_pct_0X0,  fibo_level_0X0, coef_on_btc_rsi_0X0, coef_on_stoch_rsi_0X0)
    config_pattern = "        '{pair}' => config.new(GetSourceName(src_{pair}), envelope_pct_{pair},  fibo_level_{pair}, coef_on_btc_rsi_{pair}, coef_on_stoch_rsi_{pair})\n"    
     
    with sqlite3.connect(backtest_path, 10, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as con:    
        cur = con.cursor()  
        cur.execute("PRAGMA read_uncommitted = true;");     
        
        sql = f"""SELECT pair, source_name, env_perc, coef_on_btc_rsi, coef_on_stoch_rsi, fibo_level, last_volume_usdt FROM backtesting WHERE pair IN {sql_in_config}
            UNION SELECT pair, source_name, env_perc, coef_on_btc_rsi, coef_on_stoch_rsi, fibo_level, last_volume_usdt FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY usd_per_day DESC) AS rn FROM backtesting WHERE pair NOT IN {sql_in_config} AND score IS NOT NULL) t WHERE rn <= 15"""

        #print(sql)
        res = cur.execute(sql)
        rows = cur.fetchall()        
        rows_list = []
        
        #pour récuperer les paramètres de config.db3 plutot que celui de backtest car j'ai pu les changer manuellment
        for row in rows:
            pair = row[0]
            list_row = list(row)
            if pair in dico_pair_in_config:
                list_row[1]  = dico_pair_in_config[pair][0]
                list_row[2]  = dico_pair_in_config[pair][1]
                list_row[3]  = dico_pair_in_config[pair][2]
                list_row[4]  = dico_pair_in_config[pair][3]
                list_row[5]  = dico_pair_in_config[pair][4]
            rows_list.append(list_row)            
        cur.close()

    pair_list_in_pinescript = []
    pair_list_not_enough_volume = []
    inputs_pinescript = ""
    configs_pinescript = ""
    
    for row in rows_list:
        pair_list_in_pinescript.append(f"'{row[0]}'")
        pair = row[0].replace('/','').replace('$','').replace(':USDT','_P')
        inputs_pinescript += inputs_pattern.format(pair=pair, source_name=row[1], env_perc=row[2], coef_on_btc_rsi=row[3], coef_on_stoch_rsi=row[4], fibo_level=row[5])
        configs_pinescript += config_pattern.format(pair=pair)
        if row[6] < 1000 :
          pair_list_not_enough_volume.append(row[0] + f'({row[6]})')        

    #inputs pour les pairs inconnus
    # 5 hl2 0.023 0.382 1.6 1.3
    inputs_pinescript += inputs_pattern.format(pair='unknown', source_name='hl2', env_perc=0.023, coef_on_btc_rsi=1.6, coef_on_stoch_rsi=1.3, fibo_level=0.382)

    #config pour les pairs inconnus
    configs_pinescript += '        => config.new(GetSourceName(src_unknown), envelope_pct_unknown,  fibo_level_unknown, coef_on_btc_rsi_unknown, coef_on_stoch_rsi_unknown)\n'    

    pinescript = inputs_pinescript
    pinescript += "GetBotConfig(string pair) =>"
    pinescript += '\n    switch pair\n'
    pinescript += configs_pinescript
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
    pair_list_in_config = [pair.replace('/','').replace('$','').replace(':USDT','.P') for pair in dico_pair_in_config.keys()] 
    #print("pair_list_in_config",pair_list_in_config)
    GetBotConfig = pinescript
    config_pattern = r"'(\w+USDT(?:\_P)?)'"
    matches = re.findall(config_pattern, GetBotConfig)
    #print("matches", matches)


    pair_list_to_study = sorted(list(set(matches) - set(pair_list_in_config)))
    pair_list_to_study = [pair.replace('USDT_P', 'USDT.P') for pair in pair_list_to_study]
    #print("pair_list_to_study", pair_list_to_study)

    chrome_already_open = False
    options = webdriver.ChromeOptions()
    if check_chrome_debugging_session():
        print("Une session Chrome avec le port de débogage 9222 est active.")
        #on utilise la session chrome déjà ouverte        
        options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
        chrome_already_open = True
    else:
        print("Aucune session Chrome avec le port de débogage 9222 n'a été détectée.")
        #on kill toutes les sessions Chrome
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

    if chrome_already_open == False:
        #pas de session chrome déjà ouverte on se connecte à TradingView
        url = 'https://www.tradingview.com/accounts/signin/'
        browser.get(url)
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.NAME, 'Email'))).click()        
        WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.ID, 'id_username'))).send_keys('yvinee@protonmail.com')
        WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.ID, 'id_password'))).send_keys('Uar@ulnty86330')
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-overflow-tooltip-text='Sign in']"))).click()        
        time.sleep(30)

    url='https://www.tradingview.com/chart'
    browser.get(url)

    try:
        accept_all = WebDriverWait(browser, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-overflow-tooltip-text='Accept all ']"))
        )
        accept_all.click()
    except TimeoutException:
        print("accept_all not found")    
    
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

    time.sleep(2)
    view_lines = WebDriverWait(browser, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".view-lines.monaco-mouse-cursor-text"))
    )
    ActionChains(browser).move_to_element(view_lines).click().perform()
    time.sleep(2)
    ActionChains(browser).key_down(Keys.CONTROL).send_keys('o').key_up(Keys.CONTROL).perform()
    time.sleep(2)
    ActionChains(browser).send_keys(Keys.DOWN).perform()
    time.sleep(2)
    ActionChains(browser).send_keys(Keys.RETURN).perform()
    time.sleep(4)
    view_lines = WebDriverWait(browser, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".view-lines.monaco-mouse-cursor-text"))
    )
    ActionChains(browser).move_to_element(view_lines).click().perform()
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
    rfc_2822_utc_update_date =  "rfc_2822_utc_update_date = '" + datetime.now(timezone.utc).strftime('%d %b %Y %X') + "'"
    new_script = re.sub('(<GetBotConfig>).*(//</GetBotConfig>)', rf'\1\n{update_date}\n{rfc_2822_utc_update_date}\n{GetBotConfig}\2', script,  flags=re.DOTALL)

    # Pattern to match the line starting with "startDate = input.time(timestamp('
    start_date_pattern = r'^startDate = input\.time\(timestamp\(\'(\d{4}-\d{2}-\d{2})\'\),'

    # Split the original string into lines
    lines = new_script.split('\n')

    # Replace the line matching the pattern
    for i, line in enumerate(lines):
        match = re.match(start_date_pattern, line)
        if match:
            old_date = match.group(1)
            #print("old_date", old_date)
            new_line = re.sub(old_date, date_one_week_ago, line)
            lines[i] = new_line

    # Join the lines back into a string
    new_script = '\n'.join(lines)    

    #print(new_script)

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
    
    if chrome_already_open == False:
        #si chrome n'était pas déjà ouvert alors on le ferme en enlevant les stratégies et les objets et en sauvegardant préalablement
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[aria-label='Remove objects']"))).click()
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-name='remove-studies']"))).click()    
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.ID, "header-toolbar-save-load"))).click()
        time.sleep(30)
        browser.close()
    else :
        #on sauvegarde
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.ID, "header-toolbar-save-load"))).click()
except Exception:
    print(traceback.format_exc())
    pass


