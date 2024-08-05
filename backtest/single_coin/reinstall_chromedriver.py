from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# Set up Chrome options
options = Options()
options.add_argument("--headless")  # Optional: run in headless mode

# Initialize the Chrome WebDriver
try:
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get("https://www.example.com")
    print(driver.title)  # Print the title of the page
finally:
    driver.quit()  # Make sure to close the driver
