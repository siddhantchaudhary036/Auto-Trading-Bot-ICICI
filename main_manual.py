#API Imports
from neo_api_client import NeoAPI

#Necessary system imports
import pandas as pd
import pytz
from pyotp import TOTP
from seleniumbase import SB
import threading
import time
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

# Email Imports
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Secrets Import
from keys import icici_direct_totp, icici_direct_username, icici_direct_pass
from keys import sender_email, sender_password, recipient_email, smtp_server, smtp_port
from keys import kotak_consumer_secret, kotak_consumer_key, kotak_userid, kotak_password

# Supress Warnings
import warnings
warnings.filterwarnings('ignore')

# Declare Constants
ist = pytz.timezone('Asia/Kolkata')
invest_per_trade = 0.2
with open('BSEScripMaster.txt') as f:
    BSE_scrip_df = pd.read_csv(f, delimiter=',')

# Functions
def send_email(subject,body):

    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = "Trader: "+subject
        msg.attach(MIMEText("Dear Master Chodu"+"\n"+body, 'plain'))
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        print(f"Email sent to {recipient_email}")

    except Exception as e:
        print(f"Failed to send email: {e}")


def autologin():
    for attempt in range(3):

        try:
            url = "https://secure.icicidirect.com/Customer/login?urlpara=trading%252fequity%252fhome"
            print('attempting autologin')

            with SB(headless=True) as sb:  # Non-headless mode (default)
                cookies = sb.driver.get_cookies()
                sb.open(url)
                time.sleep(2)
                
                sb.type("#txtu", icici_direct_username)
                sb.type("#txtp", icici_direct_pass)
                sb.click("#btnlogin")
                time.sleep(2)
                
                
                totp = TOTP(icici_direct_totp)
                token = totp.now()

                sb.type('//*[@id="frmotp"]/div/div[4]/div/div[1]/input', token)
                time.sleep(5)
                cookies = sb.driver.get_cookies()
                print('Autologin successful, SID generated')
                return cookies
            
        except Exception as e:
            print('Autologin Failed. Attempt: ', attempt, e)
            continue

    send_email('Error: Autologin', f"All 3 attempts to autologin to ICICI Direct have failed. {e}")
        

def create_order_df(cookies):

    for i in range(len(cookies)):
        if cookies[i]['name'] == 'ASP.NET_SessionId':
            ASP_index = i

        if cookies[i]['name'] == 'InterSecure':
            InterSecure_index = i


    # URL to which the POST request will be sent
    url = "https://secure.icicidirect.com/handler/equity"

    # Headers for the POST request
    headers ={
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en-GB;q=0.9,en;q=0.8",
    "connection": "keep-alive",
    "content-length": "254",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "cookie": f"{cookies[ASP_index]['name']}={cookies[ASP_index]['value']}; {cookies[InterSecure_index]['name']}={cookies[InterSecure_index]['value']};",
    "host": "secure.icicidirect.com",
    "origin": "https://secure.icicidirect.com",
    "referer": "https://secure.icicidirect.com/trading/equity/click2gain",
    "sec-ch-ua": "\"Chromium\";v=\"128\", \"Not;A=Brand\";v=\"24\", \"Google Chrome\";v=\"128\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
    }

    # Payload (data) to be sent in the POST request
    data = {
    "ddlbuysell": "A",
    "ddlrecommedation": "MRGN",
    "btnview": "View",
    "pgname": "eqclick2gain",
    "ismethod": 1,
    "methodname": "viewiclk2gain",
    "winurl": "https://secure.icicidirect.com/trading/equity/click2gain",
    "dbg": "",
    "dspdv": "click2gain",
    "errdv": "click2gain",
    "isfootprint": "false",
    "transtype":"",
    "stage": "0"
    }


    # Send POST request
    response = requests.post(url, headers=headers, data=data)

    # Your HTML content (replace with actual HTML content if it's stored in a file or another variable)
    html_content = response.text
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', class_='table shady-table accordian_table ajaxTable table-autosort fixheader')
    columns = []
    data = []
    thead = table.find('thead')
    for th in thead.find_all('th'):
        columns.append(th.text.strip())
    tbody = table.find('tbody')
    for tr in tbody.find_all('tr'):
        row = []
        for td in tr.find_all('td'):
            row.append(td.text.strip())
        if row:  # Only add rows that have data
            data.append(row)

    df = pd.DataFrame(data, columns=columns)
    
    return df

def prepare_data(df):

    df.columns = ['stock_name','cmp','rec_price','target_price','stop_loss','profit_percent','profit_price','exit_price','rec_update','action']
    df = df.drop(columns=['exit_price','profit_price','profit_percent','action'])
    
    for i in range(len(df)):
        if df['rec_price'][i] == None and df['target_price'][i] == None:
            df = df.drop(i)
    
    df = df.reset_index(drop=True)
            
    # Remove commas and strip spaces
    df['target_price'] = pd.to_numeric(df['target_price'].str.replace(',', '').str.strip(), errors='raise')     # FLOAT# FLOAT# FLOAT
    df['stop_loss'] = pd.to_numeric(df['stop_loss'].str.replace(',', '').str.strip(), errors='raise')           # FLOAT# FLOAT# FLOAT
    df['cmp'] = pd.to_numeric(df['cmp'].str.replace(',', '').str.strip(), errors='raise')                       # FLOAT# FLOAT# FLOAT
    df['stock_name'] = df['stock_name'].str.replace('\n', '').str.replace('\r', '').str.replace('  ', '').str.replace('\xa0', ' ')
    df['rec_price'] = df['rec_price'].str.replace('\n', '').str.replace('\r', '').str.replace('  ', '')
    df['rec_update'] = df['rec_update'].str.replace('\n','').str.replace('\r', '').str.replace('  ', '')

    df['action_type'] = ""
    df['rec_price_from'] = ""
    df['rec_price_to'] = ""

    for i in range(len(df)):
        if "buy" in df['stock_name'][i].split('-')[1].lower():
            df['action_type'][i] = 'buy'
        elif "sell" in df['stock_name'][i].split('-')[1].lower():
            df['action_type'][i] = 'sell'
        else:
            df['action_type'][i] = None
        rec_price_from = df['rec_price'][i].split('(')[0].split('-')[0].replace(',','')
        rec_price_to = df['rec_price'][i].split('(')[0].split('-')[1].replace(',','')
        df['rec_price_from'][i] = float(rec_price_from)
        df['rec_price_to'][i] = float(rec_price_to)
        

    return df

def login_to_apis():
        
    for attempt in range(3):
        try:

            '''LOGIN TO KOTAK NEO'''
            client = NeoAPI(consumer_key=kotak_consumer_key, consumer_secret=kotak_consumer_secret, environment='prod',
                        access_token=None, neo_fin_key=None)
            client.login(mobilenumber=kotak_userid, password=kotak_password)
            client.session_2fa(OTP = '119904')

            cash_url = client.scrip_master(exchange_segment='NSE') # You may pass nothing in exchange_segment to get all the scrips
        
            kotak_master_scrip = pd.read_csv(cash_url)
            initial_funds = float(client.limits()['Net'])

            print('Both APIs logged in successfully')
            return client,kotak_master_scrip,initial_funds
        
        except Exception as e:
            print('API Login Failed. Attempt: ', attempt, e)
            continue

    send_email('Critical Trader Failure: API Login', f"All 3 attempts to log in to the ICICI Direct / Kotak Neo APIs have failed. {e}")

def place_order_kotak(symbol, quantity, transaction_type, order_type, stoploss,amo,client):

    '''  Pass as "HDFCBANK", "10", "B/S","SL-M/MKT","150/0"
        Make Ordertype as SL-M for stoploss orders and set trigger price as stoploss'''
    
    amo = 'YES' if amo else 'NO'
    client.place_order(exchange_segment='nse_cm', product='MIS',
                        price="0", order_type=order_type, quantity=str(quantity),
                        validity='DAY', trading_symbol=str(symbol)+"-EQ",
                            transaction_type=str(transaction_type), amo=amo,
                            disclosed_quantity="0", market_protection="0",
                                pf="N", trigger_price=str(stoploss), tag=None)


def calculate_entry_quantity_kotak(ltp, investment_per_trade, initial_funds):
    margin = float(ltp)/4
    quantity = int((float(initial_funds)/float(margin))*float(investment_per_trade))
    return quantity

def map_security_to_standard_format(stock_name):
    '''Finds the standard format ticker symbol for the given stock name'''
    stock_name_lower = stock_name.lower()
    BSE_scrip_df_lower = BSE_scrip_df['CompanyName'].str.lower()
    return BSE_scrip_df[BSE_scrip_df_lower == stock_name_lower]['ScripID'].values[0]

def get_available_quantity(symbol,client):
    cp = None  # Initialize cp to None to check later if it has been fetched successfully

    for attempt in range(3):
        try:
            cp = client.positions()['data']  # Attempt to fetch client positions
            break  # Exit the loop if successful

        except Exception as e:
            print(f"Attempt {attempt + 1} failed. Error: {e}")
            send_email("Error in fetch client positions", "Please check the system. Orders can not be placed.",e)
            time.sleep(5)

    # Process fetched client positions for the available quantity
    for i in cp:
        if i['sym'] == symbol:
            if i['flBuyQty'] != i['flSellQty']: 
                open_quantity = max(int(i['flBuyQty']), int(i['flSellQty'])) - min(int(i['flBuyQty']), int(i['flSellQty']))
                return open_quantity
            else:
                return 0
    return 0  # Return 0 if the symbol is not found


def relogin_kotak():

    for attempt in range(3):
        try: 
            '''We need to login every time before we place an order because the system goes stale and does not execute orders without raising any errors'''
            client = NeoAPI(consumer_key=kotak_consumer_key, consumer_secret=kotak_consumer_secret, environment='prod', access_token=None, neo_fin_key=None)
            client.login(mobilenumber=kotak_userid, password=kotak_password)
            client.session_2fa(OTP = '119904')
            print('relogin to kotak neo successful')
            return client
        
        except Exception as e:
            print('Relogin Failed. Attempt: ', attempt, e)
            time.sleep(10)
            
    send_email("Kotak Relogin Failed",f"All 3 attempts to relogin to Kotak Neo API have failed. Please check the system. Orders can not be placed. Error: {e}")

def report_results():
    '''Function will send one EOD a day on weekdays between 3:31 PM and 3:35 PM IST. The master loop will reinitiate the function '''

    while True:

        if datetime.now(ist) > datetime.now(ist).replace(hour = 15, minute = 31, second = 0, microsecond=0) and datetime.now(ist) < datetime.now(ist).replace(hour = 15, minute = 35, second = 0, microsecond=0) and datetime.now(ist).weekday() < 5 and datetime.now(ist).weekday() >= 0:
            for i in range(3):
                try:
                    client = relogin_kotak()
                    cp = client.positions()
                    total_pnl = 0
                    b1_total = ""
                    for i in range(len(cp['data'])):
                        buy_price = float(cp['data'][i]['buyAmt'])/int(cp['data'][i]['flBuyQty'])
                        sell_price = float(cp['data'][i]['sellAmt'])/int(cp['data'][i]['flSellQty'])
                        pnl = float(cp['data'][i]['sellAmt']) - float(cp['data'][i]['buyAmt'])
                        total_pnl += pnl

                        b1 = f"{cp['data'][i]['sym']}, Profit: {int(pnl)}, Buy Price: {buy_price}, Sell Price: {sell_price}"
                        print(b1)
                        b1_total += "\n"+b1

                    b2 = f"Total PNL: {int(total_pnl)}\nInitial Funds Now: {initial_funds+total_pnl}"
                    print(b2)

                    send_email("EOD RESULTS", "PNL Results"+b1_total+"\n"+b2)
                    sent = True
                    return None # Exit the function after sending the email
                    

                except Exception as e:
                    print(f"Error in report_results: {e} Attempt {i+1}")
                    time.sleep(10)

            send_email("Error in EOD Reporting", f"All 3 attempts to report EOD results have failed. Please check the system. Maybe the market is closed today or Kotak is under maintainance rn?? Error: {e}")
        
        else:
            time.sleep(60)  


def on_ticks(ticks):
    
    if True: # You may add conditions like only trading margin stocks. 
        print("\n")
        print(f" ==> TICK FROM SERVER: {ticks}")
        print("\n")
        stock_name = ticks['stock_name'].split(' (')[0]
        action_type = ticks['action_type']
        recommended_price = [ticks['rec_price_from'],ticks['rec_price_to']]
        stoploss = ticks['stop_loss']
        recommended_update = ticks['rec_update']
        ticker = map_security_to_standard_format(stock_name)
        ltp = recommended_price[0]

        if datetime.now(ist) > datetime.now(ist).replace(hour = 9, minute = 16, second = 0, microsecond=0) and datetime.now(ist) < datetime.now(ist).replace(hour = 15, minute = 29, second = 0, microsecond=0):

            if recommended_update.strip() == "":

                client = relogin_kotak()
                quantity = calculate_entry_quantity_kotak(ltp, invest_per_trade,initial_funds)
                action_type = 'B' if 'buy' in action_type.lower() else 'S'
                place_order_kotak(ticker, quantity, action_type, "MKT", stoploss,False,client)
                print('Entry Order Placed')


            elif "partial" in recommended_update.lower():
                client = relogin_kotak()
                action_type = 'S' if 'buy' in action_type.lower() else 'B'
                order_quantity = int(int(get_available_quantity(ticker,client))/2) # Half the open quantity for partial exit

                if order_quantity>0: # It will return 0 if all positions are closed, or no such position was found in the for loop using symbol
                    place_order_kotak(ticker, order_quantity, action_type, "MKT", stoploss,False,client)
                    print('Partial Exit Order Placed')
                else:
                    print('No such open position exists. Ignoring partial exit updates')


            elif "full" in recommended_update.lower() or "exit" in recommended_update.lower() or "sltp" in recommended_update.lower() or "tgt" in recommended_update.lower() or recommended_update.strip() != "":
                client = relogin_kotak()
                action_type = 'S' if 'buy' in action_type.lower() else 'B'
                order_quantity = get_available_quantity(ticker,client)

                if order_quantity>0: # It will return 0 if all positions are closed, or no such position was found in the for loop using symbol
                    place_order_kotak(ticker, order_quantity, action_type, "MKT", stoploss,False,client)
                    print('Exit Order Placed')
                else:
                    print('No such open position exists. Ignoring exit updates')


        else:
            if recommended_update.strip() == "":
                print('Market is closed, placing AMO')
                client = relogin_kotak()
                quantity = calculate_entry_quantity_kotak(ltp, invest_per_trade,initial_funds)
                action_type = 'B' if 'buy' in action_type.lower() else 'S'
                place_order_kotak(ticker, quantity, action_type, "MKT", stoploss,True,client)
                print('AMO entry Order Placed')

def get_unique_key(row):
    stock_name = row['stock_name'].strip().upper()
    rec_price = str(row['rec_price']).replace(',', '').strip()
    action_type = row['action_type'].strip().lower()
    return f"{stock_name}_{rec_price}_{action_type}"

def detect_changes(cookies):

    standing_df = pd.read_csv('standing_df.csv')

    while datetime.now(ist) < datetime.now(ist).replace(hour = 15, minute = 29, second = 0, microsecond=0) and datetime.now(ist) > datetime.now(ist).replace(hour = 9, minute = 16, second = 0, microsecond=0) and datetime.now(ist).weekday() < 5 and datetime.now(ist).weekday() >= 0:
        
        time.sleep(60)
        df = create_order_df(cookies)
        df = prepare_data(df)
        df['unique_key'] = df.apply(get_unique_key, axis=1)

        for index, row in df.iterrows():
            if row['unique_key'] not in standing_df['unique_key'].values:

                print(f"Entering {row['stock_name']}")
                on_ticks(dict(row))
                row_df = pd.DataFrame([row])  # Convert row to DataFrame
                standing_df = pd.concat([row_df, standing_df]).reset_index(drop=True)  # Append new row to the top

            else:
                # Check if 'rec_update' differs from existing row
                if row['rec_update'] != standing_df[standing_df['unique_key'] == row['unique_key']]['rec_update'].values[0]:

                    print(f"Updating {row['stock_name']}")
                    on_ticks(dict(row))
                    standing_df = standing_df.drop(standing_df[standing_df['unique_key'] == row['unique_key']].index)  # Drop old row
                    row_df = pd.DataFrame([row])  # Convert row to DataFrame
                    standing_df = pd.concat([row_df, standing_df]).reset_index(drop=True)  # Append updated row to the top

                else:
                    continue

        # Second loop: Remove rows from standing_df if they are no longer in df
        for index, row in standing_df.iterrows():
            if row['unique_key'] not in df['unique_key'].values:

                row['rec_update'] = "Exit"
                print(f"Exiting {row['stock_name']}")
                on_ticks(dict(row))
                standing_df = standing_df.drop(standing_df[standing_df['unique_key'] == row['unique_key']].index)

        # So we can keep an eye on the current state of the standing_df
        standing_df.to_csv('standing_df.csv', index=False)




while True:

    # Logging into APIS
    client, kotak_master_scrip, initial_funds = login_to_apis()


    # Start the WebSocket in a separate thread
    cookies = autologin()
    websocket_thread = threading.Thread(target=detect_changes, args=(cookies,))
    websocket_thread.start()

    # Send me previous days results
    result_reporting_thread = threading.Thread(target=report_results)
    result_reporting_thread.start()


    # Sleep precisely till 7AM IST next day (DO NOT START THIS SERVER AFTER 12AM IST, WILL SKIP ONE DAY OF ON TIME)
    '''Fix this to only activat on weekdays and make it such that it can be initiated at any time of the day'''
    current_time = datetime.now(ist)
    next_day = current_time + timedelta(days=1)
    next_day = next_day.replace(hour=7, minute=0, second=0, microsecond=0)
    sleep_time = (next_day - current_time).total_seconds()
    print(f"Sleeping for {float(sleep_time)/3600} Hours")
    print("Initial funds: ", initial_funds)
    time.sleep(sleep_time)

    # Stop the WebSocket thread and result reporting thread before restarting
    websocket_thread.join()
    result_reporting_thread.join()

    # Reset the standing_df
    standing_df = pd.DataFrame(columns=['stock_name','cmp','rec_price','target_price','stop_loss','rec_update','action_type','rec_price_from','rec_price_to','unique_key'])
    standing_df.to_csv('standing_df.csv', index=False)
