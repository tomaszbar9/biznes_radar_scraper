import argparse
from datetime import datetime
import os
import random
import re
import requests
import shelve
import sys
import time

from bs4 import BeautifulSoup as bs
import pandas as pd


parser = argparse.ArgumentParser(
    prog="Warsaw Stock Exchange Data Scraper",
    description="Scraps the essential data from 'BiznesRadar.pl'.")

group = parser.add_mutually_exclusive_group()

group.add_argument(
    "-c",
    "--continue",
    dest="continue_",
    help="start scraping where the program previously stopped",
    action="store_true"
)

group.add_argument(
    "-s",
    "--select",
    help="scrap only selected symbols",
    dest="selected_symbols",
    nargs='*'
)

parser.add_argument(
    "--no-wait",
    help="do not wait 1 sec. after each request",
    default=False,
    action="store_true",
    dest="nowait"
)

parser.add_argument(
    "-v",
    "--verbose",
    help="print messages about progress",
    action='store_true',
)

args = parser.parse_args()

base_url = 'https://www.biznesradar.pl'
all_stocks_url = 'https://www.biznesradar.pl/gielda/akcje_gpw'
todays_date = datetime.utcnow().date()
current_directory = os.path.abspath('')
temp_file = (os.path.join(current_directory, 'temp_shelf'))
saved_in_temp = None


def wait():
    if not args.nowait:
        time.sleep(1 + random.random())
    else:
        pass


def get_int(string):
    if not string:
        return 0
    extracted = re.match(r'[\d -]+', string)
    if extracted:
        return int(extracted[0].replace(' ', ''))
    return 0


def last_10_years_records(records: list) -> list:
    length = len(records[1:-1])
    last_10 = records[max(1, length - 9):-1]
    return [get_int(year.text) for year in reversed(last_10)]


response_all_stocks = requests.get(all_stocks_url)
wait()

if response_all_stocks.status_code != 200:
    sys.exit("Failed to get the stock list.")


soup_all_stocks = bs(response_all_stocks.text, features="html.parser")
all_links = []

with shelve.open(temp_file) as temp:
    continue_ = False
    if args.continue_:
        continue_ = True
    
    elif len(temp) > 0:
        while True:
            answer = input("Do you want to delete the temporary data? (y/n):")
            if answer.lower() == 'y':
                temp.clear()
                break
            if answer.lower() == "n":
                continue_ = True
                break
    if continue_:
        saved_in_temp = set(temp.keys())
        if args.verbose:
            print("Saved so far:", sorted(list(saved_in_temp)))        

for link in soup_all_stocks.table.find_all('a'):

    if link['href'].startswith('/notowania'):
        symb = re.search(r'\w+', link.text)[0]

        if args.selected_symbols:
            selected_symbols = [symbol.upper()
                                for symbol in args.selected_symbols]
            if symb in selected_symbols:
                all_links.append(link['href'])

        elif saved_in_temp:
            if symb not in saved_in_temp:
                all_links.append(link['href'])

        else:
            all_links.append(link['href'])

links_length = len(all_links)

for index, link in enumerate(all_links):
    response_main_stock_site = requests.get(base_url + link)
    soup = bs(response_main_stock_site.text, features="html.parser")
    wait()

    if not soup.find(string="Liczba akcji:"):
        continue

    name_regex = re.compile(r"Notowania (\w+)( \(\w+\))*")
    search = name_regex.match(soup.h1.text)
    symbol = search.group(1)
    name = (search.group(2) or symbol).strip(" ()")

    financial_analysis_link = base_url + \
        soup.find(string="ANALIZA FINANSOWA").parent['href']
    financial_reponse = requests.get(financial_analysis_link)
    financial_soup = bs(financial_reponse.text, features="html.parser")
    wait()

    balance_link = base_url + \
        financial_soup.find(string="BILANS").parent.get('href')
    response_balance = requests.get(balance_link)
    balance_soup = bs(response_balance.text, features="html.parser")
    wait()

    dividends_link = base_url + \
        soup.find(string="DYWIDENDY").parent.get('href')
    response_dividends = requests.get(dividends_link)
    dividends_soup = bs(response_dividends.text, features="html.parser")
    wait()

    if args.verbose:
        print(f"Finished {name}.", end=' ')
        left = links_length - index - 1
        if left:
            print(f"{left} left.")
        else:
            print("All done!")

    stock_data = {
        "Nazwa": name,
        "Symbol": symbol,
        "Data": str(todays_date),
        "Kurs": float(soup.find(class_='profile-h1-c').find(class_='q_ch_act').text),
        "Liczba akcji": get_int(
            soup.find(string="Liczba akcji:").next.find('a').text),
        "Kapitalizacja": get_int(soup.find(string="Kapitalizacja:").next.text),
        "Zysk netto w ostatnich 10 latach (w tys.)": last_10_years_records(financial_soup.find(
            "tr", attrs={"data-field": "IncomeNetProfit"}).find_all('td'))
    }

    try:
        income = last_10_years_records(financial_soup.find(
            "tr", attrs={"data-field": "IncomeRevenues"}).find_all('td'))
    except:
        income = None
    stock_data["Przychód w ostatnich 10 latach (w tys.)"] = income

    try:
        cena_zysk = float(soup.find(string="C/Z").find_parents('tr')
                          [0].find(class_='value').text)
    except:
        cena_zysk = None
    stock_data['Cena/Zysk'] = cena_zysk

    table_dyw = dividends_soup.table

    if table_dyw.th.text.startswith('wypłata'):
        all_rows = table_dyw.find_all('tr')
        dividends = [row.find_all(
            'td')[2].text for row in all_rows[1:min(11, len(all_rows))]]
        dividends_in_last_10_years = 10 - dividends.count('-')
    else:
        dividends_in_last_10_years = 0

    stock_data['Dywidendy w ostatnich 10 latach'] = dividends_in_last_10_years

    financial_data = balance_soup.find_all(class_="bold")
    finances = {}
    for row in financial_data:
        fin_data_name = row.find_all('td')[0].text + " (w tys.)"
        fin_data_value = row.find(class_='newest').text
        finances[fin_data_name] = get_int(fin_data_value)

    stock_data.update(finances)

    with shelve.open(temp_file) as temp:
        temp[symbol] = stock_data


with shelve.open(temp_file) as temp:
    all_data = [row for row in temp.values()]
    all_data.sort(key=lambda row: row["Symbol"])

stock_df = pd.DataFrame(all_data)
stock_df.to_csv("wse.csv", encoding='utf-8', index=False)
