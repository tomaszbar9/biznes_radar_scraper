# BiznesRadar.pl Scraper

Scraps from the biznesradar.pl site the essential data (according to the classical book "The Intelligent Investor"(1949) by Benjamin Graham) of the companies listed on Warsaw Stock Exchange.

The program uses Beautiful Soup for web scraping.

Each row of the data is saved on the fly in a temporary shelve file, so if anything goes wrong while requesting subsequent sites, the user can restart the script using flag: -c or --continue.

When the process is done, all the data is saved in "wse.csv" file. The column headings are in polish.

## Options:

-c, --continue - start scraping where the program previously stopped

-s [SELECTED_SYMBOLS ...], --select [SELECTED_SYMBOLS ...] - scrap only selected symbols

--no-wait - do not wait 1 sec. after each request

-v, --verbose - print messages about progress
