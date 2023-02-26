#!/usr/bin/env python
import csv
import math
from pprint import pprint
import requests_cache
import logging
import requests
import pandas as pd
import tldextract
import xlsxwriter
import time
from tqdm.auto import tqdm
from url_filter import improve_urls_list, check_improvments, print_words_in_urls

logging.basicConfig(filename='logs/compliance_crawler.log',
                    level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

google_key = "AIzaSyAxRvLUHoPLNBKV4BM3je6JaunOMvWcPz4"  # findings key
# google_key = "AIzaSyCJ2F76Zxna1TgRhs4CbwdQpacltW7R5PE"  # other key


cx = "a3c4a6a5fb8624c4a"

search_word_list = []
domain_list = []

# import domain list from csv file to list
with open("csvs/domains_list.csv") as file:
    for line in file:
        domain_list = domain_list + [line.strip()]

# import word list from csv file to list
with open("csvs/Certification.csv") as file:
    for line in file:
        search_word_list = search_word_list + [line.strip()]

search_word_list = search_word_list[1:]  # Remove header
GOOGLE_URL = "https://www.googleapis.com/customsearch/v1"
NUMBER_OF_RESULTS = 10
search_word_list = search_word_list[5:8]  # Limit on word list
OUTPUT_DIR = "output/"


# define Python API calls exceptions
class TooManyAPICalls(Exception):
    pass


def google_api_get(session, params_dict, max_retries=3):
    """Schedule the calls to google API and keep it below the 100 in a minute.
     Google Custom Search API supports max of 100 searches in one minute per user.

    :param session: Current session in request format.
    :param params_dict : list of parameters to google api call
    :returns res_dict: dictionary
    """
    res_dict = {}
    retries = max_retries
    while (retries >= 0):
        res = session.get(GOOGLE_URL, params=params_dict)
        res_dict = res.json()
        if ('error' in res_dict) and (res_dict['error']['errors'][0]['reason'] == 'rateLimitExceeded'):
            logging.warning("----WARNING: rateLimitExceeded retrying in 60 seconds----")
        else:
            return res_dict
        retries -= 1
        time.sleep(60)

    logging.error(res_dict['error'])
    raise TooManyAPICalls()

    return res_dict


def findResults(companies_domain_dict):
    """Find result links in google search.
        add metadata about the search results.
        apply logic to search in different pages.
        :returns results: list of urls
        """

    session = requests_cache.CachedSession(cache_name='google_search', backend='sqlite', expire_after=180)

    results = []
    # add metadata about the search that is about to perform in google.
    # parameter list documentation:
    # https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list#request
    for keys, values in tqdm(companies_domain_dict.items()):
        for domain in companies_domain_dict.values():
            for word in search_word_list:
                params_dict = add_query_parameters(query=word, siteSearch=domain, siteSearchFilter="i", lr="lang_en")
                num_search_results = params_dict['num']
                calls_to_make = divide_to_pages(num_search_results, params_dict)
                params_dict['start'] = start_item = 1
                try:
                    res_dict = google_api_get(session, params_dict)
                except TooManyAPICalls:
                    logging.error("Error: Exceeded Queries per day limit per day")
                    return results

                if not res_dict['searchInformation']['totalResults'] == '0':
                    # get the link item from the request
                    while calls_to_make > 0:
                        try:
                            res_dict = google_api_get(session, params_dict)
                        except TooManyAPICalls:
                            logging.error("Error: Exceeded Queries per day limit per day")
                            return results
                        items = res_dict["items"]
                        res_fin = [i["link"] for i in items]
                        for link in res_fin:
                            results.append((domain, word, link))
                        calls_to_make -= 1
                        start_item += 10
                        params_dict['start'] = start_item
                        leftover = num_search_results - start_item + 1
                        if 0 < leftover < 10:
                            params_dict['num'] = leftover
    return results


def divide_to_pages(num_search_results, params_dict):
    """divide the numbers of searches to pages
        :returns calls_to_make: int
        """
    calls_to_make = 0
    if num_search_results > 100:
        # search in different pages
        logging.error('Google Custom Search API supports max of 100 results')
    elif num_search_results > 10:
        params_dict['num'] = 10  # this cannot be > 10 in API call
        calls_to_make = math.ceil(num_search_results / 10)
    else:
        calls_to_make = 1
    return calls_to_make


def add_query_parameters(**kwargs):
    """add metadata about the search results.
        :returns results: dictionary of the query parameters
        """
    query = f'"{kwargs["query"]}"'
    params_dict = {"q": query,
                   "key": google_key,
                   "cx": cx,
                   "siteSearch": kwargs["siteSearch"],
                   "siteSearchFilter": kwargs["siteSearchFilter"],
                   "num": NUMBER_OF_RESULTS,
                   "safe": "Active",
                   "lr": kwargs["lr"]
                   }
    return params_dict


def create_domain_per_company_dictionary():
    """find the domains list from the companies names list using google api.
    :returns companies_domain_dict: dictionary in the format "company name: domain"
    """
    session = requests_cache.CachedSession(cache_name='google_search', backend='sqlite', expire_after=180)
    res_dict = {}
    companies_domain_dict = {}

    # make a list from the first column ("Company name") of the csv file
    companies_names = get_company_names_from_csv('csvs/working file Vendor index test rows 1-200 - demo')
    companies_names = companies_names[1:20]
    for company_name in tqdm(companies_names):
        params_dict = add_query_parameters(query=company_name, siteSearch=None, siteSearchFilter=None, lr=None)
        try:
            res_dict = google_api_get(session, params_dict)
        except TooManyAPICalls:
            logging.error("Error: Exceeded Queries per day limit per day")
        domain = res_dict['items'][0]['formattedUrl']
        companies_domain_dict[company_name] = domain
        domain_list.append(domain)
    logging.debug(f"Content in companies_domain_dict: {companies_domain_dict}")
    return companies_domain_dict


def get_company_names_from_csv(file_name):
    """Make a list of names from the company field in the csv file

    :param file_name: str csv table name
    :returns companies_list: list
    """
    companies_list = []
    file_name = file_name + ".csv"
    # take a company from the companies column and add it to the company list
    with open(file_name) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            company = row[0]
            companies_list.append(company)
    # ignore the assessments table that appears in the same column
    companies_list = companies_list[1:200]
    return companies_list


def create_companies_and_domains_table(table_content):
    """Get a dictionary in the format "company name: domain" and present it in xlsx table
    """
    workbook = xlsxwriter.Workbook('companies domains.xlsx')
    worksheet = workbook.add_worksheet()
    row = 0
    col = 0
    domain = ""
    for key in tqdm(table_content.keys()):
        row += 1
        worksheet.write(row, col, key)
        i = 1
        for item in table_content[key]:
            for char in item:
                domain += item
            worksheet.write(row, 1, domain)
        domain = ""
    workbook.close()


def create_google_api_output_file():
    companies_domain_dict = create_domain_per_company_dictionary()
    # create_companies_and_domains_table(companies_domain_dict) # create xlsx table in the format : company | domain
    results = findResults(companies_domain_dict)
    df = pd.DataFrame(results, columns=['Site', 'Cert', 'Link'])
    df.to_csv("csvs_results/cert_results.csv")  # create csv table in the format : 'Site' | 'Cert' | 'Link'
    # df.to_excel("cert_results.xlsx") # create xlsx table in the format : 'Site' | 'Cert' | 'Link'


if __name__ == "__main__":
    create_google_api_output_file()

    # filter the urls results
    improve_urls_list()

    print_words_in_urls() # print the word and the number of time it appears in all urls


