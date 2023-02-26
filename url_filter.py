#!/usr/bin/env python
import csv
import re
from collections import Counter
from pprint import pprint
import pandas as pd
from tqdm.auto import tqdm
from datetime import datetime
from copy import copy, deepcopy

good_urls = []
bad_urls = []

with open('url_examples/bad_urls.txt', 'r', encoding='utf-8-sig') as file:
    for line in file:
        bad_urls = bad_urls + [line.strip()]
bad_urls = set(bad_urls)

# load best fit urls into list
with open('url_examples/best_fit_urls.txt', 'r', encoding='utf-8-sig') as file:
    for line in file:
        good_urls = good_urls + [line.strip()]
good_urls = set(good_urls)

def print_into_log(number_of_urls_in_file, bad_urls_file, best_url_list, bad_urls, missing_urls, found_bad_urls):
    # print the good urls, bad url, precision and the diff between old version and new version int log file
    now = datetime.now()
    old_table = pd.read_csv("csvs_results/cert_results.csv")
    new_table = pd.read_csv("csvs_results/cert_results_updated.csv")
    lines_old = len(old_table)
    lines_new = len(new_table)
    current_time = now.strftime("%H:%M:%S")
    with open("logs/log.txt", "a+") as f:
        f.write(str(now) + '\n')
        f.write(f"best url links:\n ")
        for link in best_url_list:
            f.write(link + "\n")
        f.write("\n")
        f.write("Missing URLs in the updated file: \n")
        for link in missing_urls:
            f.write(link + "\n")
        f.write("\n")
        f.write("Bad URLs in the updated file: \n")
        for link in found_bad_urls:
            f.write(link + "\n")
        f.write("\n")
        f.write(
            f"Number of examples of good urls in the file {number_of_urls_in_file} out of {len(best_url_list)}\n")
        f.write(
            f"Number of examples of bad urls in the file {bad_urls_file} out of {len(bad_urls)}\n")
        f.write(f"Number of lines in the old file: {lines_old}\n")
        f.write(f"Number of lines in the updated file: {lines_new}\n")
        percentage = round(get_change(lines_new, lines_old), 2)
        # precision = round(100 * number_of_urls_in_file/(number_of_urls_in_file+bad_urls_file), 2)
        f.write(f"Improve result by: {percentage}%\n")
        # f.write(f"Precision: {precision}%\n")
        f.write('-------- \n')

def get_company_names_from_csv(file_name):
    domain_list = []
    url_list = []
    file_name = file_name + ".csv"
    # take a company from the companies column and add it to the company list
    with open(file_name) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            url = row[3]
            url_list.append(url)
            domain = row[1]
            domain_list.append(domain)
    # ignore the assessments table that appears in the same column
    return domain_list[1:], url_list[1:]


def remove_domain_from_url(d, u):
    return u.removeprefix(d)

def print_words_in_urls():
    domain_list, url_list = get_company_names_from_csv("csvs_results/cert_results")
    url_list = url_list
    domain_list = domain_list
    list_word_lists = []
    for item in zip(domain_list, url_list):
        domain, url = item
        inner_page_url = remove_domain_from_url(domain, url)
        w_l = re.split('; |-|\.|/', inner_page_url)
        list_word_lists+= w_l
    pprint(Counter(list_word_lists))


def is_relevant(url, good_words_list):
    url = url.lower()
    additional_good_words = ["APEC", "FIPS", "RiskRecon", "SKYHIGH", "SOC", "CSA", "Processor", "certifications"]
    bad_words_list = ["blog", "articles", "finance", "B0"]

    result = False

    # add good words to the certs list
    good_words_list += additional_good_words
    for i in range(len(good_words_list)):
        good_words_list[i] = good_words_list[i].lower()

    for i in range(len(bad_words_list)):
        bad_words_list[i] = bad_words_list[i].lower()

    for bad_word in bad_words_list:
        if bad_word.lower() in url:
            return False

    for word in good_words_list:
        if word in url:
            return True
    return False

def get_change(current, previous):
    if current == previous:
        return 0
    try:
        return (abs(current - previous) / previous) * 100.0
    except ZeroDivisionError:
        return float('inf')
def check_improvments(file_name_to_check):
    file_name_to_check = file_name_to_check + ".csv"


    all_urls = []
    # check if the best fit urls in the file
    URL_IDX = 3
    with open(file_name_to_check, 'r') as file:
        reader_obj = csv.reader(file)
        for line in reader_obj:
            url = line[URL_IDX]
            all_urls.append(url)

    all_urls = set(all_urls)
    missing_urls = list(good_urls - all_urls)
    url_count = len(all_urls & good_urls)
    bad_urls_file = len(all_urls & bad_urls)
    found_bad_urls = (bad_urls & all_urls)

    # print the to log
    print_into_log(url_count, bad_urls_file, good_urls, bad_urls, missing_urls, found_bad_urls)


def has_good_score(url, cert_word):
    score = 0

    original_url = copy(url)
    url = url.lower()

    bad_words_general = ["Brief", "Summary", "blog", "event", "confrence",
                         "Solutions", "resources", "cyberglossary", "Newsroom", "content",
                         "Report", "What is", "Guide", "b0"]

    bad_words_aws = ["Integration", "Integrations", "Gateway",
                     "Press Release", "Native app", "Firewall", "VPC", "Architecture",
                     "Exam Prep", "Downloads", "Store", "Updates", "licensing"]

    bad_words_sox = ["sports-fan", "shop", "accessories", "jewelry", "boston-red-sox"]

    very_good_words = ["APEC", "FIPS", "RiskRecon", "SKYHIGH", "SOC", "CSA"]


    iso_words = ["ISO-27001", "ISO-27002", "ISO-27018", "ISO-27032",  "ISO-9001",
                 "ISO-50001", "ISO-22301"] # + ["ISO-27701", "ISO-27017",]
    additional_good_words = ["About-us", "Privacy", "product-certifications",
                             "certifications", "federal-certifications",
                             "corporate-responsibility", "responsible-sourcing",
                             "docs/compliance", "compliancemapping", "sustentabilidade", "data-privacy/cyber-security",
                             ] + iso_words

    good_suffix = [".pdf"]

    result = False

    # add good words to the certs list
    good_words_list = very_good_words + additional_good_words

    if cert_word == 'AWS':
        for bad_word_aws in bad_words_aws:
            if (bad_word_aws.lower() in url) or ("VM" in original_url):
                score = score - 1

    if cert_word == 'SOX':
        for bad_word_sox in bad_words_sox:
            if bad_word_sox.lower() in url:
                score = score - 1

    for bad_word in bad_words_general:
        if bad_word.lower() in url:
            score = score - 1

    for good_word in good_words_list:
        if good_word.lower() in url:
            score = score + 1

    for suffix in good_suffix:
        if suffix.lower() in url:
            score = score + 0.5

    if score >= 1.0:
        return True

    return False

def improve_urls_list():
    # print_words_in_urls() # print all the words that appears in the urls
    certs_list = []
    good_words_list = []
    # add all the certs to te good word list
    with open("csvs/Certification.csv") as file:
        for line in file:
            certs_list = certs_list + [line.strip()]
    certs_list = certs_list[1:]
    for cert in certs_list:
        result = cert.replace(" ", "-")
        good_words_list.append(result.lower())
    file = "csvs_results/cert_results.csv"
    updated_file = "csvs_results/cert_results_updated.csv"

    URL_IDX = 3
    cert_IDX =2
    with open(file, mode="r") as old_file:
        reader_obj = csv.reader(old_file)  # read the current csv file

        with open(updated_file, mode="w") as new_file:
            writer_obj = csv.writer(new_file, delimiter=",")  # Writes to the new CSV file
            for line in tqdm(reader_obj):
                url = line[URL_IDX]
                cert_word = line[cert_IDX]
                if has_good_score(url, cert_word):
                    # loop through the read data and write each row in new_demo_csv.csv
                    writer_obj.writerow(line)

    check_improvments("csvs_results/cert_results_updated")
    organize_table(updated_file)

def url_type(url):
    if url in good_urls:
        return "good"
    elif url in bad_urls:
        return "bad"
    return "unknown"


def organize_table(csv_file):
    """
    add title to each column, add pdf and url_type columns
    :param csv_file: the file to reorganize
    :return:
    """

    pdf_list = []
    csv_name = csv_file.removesuffix(".csv")
    df = pd.read_csv(csv_file)
    first_column = df.columns[0]
    # Delete first
    df = df.drop([first_column], axis=1)
    df.to_csv(csv_file, index=False)

    df = pd.read_csv(csv_file)
    df.columns = ['Domain', 'Cert', 'Url']
    df.to_csv(csv_file)

    df = pd.read_csv(csv_file)
    df["PDF"] = df["Url"].str.contains(".pdf")
    df["Classification"] = df["Url"].apply(url_type)
    df.to_csv(csv_file, index=False)




