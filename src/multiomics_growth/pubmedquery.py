#!/usr/bin/env python3

# Queries PubMed to obtain number of multiomics studies between 2000 and 2018 
# A multiomics study is defined as a study that has two or more from the following keywords ('Other Term' PubMed field): 
# genomics, lipidomics, proteomics, glycomics, transcriptomics, metabolomics, epigenomics, microbiomics

import xml.etree.ElementTree as ET
import itertools, requests, time, argparse, sys
from datetime import date, timedelta

# useful constants
ALL_KEYWORDS = ['genomics', 'lipidomics', 'proteomics', 'glycomics', 'transcriptomics', 'metabolomics', 'epigenomics', 'metagenomics', 'phosphoproteomics']
DATE_FORMAT = '%Y/%m/%d'
# we want to send, at most, 10 queries per second,
# this is one query every 100ms, but let's be lenient
MIN_TIME_BETWEEN_QUERIES = 110.0

# returns a pubmed url
def build_query_url(api_key, keywords, date_from, date_until):
    query_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?retmax=100000&api_key=%s&term=%s[Date+-+Publication]:%s[Date+-+Publication]' % (api_key, date_from.strftime(DATE_FORMAT), date_until.strftime(DATE_FORMAT))
    for keyword in keywords:
        query_url += '+AND+%s[Other+Term]' % (keyword)
    return query_url

# returns the ids
def query_pubmed(url):
    # In order not to overload the E-utility servers, NCBI recommends that users post no more 
    # than three URL requests per second and limit large jobs to either weekends or between 9:00 PM and 5:00 AM Eastern time during weekdays.
    # Users with an API key receive 10 queries per second! 
    global time_last_query
    print('#url=%s' % url)
    while True:
        try:
            time.sleep(MIN_TIME_BETWEEN_QUERIES / 1000.0)
            raw_response = requests.get(url).text
            xml_response = ET.fromstring(raw_response)
            # do some sanity check
            if int(xml_response.findall('.//Count')[0].text) > int(xml_response.findall('.//RetMax')[0].text):
                raise ValueError('Query returned more results than expected!!! %s' % url)
            return [study_id.text.strip() for study_id in xml_response.findall('.//IdList/Id')]
        except:            
            print('#ERROR %s' % sys.exc_info()[0])
            print('#ERROR retrieving %s, retrying in one second...' % url)
            time.sleep(1000.0)


def current_milli_time(): 
    return int(round(time.time() * 1000))

# generate combinations of 2, 3, ... N elements from all_keywords
def get_keyword_combinations(all_keywords):
    keywords = []
    for i in range(2, len(all_keywords) + 1):
        keywords.extend(itertools.combinations(all_keywords, i))
    return keywords

# performs all queries against PubMed, 
# there is one query per combination of -omics disciplines (e.g., [genomics, multiomics]) per year
# output (stdout) format:
# Keywords=genomics,multiomics,metabolomics; From=2001/01/01; To=2001/12/31; IDs=123,456,789
def do_all_queries(args):
    keyword_combinations = get_keyword_combinations(ALL_KEYWORDS)
    # go through the years, query
    current_date_from = date(args.first_year, 1, 1)
    # we need the last day of the last year
    last_date = date(int(args.last_year), 12, 31)
    while current_date_from < last_date:
        # date until the end of the year 
        current_date_until = date(current_date_from.year + 1, 1, 1) - timedelta(days=1)
        print('#Finding multiomics studies between %s and %s' % (current_date_from.strftime(DATE_FORMAT), current_date_until.strftime(DATE_FORMAT)))
        # query each combination for this year
        for current_keyword_combination in keyword_combinations:
            ids = query_pubmed(build_query_url(args.api_key, current_keyword_combination, current_date_from, current_date_until))
            # Keywords=genomics,multiomics,metabolomics; From=2001/01/01; To=2001/12/31; IDs=123,456,789
            print ('Keywords=%s; From=%s; To=%s; IDs=%s' % (','.join(current_keyword_combination), current_date_from.strftime(DATE_FORMAT), current_date_until.strftime(DATE_FORMAT), ','.join(ids)))

        # increment date to next year
        current_date_from = date(current_date_from.year + 1, 1, 1)

# parses raw results
# input format:
# Keywords=[comma-separated studies]; From=YYYY/mm/dd; To=YYYY/mm/dd; IDs=[comma-separated ids]
# sample input format:
# Keywords=genomics,multiomics,metabolomics; From=2001/01/01; To=2001/12/31; IDs=123,456,789
#
# output format (stdout):
# Year, 2-layered, 3-layered, >3-layered, total
# output sample:
# 2007,1,1,0,2
# 2008,9,0,0,9
def parse_results(results_filepath):
    # parsed results are a map, keys are years and values is a list:
    # year -> [2-layer, 3-layer, 3+-layer]
    results = {}
    with open(results_filepath) as f:        
        for line in f.readlines():
            line = line.strip()
            if line[0] != '#':
                # Keywords=genomics,proteomics,transcriptomics,metabolomics,metagenomics; From=2004/01/01; To=2004/12/31; IDs=1,2,3
                # extract values from the raw results
                fields = [field.strip() for field in line.split(';')]
                n_layers = len(fields[0].split('=')[1].split(','))
                year = int(fields[1][len('From='):len('From=')+4])
                ids = fields[3].split('=')[1].split(',')
                index = min(n_layers - 2, 2)
                if not year in results:
                    results[year] = [set(), set(), set()]
                # warning: 'IDs='.split('=')[1].split(',') == [''] is True
                if len(ids) > 0 and len(ids[0]) > 0:     
                    results[year][index].update(ids)
    # clean results 
    for year, fields in results.items():
        for index_upper_layer in range(2, -1, -1):
            # look for ids in upper indices (the higher the index, the more layered is the study)
            # and remove them from sets located in a lower index (lower index = less -omics layers)
            for index_lower_layer in range(index_upper_layer):
                for study_id in fields[index_upper_layer]:
                    fields[index_lower_layer].discard(study_id)
    # generate total number of studies
    print('#Year,2-omics,3-omics,>3-omics,Total')
    for year, fields in results.items():
        print('%d,%d,%d,%d,%d' % (year, len(fields[0]), len(fields[1]), len(fields[2]), len(fields[0]) + len(fields[1]) + len(fields[2])))


def main():
    parser = argparse.ArgumentParser(description='Query ubMed.')
    parser.add_argument('-a', '--api-key', type=str, help='API Key')
    parser.add_argument('-p', '--parse-results-from', type=str, default=None, help='File from which results will be parsed')
    parser.add_argument('-f', '--first-year', type=int, default=2000, help='Year to start querying')
    parser.add_argument('-l', '--last-year', type=int, default=2017, help='Year to stop querying')
    args = parser.parse_args()
    
    if args.parse_results_from is None:
        do_all_queries(args)
    else:
        parse_results(args.parse_results_from)

if __name__ == '__main__':
    main()
