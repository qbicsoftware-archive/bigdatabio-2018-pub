#!/usr/bin/env python3

# Queries PubMed to obtain number of multiomics studies between 2000 and 2018 
# A multiomics study is defined as a study that has two or more from the following keywords ("Other Term" PubMed field): 
# genomics, lipidomics, proteomics, glycomics, transcriptomics, metabolomics, epigenomics, microbiomics

import xml.etree.ElementTree as ET
import itertools, requests, time, argparse
from datetime import date, timedelta

# useful constants
#DATE_FROM = date(2009, 1, 1)
DATE_UNTIL = date(2018, 12, 31)
ALL_KEYWORDS = ["genomics", "lipidomics", "proteomics", "glycomics", "transcriptomics", "metabolomics", "epigenomics", "metagenomics", "phosphoproteomics"]
DATE_FORMAT = "%Y/%m/%d"
# we want to send, at most, 8 queries per second,
# this is one query every 125ms
MIN_TIME_BETWEEN_QUERIES = 110.0


# global variables
time_last_query = -1

# returns a pubmed url
def build_query_url(api_key, keywords, date_from, date_until):
    query_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?retmax=100000&api_key=%s&term=%s[Date+-+Publication]:%s[Date+-+Publication]" % (api_key, date_from.strftime(DATE_FORMAT), date_until.strftime(DATE_FORMAT))
    for keyword in keywords:
        query_url += "+AND+%s[Other+Term]" % (keyword)
    return query_url

# returns the ids
def query_pubmed(url):
    # In order not to overload the E-utility servers, NCBI recommends that users post no more 
    # than three URL requests per second and limit large jobs to either weekends or between 9:00 PM and 5:00 AM Eastern time during weekdays. 
    global time_last_query
    # new window started, query away!
    #time_since_last_query = current_milli_time() - time_last_query
    #if time_since_last_query < MIN_TIME_BETWEEN_QUERIES:
    #    print("#Doing it like Verizon...")
    #    time.sleep((MIN_TIME_BETWEEN_QUERIES - time_since_last_query) / 1000.0)
    print("#url=%s" % url)
    #time_last_query = current_milli_time()
    while True:
        try:
            time.sleep(MIN_TIME_BETWEEN_QUERIES / 1000.0)
            raw_response = requests.get(url).text
            #print("Querying using %s" % url)
            #raw_response = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><!DOCTYPE eSearchResult PUBLIC \"-//NLM//DTD esearch 20060628//EN\" \"https://eutils.ncbi.nlm.nih.gov/eutils/dtd/20060628/esearch.dtd\"><eSearchResult><Count>15</Count><RetMax>15</RetMax><RetStart>0</RetStart><IdList><Id>12465651</Id><Id>12159840</Id><Id>11701847</Id><Id>11262879</Id><Id>11187813</Id><Id>10755918</Id><Id>11130686</Id><Id>23105267</Id><Id>10778367</Id><Id>10766612</Id><Id>10746694</Id><Id>10746688</Id><Id>10659812</Id><Id>10646564</Id><Id>10639503</Id></IdList><TranslationSet/><TranslationStack>   <TermSet>    <Term>2000/01/01[PDAT]</Term>    <Field>PDAT</Field>    <Count>0</Count>    <Explode>N</Explode>   </TermSet>   <TermSet>    <Term>2000/12/31[PDAT]</Term>    <Field>PDAT</Field>    <Count>0</Count>    <Explode>N</Explode>   </TermSet>   <OP>RANGE</OP>   <TermSet>    <Term>genomics[Other Term]</Term>    <Field>Other Term</Field>    <Count>7402</Count>    <Explode>N</Explode>   </TermSet>   <OP>AND</OP>  </TranslationStack><QueryTranslation>2000/01/01[PDAT] : 2000/12/31[PDAT] AND genomics[Other Term]</QueryTranslation></eSearchResult>" 
            xml_response = ET.fromstring(raw_response)
            # do some sanity check
            if int(xml_response.findall(".//Count")[0].text) > int(xml_response.findall(".//RetMax")[0].text):
                raise ValueError("Query returned more results than expected!!! %s" % url)
            return [study_id.text.strip() for study_id in xml_response.findall(".//IdList/Id")]
        except:
            print("#ERROR retrieving %s, retrying in one second..." % url)
            time.sleep(1000.0)


def current_milli_time(): 
    return int(round(time.time() * 1000))

# generate combinations of 2, 3, ... N elements from all_keywords
def get_keyword_combinations(all_keywords):
    keywords = []
    for i in range(2, len(all_keywords) + 1):
        keywords.extend(itertools.combinations(all_keywords, i))
    return keywords

def do_all_queries(api_key, start_year):
    keyword_combinations = get_keyword_combinations(ALL_KEYWORDS)
    # go through the years, query
    current_date_from = date(start_year, 1, 1)
    while current_date_from < DATE_UNTIL:
        # date until the end of the year 
        current_date_until = date(current_date_from.year + 1, 1, 1) - timedelta(days=1)
        print("#Finding multiomics studies between %s and %s" % (current_date_from.strftime(DATE_FORMAT), current_date_until.strftime(DATE_FORMAT)))
        # keep track of all studies published in this same year
        multiomics_studies = set()
        for current_keyword_combination in keyword_combinations:
            ids = query_pubmed(build_query_url(api_key, current_keyword_combination, current_date_from, current_date_until))
            print ("  Keywords=%s; From=%s; To=%s; IDs=%s" % (','.join(current_keyword_combination), current_date_from.strftime(DATE_FORMAT), current_date_until.strftime(DATE_FORMAT), ','.join(ids)))
            multiomics_studies.update(ids)

        print("#From %s to %s there were %d multiomics studies" % (current_date_from.strftime(DATE_FORMAT), current_date_until.strftime(DATE_FORMAT), len(multiomics_studies)))

        # increment date to next year
        current_date_from = date(current_date_from.year + 1, 1, 1)

def parse_results(results_filepath):
    # parsed results are a map, keys are years and values is a list:
    # year -> [2-layer, 3-layer, 3+-layer]
    results = {}
    with open(results_filepath) as f:        
        for line in f.readlines():
            line = line.strip()
            if line[0] != '#':
                # Keywords=genomics,proteomics,transcriptomics,metabolomics,metagenomics; From=2004/01/01; To=2004/12/31; IDs=1,2,3
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
            # and remove them from lower lyers
            for index_lower_layer in range(index_upper_layer):
                for study_id in fields[index_upper_layer]:
                    fields[index_lower_layer].discard(study_id)
    # generate total number of studies
    print("#Year,2-omics,3-omics,>3-omics,Total")
    for year, fields in results.items():
        print("%d,%d,%d,%d,%d" % (year, len(fields[0]), len(fields[1]), len(fields[2]), len(fields[0]) + len(fields[1]) + len(fields[2])))


def main():
    parser = argparse.ArgumentParser(description='Query ubMed.')
    parser.add_argument('--api-key', metavar='api_key', type=str, help='API Key')
    parser.add_argument('--parse-results', metavar='parse_results', type=str, default=None, help='File from which results will be parsed')
    parser.add_argument('--start-year', metavar='start_year', type=int, default=2000, help='Year of first query')
    args = parser.parse_args()
    
    if args.parse_results is None:
        do_all_queries(args.api_key, args.start_year)
    else:
        parse_results(args.parse_results)

if __name__ == '__main__':
    main()
