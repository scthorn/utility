# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 11:51:23 2024

@author: sct8690
"""

import json, urllib, pandas, os
from urllib.parse import quote

# Set the working directory to the location of this script (note that you have to run the whole script to do this)
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

#list of keywords

kw_df = pandas.read_csv('../keywords.csv')
kwlist = kw_df['Keyword'].tolist()
encoded_kwlist = [quote(kw) for kw in kwlist]

#date parameters as strings
start_year = '1990'
end_year = '1999'

#construct api call with placeholder for cursor
kwstring = "title_and_abstract.search:" + '|'.join(encoded_kwlist)
datestring = 'publication_year:' + start_year + '-' + end_year
cursor = "*"

apiplaceholder = ('https://api.openalex.org/works?filter=type:article,{},{}&per-page=200'.format(datestring,kwstring)) + "&cursor={}"



#set up an empty list to hold results
data_list = []

#we have to loop through and do multiple api calls because the max number of works per call is 200. 
#initially cursor is set to *, then each call gives a cursor parameter telling us where to start the next call 

while cursor:
    #insert the cursor value into the apicall url
    apicall = apiplaceholder.format(cursor)
    #do the apicall
    messyworks = urllib.request.urlopen(apicall)
    #  Convert to json
    worksplus = json.load(messyworks)
    works = worksplus['results']
       
    #   add results to a dataframe
    for work in works:
        
        #first get pdf url for open access articles if available
        best_oa_location = work.get('best_oa_location')  # Accessing best_oa_location
        if best_oa_location is not None:
            is_oa = best_oa_location.get('is_oa', None)
            if is_oa:
                pdf_url = best_oa_location.get('pdf_url', None)  # Retrieve pdf_url if is_oa is True
                landing_page_url = best_oa_location.get('landing_page_url', None)
            else:
                pdf_url = None
                landing_page_url = None
        else:
            is_oa = None
            pdf_url = None
            landing_page_url = None
    
        # Extract domain and field information from primary_topic
        primary_topic = work.get('primary_topic')
        if isinstance(primary_topic, dict):
            domain_info = primary_topic.get('domain')
            field_info = primary_topic.get('field')
            if isinstance(domain_info, dict):
                domain_display_name = domain_info.get('display_name')
            else:
                domain_display_name = None
            if isinstance(field_info, dict):
                field_display_name = field_info.get('display_name')
            else:
                field_display_name = None
        else:
            domain_display_name = None
            field_display_name = None
            
        #all the other metadata is pretty straightforward so just dump everything in a list
        d = {
            'abstract': work.get('abstract_inverted_index', None), 
            'title': work.get('title', None), 
            'is_oa': is_oa,
            'oa_pdf_url': pdf_url,
            'oa_landing_page_url': landing_page_url,
            'publication_date': work.get('publication_date', None),
            'publication_year': work.get('publication_year', None),
            'biblio': work.get('biblio', None),
            'OAID': work.get('id', None), 
            'doi': work.get('doi', None), 
            'ngrams_url': work.get('ngrams_url', None), 
            'topics': work.get('topics', None),
            'concepts': work.get('concepts', None),
            'domain': domain_display_name, 
            'field': field_display_name
        }
        data_list.append(d)
    
    big_df = pandas.DataFrame(data_list)
    #update the cursor value
    cursor = worksplus['meta']['next_cursor']
    print("\n" + "hey" + cursor)



#drop duplicates
big_df = big_df.drop_duplicates(subset='OAID', keep='first')

####Deal with dirty abstracts
#function to check for advertisements in abstract field
def check_if_advertisement(dictionary):
    if isinstance(dictionary, dict) and dictionary:
        first_key = next(iter(dictionary.keys()))
        return first_key.lower() == 'advertisement'
    else:
        return False


#function to check for nonsense coming from Wiley Online Library in abstract field
def check_if_wiley(dictionary):
    if isinstance(dictionary, dict) and dictionary:
        words_to_check = {"Wiley", "Online", "Library"}  
        index_words = set(dictionary.keys()) 
        return all(word in index_words for word in words_to_check)
    else:
        return False
    
#function to check for metadata in abstract field
def check_if_metadata(dictionary):
    if isinstance(dictionary, dict) and dictionary:
        words_to_check = {"Volume", "Issue", "Number", "Vol.", "No.", "pp.", "Pp.", "PP.", "doi", "DOI", "Citations"}  
        index_words = dictionary.keys()  
        for word1 in words_to_check: #any two of these words is a good indication that this is just metadata
            for word2 in words_to_check:
                if word1 != word2 and (word1 in index_words) and (word2 in index_words):
                    return True
        return False
    else:
        return False

#function to check for questionably short or long abstract indices
def check_reasonable_length(dictionary):
    if isinstance(dictionary, dict) and dictionary:
        if len(dictionary) > 15 and len(dictionary) < 800:
            return True
        else:
            return False
    else:
        return False
            
        
#mask for missing or faulty abstracts
has_legit_abstract = (
    (big_df['abstract'] != '') & 
    (big_df['abstract'].notnull()) & 
    (~big_df['abstract'].apply(check_if_advertisement)) & 
    (~big_df['abstract'].apply(check_if_wiley)) & 
    (~big_df['abstract'].apply(check_if_metadata)) & 
    (big_df['abstract'].apply(check_reasonable_length))
)

#distribution by year and abstract coverage
total_pubs = big_df['publication_year'].value_counts()

pubs_with_abstracts = big_df.loc[has_legit_abstract, 'publication_year'].value_counts()

percentage_with_abstracts = (pubs_with_abstracts / total_pubs) * 100


# filter to articles with abstracts
big_df = big_df.loc[has_legit_abstract]


#reconstruct abstracts from inverted index

def reconstruct_abstract(inverted_abstract):
    word_index = [(word, index) for word, indices in inverted_abstract.items() for index in indices]
    word_index = sorted(word_index,key = lambda x : x[1])
    abstract_string = ' '.join(pair[0] for pair in word_index)
    return abstract_string

big_df['abstract'] = big_df['abstract'].apply(reconstruct_abstract)


#save results
filepath = 'output-data/'
filename = start_year + '-' + end_year + ".csv"
big_df.to_csv(filepath + filename)


