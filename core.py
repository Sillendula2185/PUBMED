from Bio import Entrez
import re
import xmltodict
import logging

logging.basicConfig(filename='pubmed.log', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


def fetch_pubmedid_details(pubmed_id):
    def remove_html_tags(text):
        try:
            pattern = re.compile('<.*?>')
            text = re.sub(pattern, '', text)
            text = text.replace("\u2009", " ")
            return text
        except Exception as e:
            return " "

    try:
        # Construct the PubMed query
        handle = Entrez.efetch(db='pubmed', id=pubmed_id)

        # Read and parse the XML response
        record = Entrez.read(handle)

        # Extract abstracts from the parsed record
        abstracts = []
        try:
            title = record['PubmedArticle'][0]['MedlineCitation']['Article']['Journal']['Title']
        except Exception as e:
            title = 'No Title'
        try:
            authors = [author["ForeName"] + " " + author["LastName"]
                       for author in record['PubmedArticle'][0]['MedlineCitation']['Article']['AuthorList']]
        except Exception as e:
            authors = []
        try:
            keywords = [kw for kw in record['PubmedArticle'][0]['MedlineCitation']["KeywordList"][0]]
        except Exception as e:
            keywords = []
        authors = " ,".join(authors)
        keywords = " ,".join(keywords)
        for article in record['PubmedArticle']:
            if 'MedlineCitation' in article:
                citation = article['MedlineCitation']
                if 'Article' in citation:
                    article_info = citation['Article']
                    if 'Abstract' in article_info:
                        abstract = article_info['Abstract']['AbstractText']
                        abstracts.extend(list(map(lambda x: remove_html_tags(x), abstract)))
        if not abstracts:
            abstracts.append("")

        return (pubmed_id, title, abstracts[0], authors, keywords)
    except Exception as e:
        return None


def search_pubmed_term(search_term):
    try:
        # Set your email address for Entrez
        Entrez.email = "dummy@yahoo.com"
        handle = Entrez.esearch(db="pubmed", term=search_term, retmax=100000)
        record = Entrez.read(handle)
        return record["IdList"]
    except Exception as e:
        return []


def pubmed_search(search_term, min_date=None, max_date=None):
    try:
        # Set your email address for Entrez
        Entrez.email = "dummy@yahoo.com"
        search_results = Entrez.read(
            Entrez.esearch(
                db="pubmed", term=search_term, datetype="pdat", usehistory="y",
                mindate=min_date, maxdate=max_date
            )
        )
        count = int(search_results["Count"])
        return count, search_results
    except Exception as e:
        return []


def get_PubmedBookArticle_details(article, search_term):
    parsed_result = dict()
    parsed_result['SEARCH TERM'] = search_term

    try:
        parsed_result['TITLE'] = article['BookDocument']['ArticleTitle']['#text']
    except:
        parsed_result['TITLE'] = " "

    try:
        parsed_result['PMID'] = article['BookDocument']['PMID']['#text']
    except:
        parsed_result['PMID'] = " "

    try:
        parsed_result['PUBDATE'] = article['BookDocument']['Book']['PubDate']['Year']
    except:
        parsed_result['PUBDATE'] = " "

    try:
        parsed_result['ABSTRACT'] = article['BookDocument']['Abstract']['AbstractText']
    except:
        parsed_result['ABSTRACT'] = " "

    try:
        if "#text" in article['BookDocument']['Abstract']['AbstractText']:
            try:
                parsed_result['ABSTRACT'] = article['BookDocument']['Abstract']['AbstractText']["#text"]
            except Exception as e:
                parsed_result['ABSTRACT'] = " "

        if isinstance(article['BookDocument']['Abstract']['AbstractText'],list):
            try:
                for abstract in article['BookDocument']['Abstract']['AbstractText']:
                    parsed_result['ABSTRACT'] += abstract["#text"]
            except Exception as e:
                parsed_result['ABSTRACT'] = " "
    except:
        parsed_result['ABSTRACT'] = " "

    try:
        parsed_result['AUTHOR'] = ",".join([author["LastName"] + " " + author["ForeName"]
                                            for author in
                                            article['BookDocument']['AuthorList']['Author']])
    except:
        parsed_result['AUTHOR'] = " "

    parsed_result['KEYWORDS'] = " "

    parsed_result['PMC'] = " "

    return parsed_result


def get_PubmedArticle_details(article, search_term):
    parsed_result = dict()
    parsed_result['SEARCH TERM'] = search_term

    try:
        parsed_result['TITLE'] = article['MedlineCitation']['Article']['Journal']['Title']
    except:
        parsed_result['TITLE'] = " "

    try:
        parsed_result['PMID'] = article['MedlineCitation']['PMID']["#text"]
    except:
        parsed_result['PMID'] = " "

    try:
        parsed_result['PUBDATE'] = article['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate']['Year']
    except:
        parsed_result['PUBDATE'] = " "

    try:
        parsed_result['ABSTRACT'] = article['MedlineCitation']['Article']['Abstract']['AbstractText']["#text"]
    except:
        parsed_result['ABSTRACT'] = None

    try:
        if parsed_result['ABSTRACT'] is None:
            try:
                parsed_result['ABSTRACT'] = " "
                for abstract in article['MedlineCitation']['Article']['Abstract']['AbstractText']:
                    parsed_result['ABSTRACT'] += abstract["#text"]
            except Exception as e:
                parsed_result['ABSTRACT'] = None

        if parsed_result['ABSTRACT'] is None:
            try:
                parsed_result['ABSTRACT'] = article['MedlineCitation']['Article']['Abstract']
            except Exception as e:
                parsed_result['ABSTRACT'] = "  "

        if "AbstractText" in parsed_result['ABSTRACT']:
            try:
                parsed_result['ABSTRACT'] = parsed_result['ABSTRACT']["AbstractText"]
            except Exception as e:
                parsed_result['ABSTRACT'] = "  "

    except Exception as e:
        parsed_result['ABSTRACT'] = "  "

    try:
        parsed_result['AUTHOR'] = ",".join([author["LastName"] + " " + author["ForeName"]
                                            for author in
                                            article['MedlineCitation']['Article']['AuthorList']['Author']])
    except:
        parsed_result['AUTHOR'] = " "

    try:
        parsed_result['KEYWORDS'] = ",".join([keyword["#text"]
                                              for keyword in article['MedlineCitation']["KeywordList"]["Keyword"]])
    except:
        parsed_result['KEYWORDS'] = " "

    try:
        parsed_result['PMC'] = ",".join([keyword["#text"]
                                         for keyword in article['PubmedData']["ArticleIdList"]["ArticleId"]
                                         if keyword['@IdType'] == 'pmc'])
    except:
        parsed_result['PMC'] = " "

    return parsed_result


def pubmed_batch_download(search_term, search_results, batch_size, start=0):
    result = []
    try:
        stream = Entrez.efetch(
            db="pubmed",
            rettype="medline",
            retmode="xml",
            retstart=start,
            retmax=batch_size,
            webenv=search_results["WebEnv"],
            query_key=search_results["QueryKey"],
        )
        data = stream.read()
        stream.close()
        # Parse the XML data into a dictionary
        parsed_data = xmltodict.parse(data)

        try:
            if 'PubmedArticle' in parsed_data["PubmedArticleSet"]:
                articles = parsed_data["PubmedArticleSet"]['PubmedArticle']
                if isinstance(articles,dict):
                    result.append(get_PubmedArticle_details(articles, search_term))
                if isinstance(articles, list):
                    for article in articles:
                        result.append(get_PubmedArticle_details(article, search_term))
        except Exception as e:
            logging.error(f"Error fetch pubmed article {e} ")

        try:
            if 'PubmedBookArticle' in parsed_data["PubmedArticleSet"]:
                articles = parsed_data["PubmedArticleSet"]['PubmedBookArticle']
                # logging.info(f"Article Type: {type(articles)}")
                if isinstance(articles,dict):
                    # logging.info(f"Processing Dict Article Type: {type(articles)}")
                    result.append(get_PubmedBookArticle_details(articles, search_term))
                if isinstance(articles,list):
                    # logging.info(f"Processing List Article Type: {type(articles)}")
                    for article in articles:
                        result.append(get_PubmedBookArticle_details(article, search_term))
        except Exception as e:
            logging.error(f"Error fetch pubmed book article {e} ")

        return result
    except Exception as e:
        logging.error(f"Exception at Core {e}")
        return result
