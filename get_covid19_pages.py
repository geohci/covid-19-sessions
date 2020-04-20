import argparse
import requests
import time

import pandas as pd

# Covid-19 article lists prepared by Diego
COVID_ENDPOINT = 'https://covid-data.wmflabs.org/pagesNoHumans?data=True'
KEYS_TO_KEEP = ['page', 'project', 'Instace_Of_Label']
PAGES_TO_ADD = ["Coronavirus"]  # this generic article receives most of the early Covid-19-related pageviews in January

# Gather QID for a title to get its sitelinks
QID_ENDPONT = "https://en.wikipedia.org/w/api.php?"
QID_PARAMS = {'action': 'query',
              'prop': 'pageprops',
              'format': 'json',
              'ppprop': 'wikibase_item',
              'titles': '|'.join(PAGES_TO_ADD)}

# Gather article titles in all languages
SITELINKS_ENDPOINT = 'https://www.wikidata.org/w/api.php?'
SITELINK_PARAMS = {'action': 'wbgetentities',
                   'format': 'json',
                   'props': 'sitelinks/urls'}

# Gather page IDs (stable identifier) for a given wiki + title
PAGEID_ENDPOINT = 'https://{0}.org/w/api.php?'
PAGEID_PARAMS = {'action': 'query',
                 'prop': 'info',
                 'format': 'json',
                 'redirects': True}

def url_to_project(url):
    """https://en.wikipedia.org/wiki/<page-title> -> en.wikipedia"""
    project_start_idx = len('https://')
    project_end_idx = url.index('.org')
    return url[project_start_idx:project_end_idx]

def add_missing_articles(filtered_pages):
    """Add any articles that are missed in Diego's identification process."""
    additional_rows = []
    with requests.session() as session:
        # get QIDs for all enwiki titles provided
        res = session.get(url=QID_ENDPONT, params=QID_PARAMS).json()
        qids = {}
        for r in res['query']['pages']:
            title = res['query']['pages'][r]['title']
            qid = res['query']['pages'][r]['pageprops']['wikibase_item']
            qids[title] = qid
        # for each qid, get all wikipedia sitelinks
        for p in PAGES_TO_ADD:
            qid = qids[p]
            params = SITELINK_PARAMS.copy()
            params['ids'] = qid
            sitelinks = session.get(url=SITELINKS_ENDPOINT, params=params).json()
            for s in sitelinks['entities'][qid]['sitelinks']:
                page_title = sitelinks['entities'][qid]['sitelinks'][s]['title']
                project = url_to_project(sitelinks['entities'][qid]['sitelinks'][s]['url'])
                if 'wikipedia' in project:
                    p31_label = {'Manually added'}  # I could populate this correctly but this is more informative
                    additional_rows.append({'page_title':page_title, 'project':project, 'P31-Label':p31_label})
    additional_df = pd.DataFrame(additional_rows)
    print("Adding {0} sitelinks from {1} pages:".format(len(additional_rows), len(PAGES_TO_ADD)))
    print(additional_df)  # debugging
    filtered_pages = filtered_pages.append(additional_df)
    return filtered_pages


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_tsv", help="TSV file to write preprocessed page data.")
    args = parser.parse_args()

    # get all Covid-19-related articles per Diego's criteria (this does not include instance-of:human articles)
    with requests.session() as session:
        pages = session.get(url=COVID_ENDPOINT).json()

    # convert text JSON to JSON objects
    data = []
    for project in pages:
        for p in pages[project]:
            j = eval(p.replace('null', 'None'))
            data.append({k:j[k] for k in KEYS_TO_KEEP})
    for d in data:
        d['page'] = d['page'].replace('\\', '')

    all_pages = pd.DataFrame(data)
    filtered_pages = filter_pages(all_pages)
    filtered_pages = add_missing_articles(filtered_pages)
    projects = set(filtered_pages['project'])
    titles_to_pageids = {}
    # gather pageIDs for each title
    for p in projects:
        api_endpoint = PAGEID_ENDPOINT.format(p)
        titles_to_pageids[p] = {}
        print("Processing: {0}".format(p))
        for title_batch in chunk(filtered_pages[filtered_pages['project'] == p]['page_title']):
            params = PAGEID_PARAMS.copy()
            params['titles'] = '|'.join(title_batch)
            try:
                result = session.get(url=api_endpoint, params=params)
            except Exception:
                print('API Error: {1}'.format(p, params['titles']))
                continue
            try:
                result = result.json()
            except Exception:
                print("JSON error: {0}".format(result))
                continue
            redirects = {}
            if 'redirects' in result['query']:
                for r in result['query']['redirects']:
                    redirects[r['to']] = r['from']
            for pageid in result['query']['pages']:
                if 'missing' in result['query']['pages'][pageid]:
                    print("Missing: {0}".format(result['query']['pages'][pageid]))
                    continue
                try:
                    title = result['query']['pages'][pageid]['title']
                    if title in redirects:
                        title = redirects[title]
                    # remove remaining categories / templates
                    if result['query']['pages'][pageid]['ns'] != 0:
                        print("Skipping https://{0}.org/wiki/{1}".format(p, title))
                        continue
                    titles_to_pageids[p][title] = pageid
                except KeyError:
                    print("Bad data: {0}".format(result['query']['pages'][pageid]))
            # be nice to the API
            time.sleep(1)

    filtered_pages['pageid'] = filtered_pages.apply(lambda x: titles_to_pageids.get(x['project'], {}).get(x['page_title'], None), axis=1)
    print("Removing {0} pages: {1}".format(sum(filtered_pages['pageid'].isnull()),
                                           filtered_pages[filtered_pages['pageid'].isnull()]))
    filtered_pages = filtered_pages[~filtered_pages['pageid'].isnull()]
    filtered_pages.to_csv(args.output_tsv, sep='\t', header=True, index=False)


def chunk(pagetitles, batch_size=20):
    """Batch pageIDS into sets of 20 for the Mediawiki API.

    Technically this can be 50, but sometimes the URL is too long and causes an error then so I limit to 20.
    """
    for i in range(0, len(pagetitles), batch_size):
        yield [str(p) for p in pagetitles[i:i+batch_size]]


def filter_pages(df):
    """Reformat and remove some extraneous pages to reduce future API calls (the rest will be filtered out later):
        * non-Wikipedia articles
        * categories / templates -- i.e. non-namespace-1 pages
    """
    df = df.copy()
    print("Covid-19 pages: starting with {0} rows".format(len(df)))
    # remove non-wiki items
    df = df[df['project'].apply(lambda x: 'wikipedia' in str(x))]
    print("{0} rows after removing non-wikipedia items.".format(len(df)))
    # remove categories/templates
    df = df[df['page'].apply(lambda x: not str(x).startswith('Category:') and not str(x).startswith('Template:'))]
    print("{0} rows after removing Categories/Templates.".format(len(df)))
    # reformat columns
    columns_to_keep = {'page':'page_title', 'project':'project', 'Instace_Of_Label':'P31-Label'}
    df = df[list(columns_to_keep)]
    df.rename(columns=columns_to_keep, inplace=True)
    df = df.groupby(['page_title', 'project'])['P31-Label'].apply(lambda x: set(x)).reset_index()
    return df


if __name__ == "__main__":
    main()