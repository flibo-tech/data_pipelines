import warnings
warnings.filterwarnings("ignore")

import pandas as pd
from urllib import request
import yaml


def awards_scrape(titles=None):
    config = yaml.safe_load(open('./../config.yml'))
    scraped_folder = config['scraped_data_folder']

    try:
        df_scraped = pd.read_csv(scraped_folder+'awards_scraped.csv')
        df_scraped['event_year'] = df_scraped.apply(lambda row: row['event_id']+'^'+str(row['event_year']), axis=1)
        scraped = list(df_scraped['event_year'].unique())
    except:
        scraped = []

    event_ids = ['ev0000223', 'ev0000353', 'ev0000206', 'ev0000003', 'ev0000123', 'ev0000292', 'ev0000598', 'ev0002032',
                 'ev0000349', 'ev0000467', 'ev0000245', 'ev0000415', 'ev0000133', 'ev0002990', 'ev0002704', 'ev0000530',
                 'ev0000296', 'ev0000644']
    awards_distribution = []
    i = 0
    for event_id in event_ids:
        i += 1
        print(i, '/', len(event_ids), '-', event_id)
        event_url = 'https://www.imdb.com/event/' + event_id + '/overview'
        event_response = request.urlopen(event_url)
        event_response = event_response.read()
        event_response = event_response.decode().split('IMDbReactWidgets.EventHistoryWidget.push(')[1].split(');\n')[0]
        event_response = eval(event_response.replace('null', 'None').replace('true', 'True').replace('false', 'False'))[
            1]
        j = 0
        for edition in event_response['eventHistoryWidgetModel']['eventEditions']:
            j += 1
            edition_url = 'https://www.imdb.com/event/' + event_id + '/' + str(edition['year']) + '/' + str(
                edition['instanceWithinYear'])
            print(j, '/', len(event_response['eventHistoryWidgetModel']['eventEditions']), '-', edition_url)
            if event_id+'^'+str(edition['year']) not in scraped:
                response = request.urlopen(edition_url)
                response = response.read()
                response = response.decode().split('IMDbReactWidgets.NomineesWidget.push(')[1].split(');\n')[0]
                response = eval(response.replace('null', 'None').replace('true', 'True').replace('false', 'False'))[1]

                event_id = response['nomineesWidgetModel']['eventEditionSummary']['eventId']
                event_name = response['nomineesWidgetModel']['eventEditionSummary']['eventName']
                event_year = response['nomineesWidgetModel']['eventEditionSummary']['year']
                for award in response['nomineesWidgetModel']['eventEditionSummary']['awards']:
                    award_name = award['awardName']
                    for catg in award['categories']:
                        catg_name = catg['categoryName']
                        for nomination in catg['nominations']:
                            primary_nominees = [item['const'] for item in nomination['primaryNominees']]
                            secondary_nominees = [item['const'] for item in nomination['secondaryNominees']]
                            awards_distribution.append({
                                'event_id': event_id,
                                'event_name': event_name,
                                'event_year': event_year,
                                'award_name': award_name,
                                'award_category_name': catg_name,
                                'primary_nominees': primary_nominees,
                                'secondary_nominees': secondary_nominees,
                                'won': nomination['isWinner'],
                                'nomination_notes': nomination['notes']
                            })
        print('\n')

    try:
        df = pd.read_csv(scraped_folder + 'awards_scraped.csv')
    except:
        df = pd.DataFrame()

    df = pd.concat([df, pd.DataFrame(awards_distribution)], axis=0)
    df['primary_nominees'] = df['primary_nominees'].astype(str)
    df['secondary_nominees'] = df['secondary_nominees'].astype(str)
    df.drop_duplicates(inplace=True)
    df['primary_nominees'] = df['primary_nominees'].apply(lambda x: eval(x))
    df['secondary_nominees'] = df['secondary_nominees'].apply(lambda x: eval(x))
    df.to_csv(scraped_folder+'awards_scraped.csv', index=False)


    #################################################################################################################################################################################

                                        #To clean above scraped data

    #################################################################################################################################################################################

    print('Cleaning scraped data...')
    df_awards_master = pd.read_csv('./../resources/awards_master.csv', sep='^')

    df = pd.read_csv(scraped_folder+'awards_scraped.csv')
    df['award_category_name'] = df['award_category_name'].apply(lambda x: x.strip() if type(x)==str else x)

    df = pd.merge(df, df_awards_master[['event_id', 'award_category_name', 'award_id']], on=['event_id', 'award_category_name'], how='left')
    del df['award_category_name']
    del df['award_name']
    del df['event_id']

    print('Combining primary & secondary nominations...')


    def mix(a,b):
        if not a:
            a = [None]
        if not b:
            b = [None]

        return [[x,y] for x in a for y in b]

    df['primary_secondary'] = df.apply(lambda row: mix(eval(row['primary_nominees']),
                                                       eval(row['secondary_nominees'])), axis=1)

    print('Preparing dicts...')


    def dicts_for_df(row):
        list_o_dict = []
        items = row['primary_secondary']
        for item in items:
            if item:
                temp_dict = {}

                temp_dict['imdb_content_id'] = None
                temp_dict['imdb_person_id'] = None
                if str(item[0]).count('tt') == 1:
                    temp_dict['imdb_content_id'] = item[0]
                elif str(item[0]).count('nm') == 1:
                    temp_dict['imdb_person_id'] = item[0]

                if str(item[1]).count('tt') == 1:
                    temp_dict['imdb_content_id'] = item[1]
                elif str(item[1]).count('nm') == 1:
                    temp_dict['imdb_person_id'] = item[1]

                temp_dict['award_id'] = row['award_id']
                temp_dict['event_year'] = row['event_year']
                temp_dict['nomination_notes'] = row['nomination_notes']
                temp_dict['won'] = row['won']

                list_o_dict.append(temp_dict)
        return list_o_dict
    df['dict_for_df'] = df.apply(dicts_for_df, axis=1)

    print('Aggregating dicts...')
    df_awards_distribution = pd.DataFrame(df['dict_for_df'].sum())
    df_awards_distribution.drop_duplicates(inplace=True)
    df_awards_distribution[
        pd.notnull(df_awards_distribution['award_id'])
        &
        (pd.notnull(df_awards_distribution['imdb_content_id']) | pd.notnull(df_awards_distribution['imdb_person_id']))
    ].to_csv(scraped_folder+'cleaned_awards_scraped.csv', index=False)

    return True
