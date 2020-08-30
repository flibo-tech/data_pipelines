import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import sqlalchemy

engine = sqlalchemy.create_engine('postgres://postgres:openit123@localhost:5432/flibo')

query = """
           select content_id, is_adult, language, release_year as year, runtime, genres, imdb_score, episodes, seasons,
                  num_votes, nudity, violence, profanity, drugs, intense_scenes, award_wins, award_nominations, avg_age_limit
           from app.content_details
        """
df_contents = pd.read_sql(query, engine)

credit_catgs = ['Cast', 'Cinematography by', 'Directed by', 'Film Editing by', 'Music by', 'Writing Credits']
for catg in credit_catgs:
    print(catg)
    query = """
               select content_id, cum_experience_content, cum_experience_years,
                      content_done_in_the_catg, years_in_the_catg, num_votes, imdb_score
               from app.content_crew
               where credit_category = '""" + catg + """'
               and credit_order <= 3
               and cast(content_id as varchar) like '1%%'
               union all
               select content_id, cum_experience_content, cum_experience_years,
                      content_done_in_the_catg, years_in_the_catg, num_votes, imdb_score
               from app.content_crew
               where credit_category = '""" + catg + """'
               and credit_order <= 6
               and cast(content_id as varchar) like '2%%'
            """
    df = pd.read_sql(query, engine)
    df = df.groupby('content_id')['cum_experience_content', 'cum_experience_years', 'content_done_in_the_catg', 'years_in_the_catg',
                                'num_votes', 'imdb_score'].mean().reset_index()
    df.columns = df.columns.map(lambda x: str(x) + '_' + catg.lower().replace(' ', '_') if x != 'content_id' else x)
    df_contents = pd.merge(df_contents, df, how='left', on='content_id')

print('\n')
columns_to_exclude_data_fill = ['content_id', 'is_adult', 'year', 'runtime', 'genres', 'language']
for column in list(df_contents.columns):
    print('Filling missing data for', column)
    if column not in columns_to_exclude_data_fill:
        if column in ['award_wins', 'award_nominations']:
            df_contents[column][pd.isnull(df_contents[column])] = 0
        else:
            df_contents[column][pd.isnull(df_contents[column])] = df_contents[column].mean()
        df_contents[column] = df_contents[column].astype(float)
df_contents['award_nominations'] = df_contents.apply(lambda row: row['award_wins'] + row['award_nominations'], axis=1)

def clean_array(array):
    output = []
    if type(array) == list:
        for item in array:
            output.append(item.replace("'", '').strip())
        return output
    else:
        return None

df_contents['genres'] = df_contents['genres'].apply(clean_array)
df_contents = df_contents[pd.notnull(df_contents['genres'])]

df_contents['language'] = df_contents['language'].apply(clean_array)
df_contents = df_contents[pd.notnull(df_contents['language'])]

df_contents.to_csv('/tmp/full_data.csv', index=False, sep='^')
