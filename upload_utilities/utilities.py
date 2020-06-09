import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import yaml
import sqlalchemy
from nltk.corpus import wordnet, stopwords
from sklearn.preprocessing import MinMaxScaler
from sklearn.neighbors import KNeighborsRegressor
from sklearn.feature_selection import VarianceThreshold
import numpy as np
from multiprocessing import Pool
import os


config = yaml.safe_load(open('./../config.yml'))
data_folder = config['movies_data_folder']
streaming_sources_folder = config['streaming_sources']
to_upload_folder = config['to_upload']
upload_resources_folder = config['upload_resources']
movies_data_folder = config['movies_data_folder']
tv_series_data_folder = config['tv_series_data_folder']
spot_instance_scraped_data_folder = config['spot_instance_scraped_data_folder']+'\\scraped\\'


def parallelize_dataframe(df, func, n_cores=config['algo']['vCPU']):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def find_synonyms(phrase):
    words = [word for word in phrase.split(' ') if word not in stop_words]
    synonyms = []
    for word in words:
        syns = wordnet.synsets(word)
        syns = [l.name() for s in syns for l in s.lemmas()]
        if syns:
            synonyms = synonyms + syns
        else:
            synonyms.append(word)
    synonyms = list(set(synonyms))

    return synonyms


def apply_find_synonyms(df):
    df['synonym_tags'] = df['tag'].apply(find_synonyms)
    return df


must_haves = ['Animation', 'Biography', 'Comedy', 'Documentary', 'Drama',
              'Fantasy', 'Film-Noir', 'History', 'Horror', 'Music',
              'Musical', 'News', 'Romance', 'Sport', 'War', 'Western', 'Talk-Show', 'Short', 'Game-Show',
              'Reality-TV', 'Mystery', 'Sci-Fi']
genre_groups = {'Action': ['Action', 'Drama', 'Adventure', 'Crime', 'Mystery', 'Sci-Fi', 'Thriller'],
                'Adult': ['Adult', 'Drama'],
                'Adventure': ['Adventure', 'Drama', 'Action', 'Crime', 'Mystery', 'Sci-Fi', 'Thriller'],
                'Animation': ['Animation', 'Drama'],
                'Biography': ['Biography', 'Drama'],
                'Comedy': ['Comedy', 'Drama'],
                'Crime': ['Crime', 'Drama', 'Action', 'Adventure', 'Mystery', 'Sci-Fi', 'Thriller'],
                'Drama': ['Drama', 'Drama'],
                'Family': ['Family', 'Drama'],
                'Fantasy': ['Fantasy', 'Drama'],
                'Film-Noir': ['Film-Noir', 'Drama'],
                'History': ['History', 'Drama'],
                'Horror': ['Horror', 'Drama'],
                'Music': ['Music', 'Drama'],
                'Musical': ['Musical', 'Drama'],
                'Documentary': ['Documentary', 'Drama'],
                'News': ['News', 'Drama'],
                'Romance': ['Romance', 'Drama'],
                'Sport': ['Sport', 'Drama'],
                'Mystery': ['Mystery', 'Drama', 'Adventure', 'Crime', 'Action', 'Sci-Fi', 'Thriller'],
                'Sci-Fi': ['Sci-Fi', 'Drama', 'Adventure', 'Crime', 'Mystery', 'Action', 'Thriller'],
                'Thriller': ['Thriller', 'Drama', 'Adventure', 'Crime', 'Mystery', 'Sci-Fi', 'Action'],
                'War': ['War', 'Drama', 'History'],
                'Western': ['Western', 'Drama', 'Adventure', 'Action', 'Crime', 'Mystery', 'Thriller'],
                'Talk-Show': ['Talk-Show', 'Drama'],
                'Short': ['Short', 'Drama'],
                'Game-Show': ['Game-Show', 'Drama', 'Reality-TV'],
            'Reality-TV': ['Game-Show', 'Drama', 'Reality-TV', 'Talk-Show']}


def common_contents(row):
    content = str(row['content_id'])[0]
    synonym_tags = row['synonym_tags']
    genres = row['genres']
    languages = row['language']
    contents = []

    current_must_haves = []
    common_genres = []
    if str(genres).lower() not in ['nan', 'none']:
        current_must_haves = [genre for genre in genres if genre in must_haves]
        common_genres = []
        for genre in genres:
            common_genres.extend(genre_groups[genre])
        common_genres = list(set(common_genres))

    def filter_on_genres(row_genres):
        must_have_check = True
        common_genres_check = True
        for genre in current_must_haves:
            if genre not in row_genres:
                must_have_check = False
        for genre in row_genres:
            if genre not in common_genres:
                common_genres_check = False
        if common_genres_check and must_have_check:
            return True
        else:
            return False

    df_temp = df_contents[df_contents['content_id'].astype(str).str.startswith(content)]
    df_temp['check'] = df_temp['genres'].apply(lambda x: filter_on_genres(x) if str(x).lower() not in ['nan', 'none'] else False)
    df_temp= df_temp[df_temp['check']]
    if not df_temp.empty:
        del df_temp['check']

        def filter_on_language(row_languages):
            check = False
            if len(set(languages).intersection(row_languages)) != 0:
                check = True
            return check

        df_temp['check'] = df_temp['language'].apply(lambda x: filter_on_language(x) if str(x).lower() not in ['nan', 'none'] else False)
        df_temp = df_temp[df_temp['check']]
        if not df_temp.empty:
            del df_temp['check']

            df_temp.reset_index(inplace=True, drop=True)

            common_count = 500 if content == '1' else 100

            df_temp['common'] = df_temp['synonym_tags'].apply(lambda x: len(set(x).intersection(synonym_tags)))
            contents = list(df_temp.sort_values('common', ascending=False)['content_id'].head(common_count).unique())
    return contents


def apply_common_contents(df):
    df['common_contents'] = df.apply(lambda row: common_contents(row), axis=1)
    return df


def content_search():
    try:
        df_movies_search = pd.read_csv(to_upload_folder + 'movies.csv', sep='^')
        df_movies_search = df_movies_search[['content_id',  'title', 'poster', 'num_votes']]
    except:
        df_movies_search = pd.DataFrame(columns=['content_id',  'title', 'poster', 'num_votes'])
    try:
        df_tv_series_search = pd.read_csv(to_upload_folder + 'tv_series.csv', sep='^')
        df_tv_series_search = df_tv_series_search[['content_id', 'title', 'poster', 'num_votes']]
    except:
        df_tv_series_search = pd.DataFrame(columns=['content_id',  'title', 'poster', 'num_votes'])

    engine = sqlalchemy.create_engine('postgres://' + config['sql']['user'] + ':' +
                                                      config['sql']['password'] + '@' +
                                                      config['sql']['host'] + ':' +
                                                      str(config['sql']['port']) + '/' +
                                                      config['sql']['db'])

    conn = engine.connect()
    df_db_content = pd.read_sql("""
                               select content_id, title, poster, num_votes
                               from """ + config['sql']['schema'] + """.content_details
                               """, con=conn)

    df_content_search = pd.concat([df_movies_search, df_tv_series_search, df_db_content], axis=0)
    df_content_search.drop_duplicates(['content_id'], inplace=True)
    df_content_search.sort_values('num_votes', ascending=False, inplace=True)

    df_content_search.rename(columns={
        'content_id': 'subject_id',
        'title': 'subject',
        'poster': 'image',
        'num_votes': 'popularity'
    }, inplace=True)
    df_content_search['type'] = 'content'

    conn.close()

    return df_content_search


def artists_to_contents():
    df_artists_search = pd.read_csv(to_upload_folder + 'artists.csv', sep='^')
    df_artists_search = df_artists_search[['person_id', 'name', 'picture']]

    try:
        df_movies = pd.read_csv(to_upload_folder + 'movies.csv', sep='^')
        df_movies = df_movies[['content_id', 'num_votes']]
    except:
        df_movies = pd.DataFrame(columns=['content_id', 'num_votes'], dtype=float)
    try:
        df_tv_series = pd.read_csv(to_upload_folder + 'tv_series.csv', sep='^')
        df_tv_series = df_tv_series[['content_id', 'num_votes']]
    except:
        df_tv_series = pd.DataFrame(columns=['content_id', 'num_votes'], dtype=float)

    df_content = pd.concat([df_movies, df_tv_series], axis=0)

    df_crew = pd.read_csv(to_upload_folder + 'content_crew.csv', sep='^')
    df_crew = df_crew[['person_id', 'content_id']][df_crew['credit_order']<=5]

    df_crew = pd.merge(df_crew, df_content, how='left', on='content_id')
    df_crew['num_votes'] = df_crew['num_votes'].astype(float)

    df_artists_search = pd.merge(df_artists_search,
                                 df_crew.groupby('person_id').num_votes.mean().reset_index(),
                                 how='left',
                                 on='person_id')

    engine = sqlalchemy.create_engine('postgres://' + config['sql']['user'] + ':' +
                                                      config['sql']['password'] + '@' +
                                                      config['sql']['host'] + ':' +
                                                      str(config['sql']['port']) + '/' +
                                                      config['sql']['db'])

    conn = engine.connect()

    df_db_artists = pd.read_sql("""
                                select t4.person_id, name, picture, num_votes
                                from (
                                        select person_id, avg(num_votes) as num_votes
                                        from (
                                              select person_id, t2.num_votes
                                              from """ + config['sql']['schema'] + """.content_crew t1
                                              left join """ + config['sql']['schema'] + """.content_details t2
                                              on t1.content_id = t2.content_id
                                              where t1.credit_order <=5
                                              ) t3
                                        group by person_id
                                    ) t4
                                left join """ + config['sql']['schema'] + """.artists t5
                                on t4.person_id = t5.person_id
                                """, con=conn)

    df_artists_search = pd.concat([df_db_artists, df_artists_search], axis=0)
    df_artists_search.sort_values('num_votes', ascending=False, inplace=True)
    df_artists_search.drop_duplicates(inplace=True)
    df_artists_search['name'][pd.isnull(df_artists_search['name'])] = '0'
    df_artists_search['picture'][pd.isnull(df_artists_search['picture'])] = '0'
    df_artists_search = df_artists_search.groupby('person_id').agg({
                                                                        'name': 'max',
                                                                        'picture': 'max',
                                                                        'num_votes': 'mean'
                                                                    }).reset_index()

    df_artists_search['name'][df_artists_search['name']=='0'] = None
    df_artists_search['picture'][df_artists_search['picture']=='0'] = None

    del df_crew['num_votes']

    df_db_crew = pd.read_sql("""
                                select person_id, content_id
                                from """ + config['sql']['schema'] + """.content_crew
                                where credit_order <=5
                                """, con=conn)

    df_crew = pd.concat([df_db_crew, df_crew], axis=0)
    df_crew.drop_duplicates(inplace=True)

    int_columns = ['content_id']
    for col in int_columns:
        df_crew[col][pd.notnull(df_crew[col])] = df_crew[col][pd.notnull(df_crew[col])].apply(
            lambda x: eval(str(x).replace(',', '')))
        df_crew[col][pd.notnull(df_crew[col])] = df_crew[col][pd.notnull(df_crew[col])].apply(
            lambda x: '{:.0f}'.format(x))

    df_crew = df_crew.groupby('person_id')['content_id'].apply(list).reset_index()
    df_crew['content_id'] = df_crew['content_id'].apply(lambda x: list(set(x)))

    df_artists_search = pd.merge(df_artists_search, df_crew, how='left', on='person_id')

    df_artists_search.rename(columns={
        'person_id': 'subject_id',
        'name': 'subject',
        'picture': 'image',
        'num_votes': 'popularity',
        'content_id': 'contents'
    }, inplace=True)
    df_artists_search['type'] = 'artist'
    df_artists_search.sort_values('popularity', ascending=False, inplace=True)

    df_artists_search.to_csv(upload_resources_folder+'artists_to_contents.csv', index=False)

    conn.close()

    return True


def tags_to_contents():
    df_tags = pd.read_csv(to_upload_folder + 'content_tags.csv', sep='^')
    df_tags = df_tags[['content_id', 'tag']][df_tags['tag_order'] <= 50]

    engine = sqlalchemy.create_engine('postgres://' + config['sql']['user'] + ':' +
                                      config['sql']['password'] + '@' +
                                      config['sql']['host'] + ':' +
                                      str(config['sql']['port']) + '/' +
                                      config['sql']['db'])
    conn = engine.connect()

    df_db_tags = pd.read_sql("""
                                select content_id, tag
                                from """ + config['sql']['schema'] + """.content_tags
                                where tag_order <= 50
                                """, con=conn)

    df_tags = pd.concat([df_db_tags, df_tags], axis=0)
    df_tags.drop_duplicates(inplace=True)

    stop_words = set(stopwords.words('english'))

    def find_synonyms(phrase):
        words = [word for word in phrase.split(' ') if word not in stop_words]
        synonyms = []
        for word in words:
            syns = wordnet.synsets(word)
            syns = [l.name() for s in syns for l in s.lemmas()]
            if syns:
                synonyms = synonyms + syns
            else:
                synonyms.append(word)
        synonyms = list(set(synonyms))

        return synonyms

    df_tags['synonym_tags'] = df_tags['tag'].apply(find_synonyms)
    df_contents = df_tags.groupby('content_id')['synonym_tags'].apply(sum).reset_index()
    df_contents['synonym_tags'] = df_contents['synonym_tags'].apply(lambda x: list(set(x)))

    int_columns = ['content_id']
    for col in int_columns:
        df_contents[col][pd.notnull(df_contents[col])] = df_contents[col][pd.notnull(df_contents[col])].apply(
            lambda x: eval(str(x).replace(',', '')))
        df_contents[col][pd.notnull(df_contents[col])] = df_contents[col][pd.notnull(df_contents[col])].apply(
            lambda x: '{:.0f}'.format(x))

    df_unique_tags = pd.DataFrame()
    for i in range(df_contents.shape[0]):
        df = pd.DataFrame(df_contents['synonym_tags'][i])
        df.rename(columns={0: 'subject'}, inplace=True)
        df['content_id'] = df_contents['content_id'][i]
        df_unique_tags = pd.concat([df_unique_tags, df], axis=0)

    df_unique_tags = df_unique_tags.groupby('subject')['content_id'].apply(list).reset_index()
    df_unique_tags.rename(columns={'content_id': 'contents'}, inplace=True)
    df_unique_tags['type'] = 'tag'

    df_unique_tags.to_csv(upload_resources_folder + 'tags_to_contents.csv', index=False)

    return True


def synonyms_similar_contents():
    engine = sqlalchemy.create_engine('postgres://' + config['sql']['user'] + ':' +
                                      config['sql']['password'] + '@' +
                                      config['sql']['host'] + ':' +
                                      str(config['sql']['port']) + '/' +
                                      config['sql']['db'])
    conn = engine.connect()

    df_tags = pd.read_sql("""
                            select content_id, tag
                            from """ + config['sql']['schema'] + """.content_tags
                            where tag_order <= 50
                            """, con=conn)

    global stop_words
    stop_words = set(stopwords.words('english'))

    df_tags = parallelize_dataframe(df_tags, apply_find_synonyms)

    global df_contents
    df_contents = df_tags.groupby('content_id')['synonym_tags'].apply(sum).reset_index()
    df_contents['synonym_tags'] = df_contents['synonym_tags'].apply(lambda x: list(set(x)))

    df_full_data = pd.read_csv(upload_resources_folder + 'full_data.csv')
    df_full_data = df_full_data[['content_id', 'genres', 'language']]
    df_full_data['genres'] = df_full_data['genres'].apply(lambda x: eval(x) if x else None)
    df_full_data['language'] = df_full_data['language'].apply(lambda x: eval(x) if x else None)

    df_contents = pd.merge(df_contents, df_full_data, how='left', on='content_id')

    df_contents_final = parallelize_dataframe(df_contents.copy(), apply_common_contents)

    df_contents_final.to_csv(upload_resources_folder + 'synonyms_similar_contents.csv', index=False)


def get_full_data():
    engine = sqlalchemy.create_engine('postgres://' + config['sql']['user'] + ':' +
                                      config['sql']['password'] + '@' +
                                      config['sql']['host'] + ':' +
                                      str(config['sql']['port']) + '/' +
                                      config['sql']['db'])

    query = """
               select content_id, is_adult, language, release_year as year, runtime, genres, imdb_score, episodes, seasons,
                      num_votes, nudity, violence, profanity, drugs, intense_scenes, award_wins, award_nominations, avg_age_limit
               from """ + config['sql']['schema'] + """.content_details
            """
    df_contents = pd.read_sql(query, engine)

    credit_catgs = ['Cast', 'Cinematography by', 'Directed by', 'Film Editing by', 'Music by', 'Writing Credits']
    for catg in credit_catgs:
        query = """
                   select content_id, cum_experience_content, cum_experience_years,
                          content_done_in_the_catg, years_in_the_catg, num_votes, imdb_score
                   from """ + config['sql']['schema'] + """.content_crew
                   where credit_category = '""" + catg + """'
                   and credit_order <= 3
                   and cast(content_id as varchar) like '1%%'
                   union all
                   select content_id, cum_experience_content, cum_experience_years,
                          content_done_in_the_catg, years_in_the_catg, num_votes, imdb_score
                   from """ + config['sql']['schema'] + """.content_crew
                   where credit_category = '""" + catg + """'
                   and credit_order <= 6
                   and cast(content_id as varchar) like '2%%'
                """
        df = pd.read_sql(query, engine)
        df = df.groupby('content_id')['cum_experience_content', 'cum_experience_years', 'content_done_in_the_catg', 'years_in_the_catg',
                                    'num_votes', 'imdb_score'].mean().reset_index()
        df.columns = df.columns.map(lambda x: str(x) + '_' + catg.lower().replace(' ', '_') if x != 'content_id' else x)
        df_contents = pd.merge(df_contents, df, how='left', on='content_id')

    columns_to_exclude_data_fill = ['content_id', 'is_adult', 'year', 'runtime', 'genres', 'language']
    for column in list(df_contents.columns):
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

    df_contents.to_csv(upload_resources_folder + 'full_data.csv', index=False)


def get_features_recom(df_resp, weight_power):
    total_columns = list(df_resp.columns)
    columns_to_exclude_mean = ['content_id', 'is_adult', 'genres', 'genres_str', 'language']
    columns_to_keep = [column for column in total_columns if column not in columns_to_exclude_mean]

    df_resp_mean_attribs = df_resp[columns_to_keep]

    # feature weights
    x = df_resp[[column for column in columns_to_keep if column not in ['rating', 'content_id']]]
    min_max_scaler = MinMaxScaler()
    if not x.empty:
        x = min_max_scaler.fit_transform(x)
        selector = VarianceThreshold()
        try:
            selector.fit_transform(x)
            variances = selector.variances_
            variances[variances == 0] = 1
            variances = np.reciprocal(variances)
            variances = variances ** weight_power
        except ValueError:
            variances = np.array([1] * len(x[0]))
    else:
        variances = np.array([1] * x.shape[0])

    return df_resp_mean_attribs, variances


def filter_data_recom(content_id, df_full_data, df_clusters):
    contents_to_knn = df_clusters['common_contents'][df_clusters['content_id'] == content_id].sum()
    if contents_to_knn:
        contents_to_knn = list(contents_to_knn)
    else:
        contents_to_knn = []
    df_full_data = df_full_data[(df_full_data['content_id'].isin(contents_to_knn))]

    return df_full_data


def get_recommendations_recom(df_filtered, df_subject, neighbours, variances):
    total_columns = list(df_filtered.columns)
    columns_to_exclude_mean = ['is_adult', 'genres', 'genres_str', 'language']
    columns_to_keep = [column for column in total_columns if column not in columns_to_exclude_mean]

    df_filtered = df_filtered[columns_to_keep]

    df_filtered.dropna(inplace=True)

    if not df_filtered.empty:
        # FItting the KNN Model
        x = df_filtered[[column for column in columns_to_keep if column != 'content_id']]
        y = df_filtered['content_id']

        min_max_scaler = MinMaxScaler()
        x = x.values
        x = min_max_scaler.fit_transform(x)
        y = y.values

        try:
            def weighed_distance(x1, x2):
                return sum(((x1 - x2) * variances) ** 2) ** 0.5

            reg = KNeighborsRegressor(n_neighbors=100, weights='distance', metric=weighed_distance)
            reg.fit(x, y)

            # Getting nearest neighbours
            subject = min_max_scaler.transform(df_subject.values)
            distances, indices = reg.kneighbors(subject, neighbours)

            return df_filtered, indices
        except ValueError:
            return False, False
    else:
        return False, False


def process_spot_instance_data():
    files_name_mapping = {
        'movie_budget_n_metacritic_scrape': movies_data_folder+'cleaned_movie_budget_n_metacritic',
        'movie_cleaned_certificates': movies_data_folder+'cleaned_certificates',
        'movie_content': movies_data_folder+'movie_content',
        'movie_crew_scrape': movies_data_folder+'movie_crew',
        'movie_keywords_scrape': movies_data_folder+'cleaned_movie_keywords',
        'movie_synopsys_scrape': movies_data_folder+'movie_synopsys',
        'movie_technical_specs_scrape': movies_data_folder+'movie_technical_specs',
        'movie_tmdb_data_collection': movies_data_folder+'cleaned_movie_tmdb',
        'tv_series_cleaned_certificates': tv_series_data_folder+'cleaned_certificates',
        'tv_series_content': tv_series_data_folder+'tv_series_content',
        'tv_series_crew_scrape': tv_series_data_folder+'cleaned_tv_series_crew',
        'tv_series_details_scrape': tv_series_data_folder+'cleaned_tv_series_details',
        'tv_series_keywords_scrape': tv_series_data_folder+'cleaned_tv_series_keywords',
        'tv_series_synopsys_scrape': tv_series_data_folder+'tv_series_synopsys',
        'tv_series_technical_specs_scrape': tv_series_data_folder+'tv_series_technical_specs',
        'tv_series_tmdb_data_collection': tv_series_data_folder+'cleaned_tv_series_tmdb'
    }
    for key in files_name_mapping.keys():
        print('Concatenating', key, 'files...')
        df = pd.DataFrame()
        for filename in os.listdir(spot_instance_scraped_data_folder):
            if filename.startswith(key) and filename.endswith('.csv'):
                print(filename)
                df = pd.concat([df, pd.read_csv(spot_instance_scraped_data_folder+filename)], axis=0)
        print('\nDumping into file', files_name_mapping[key]+'.csv')
        df.drop_duplicates(inplace=True)
        df.to_csv(files_name_mapping[key]+'.csv', index=False)
        print('\n\n')

    print('\nAll files read & processed.')
