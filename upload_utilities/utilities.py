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
import boto3
import time
import paramiko
from paramiko_expect import SSHClientInteraction
import re


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

    df_full_data = pd.read_csv('/home/ec2-user/calculated/full_data.csv')
    df_full_data = df_full_data[['content_id', 'genres', 'language']]
    df_full_data['genres'] = df_full_data['genres'].apply(lambda x: eval(x) if x else None)
    df_full_data['language'] = df_full_data['language'].apply(lambda x: eval(x) if x else None)

    df_contents = pd.merge(df_contents, df_full_data, how='left', on='content_id')

    df_contents_final = parallelize_dataframe(df_contents.copy(), apply_common_contents)

    df_contents_final.to_csv('/home/ec2-user/calculated/synonyms_similar_contents.csv', index=False)


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

    df_contents.to_csv('/home/ec2-user/calculated/full_data.csv', index=False)


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


def ssh_into_remote(hostname, username, key_file):
    client = None
    while client is None:
        try:
            print('Trying to ssh...')
            key = paramiko.RSAKey.from_private_key_file(key_file)
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            client.connect(hostname=hostname, username=username, pkey=key)
        except:
            print('Remote not completely up yet, sleeping for 10 sec...')
            time.sleep(10)
            client = None
    return client


def launch_spot_instance(size='big'):
    session = boto3.Session(
        aws_access_key_id=config['s3']['aws_access_key_id'],
        aws_secret_access_key=config['s3']['aws_secret_access_key'],
        region_name=config['s3']['region_name']
    )
    client = session.client('ec2')

    print('Submitting fleet request...')
    if size == 'big':
        response = client.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::772835535876:role/aws-ec2-spot-fleet-tagging-role",
                "AllocationStrategy": "capacityOptimized",
                "TargetCapacity": 1,
                "TerminateInstancesWithExpiration": True,
                "LaunchSpecifications": [],
                "Type": "request",
                "LaunchTemplateConfigs": [
                    {
                        "LaunchTemplateSpecification": {
                            "LaunchTemplateId": "lt-0801fa586840fa707",
                            "Version": "4"
                        },
                        "Overrides": [
                            {
                                "InstanceType": "m5dn.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5d.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5a.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5ad.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5a.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5d.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5d.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5a.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5d.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5n.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5dn.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5n.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5d.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5ad.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            }
                        ]
                    }
                ]
            }
        )
    else:
        response = client.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::772835535876:role/aws-ec2-spot-fleet-tagging-role",
                "AllocationStrategy": "capacityOptimized",
                "TargetCapacity": 1,
                "TerminateInstancesWithExpiration": True,
                "LaunchSpecifications": [],
                "Type": "request",
                "LaunchTemplateConfigs": [
                    {
                        "LaunchTemplateSpecification": {
                            "LaunchTemplateId": "lt-0801fa586840fa707",
                            "Version": "4"
                        },
                        "Overrides": [
                            {
                                "InstanceType": "m5d.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5dn.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5n.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "a1.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "a1.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5ad.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5d.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r4.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5n.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5a.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5dn.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5d.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "t3.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r6g.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5n.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m6g.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5ad.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5dn.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5a.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r4.2xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            }
                        ]
                    }
                ]
            }
        )

    spot_fleet_request_id = response['SpotFleetRequestId']
    print('Fleet request id -', spot_fleet_request_id)

    print('Fetching instances...')
    response = client.describe_spot_fleet_instances(
        SpotFleetRequestId=spot_fleet_request_id
    )
    while len(response['ActiveInstances']) == 0:
        time.sleep(5)
        print('Fetching instances again...')
        response = client.describe_spot_fleet_instances(
            SpotFleetRequestId=spot_fleet_request_id
        )
    instance_id = response['ActiveInstances'][0]['InstanceId']
    print('Instance id -', instance_id)

    print('Fetching instance public dns...')
    response = client.describe_instances(
        InstanceIds=[instance_id]
    )
    public_dns = response['Reservations'][0]['Instances'][0]['PublicDnsName']
    private_ip = response['Reservations'][0]['Instances'][0]['PrivateIpAddress']

    return spot_fleet_request_id, public_dns, private_ip


def close_spot_fleet_request_and_instances(spot_fleet_request_id):
    session = boto3.Session(
        aws_access_key_id=config['s3']['aws_access_key_id'],
        aws_secret_access_key=config['s3']['aws_secret_access_key'],
        region_name=config['s3']['region_name']
    )
    client = session.client('ec2')

    print('Cancelling fleet request & terminating instances...')
    client.cancel_spot_fleet_requests(
        SpotFleetRequestIds=[spot_fleet_request_id],
        TerminateInstances=True
    )

    return True


def install_requirements_on_remote(public_dns, private_ip, username, key_file, postgres=False):
    default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)

    client = ssh_into_remote(public_dns, username, key_file)
    with SSHClientInteraction(client, timeout=60, display=True) as interact:
        interact.expect(default_prompt)

        interact.send('sudo yum install htop')
        interact.expect('Is this ok \[y/d/N\]:\s+')
        interact.send('y')
        interact.expect(default_prompt)

        if postgres:
            interact.send('sudo yum install postgresql postgresql-server postgresql-devel postgresql-contrib postgresql-docs')
            interact.expect('Is this ok \[y/d/N\]:\s+')
            interact.send('y')
            interact.expect(default_prompt)

            interact.send('sudo service postgresql initdb')
            interact.expect(default_prompt)

            interact.send('sudo service postgresql start')
            interact.expect(default_prompt)
        else:
            interact.send('sudo yum install python36 python36-pip')
            interact.expect('Is this ok \[y/d/N\]:\s+')
            interact.send('y')
            interact.expect(default_prompt)

            interact.send('sudo pip-3.6 install virtualenv')
            interact.expect(default_prompt)

            interact.send('sudo python3.6 -m virtualenv venv_similar_content')
            interact.expect(default_prompt)

            interact.send('source ./venv_similar_content/bin/activate')
            interact.expect('\(venv_similar_content\)\s+'+default_prompt)

            interact.send('sudo pip install --upgrade pip')
            interact.expect('\(venv_similar_content\)\s+' + default_prompt)

            interact.send('sudo yum install python36-devel')
            interact.expect('Is this ok \[y/d/N\]:\s+')
            interact.send('y')
            interact.expect('\(venv_similar_content\)\s+' + default_prompt)

            interact.send('sudo yum  install libevent-devel')
            interact.expect('Is this ok \[y/d/N\]:\s+')
            interact.send('y')
            interact.expect('\(venv_similar_content\)\s+' + default_prompt)

            interact.send('sudo yum -y install gcc')
            interact.expect('\(venv_similar_content\)\s+' + default_prompt)

            interact.send('sudo yum install git')
            interact.expect('Is this ok \[y/d/N\]:\s+')
            interact.send('y')
            interact.expect('\(venv_similar_content\)\s+' + default_prompt)

            interact.send('git clone https://github.com/flibo-tech/data_pipelines.git')
            interact.expect("Username for 'https://github.com':\s+")
            interact.send(config['git']['username'])
            interact.expect("Password for 'https://"+config['git']['username']+"@github.com':\s+")
            interact.send(config['git']['password'])
            interact.expect('\(venv_similar_content\)\s+' + default_prompt)

            interact.send('cd data_pipelines')
            interact.expect('\(venv_similar_content\)\s+' + default_prompt.replace('~', 'data_pipelines'))

            interact.send('git checkout develop')
            interact.expect('\(venv_similar_content\)\s+' + default_prompt.replace('~', 'data_pipelines'))

            interact.send('sudo pip-3.6 install -r requirements.txt')
            interact.expect('\(venv_similar_content\)\s+' + default_prompt.replace('~', 'data_pipelines'))

            interact.send('sudo python3.6')
            interact.expect('\>\>\>\s+')

            interact.send('import nltk')
            interact.expect('\>\>\>\s+')

            interact.send("nltk.download('stopwords')")
            interact.expect('\>\>\>\s+')

            interact.send("nltk.download('wordnet')")
            interact.expect('\>\>\>\s+')

            interact.send('exit()')
            interact.expect('\(venv_similar_content\)\s+' + default_prompt.replace('~', 'data_pipelines'))

        client.close()
        return True


def calculate_on_remote(public_dns, private_ip, username, key_file, arg):
    default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)

    client = ssh_into_remote(public_dns, username, key_file)
    with SSHClientInteraction(client, timeout=60*60, display=True) as interact:
        interact.expect(default_prompt)

        interact.send('source ./venv_similar_content/bin/activate')
        interact.expect('\(venv_similar_content\)\s+' + default_prompt)

        interact.send('mkdir /home/' + username + '/calculated')
        interact.expect('\(venv_similar_content\)\s+' + default_prompt)

        interact.send('cd data_pipelines/upload_utilities')
        interact.expect('\(venv_similar_content\)\s+' + default_prompt.replace('~', 'scraping_utilities'))

        interact.send('sudo python3.6 upload.py '+arg)
        interact.expect('\(venv_similar_content\)\s+' + default_prompt.replace('~', 'scraping_utilities'))

        interact.send('sudo chmod -R 777 /home/' + username + '/calculated/')
        interact.expect('\(venv_similar_content\)\s+' + default_prompt.replace('~', 'scraping_utilities'))

        interact.send('sudo rm /home/' + username + '/calculated/full_data.csv')
        interact.expect('\(venv_similar_content\)\s+' + default_prompt.replace('~', 'scraping_utilities'))

        interact.send('sudo rm /home/' + username + '/calculated/synonyms_similar_contents.csv')
        interact.expect('\(venv_similar_content\)\s+' + default_prompt.replace('~', 'scraping_utilities'))

        client.close()
        return True


def calculate_crew_table_on_remote(public_dns, private_ip, username, key_file):
    print('Dumping prod tables into CSVs...')
    engine = sqlalchemy.create_engine(
        'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
            'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])
    con = engine.connect()
    trans = con.begin()

    query = 'select count(*) from app.content_crew;'
    current_crew_count = con.execute(query).first()[0]

    con.close()
    print('Dumping into CSVs finished.')

    print('\nTransferring RSA key to spot instance...')
    cmd = 'scp -r -o StrictHostKeyChecking=no -i ' + key_file + ' ' + key_file + ' ec2-user@' + public_dns + ':/tmp/key.pem'
    os.system(cmd)

    print('\nStarting work on remote...')
    client = ssh_into_remote(public_dns, username, key_file)
    with SSHClientInteraction(client, timeout=2*60, display=True) as interact:
        default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)
        interact.expect(default_prompt)

        print('\nFetching prod table CSVs...')

        interact.send('sudo scp -r -o StrictHostKeyChecking=no -i /tmp/key.pem ec2-user@ec2-13-59-44-163.us-east-2.compute.amazonaws.com:/tmp/content_crew.csv /tmp/content_crew.csv')
        interact.expect(default_prompt)

        interact.send('psql -h '+config['sql']['host']+' -U '+config['sql']['user']+' -p '+str(config['sql']['port']))
        interact.expect('Password for user postgres\:\s*')

        interact.send(config['sql']['password'])
        interact.expect('postgres\=\#\s+')

        interact.send('\c flibo')
        interact.expect('flibo\=\#\s+')

        interact.send("\copy (select * From app.awards_distribution) To '/tmp/db_backup_awards_distribution.csv' WITH CSV DELIMITER '^' HEADER;")
        interact.expect('flibo\=\#\s+')

        interact.send("\copy (select * From app.content_details) To '/tmp/db_backup_content_details.csv' WITH CSV DELIMITER '^' HEADER;")
        interact.expect('flibo\=\#\s+')

        interact.send("\copy (select * From app.content_crew) To '/tmp/db_backup_content_crew.csv' WITH CSV DELIMITER '^' HEADER;")
        interact.expect('flibo\=\#\s+')

        interact.send('\q')
        interact.expect(default_prompt)

        interact.send('sudo chmod -R 777 /tmp/')
        interact.expect(default_prompt)

        # Creating SQL db...
        interact.send('sudo su - postgres')
        interact.expect('\-bash\-4\.2\$\s+')

        interact.send('psql -U postgres')
        interact.expect('postgres\=\#\s+')

        interact.send('create database flibo;')
        interact.send('\c flibo')
        interact.expect('flibo\=\#\s+')

        query = """
                create schema app;

                CREATE TABLE app.awards_distribution (
                    award_distribution_id serial NOT NULL,
                    award_id int4 NULL,
                    event_year int4 NULL,
                    content_id int4 NULL,
                    person_id int4 NULL,
                    nomination_notes varchar NULL,
                    won bool NULL,
                    CONSTRAINT awards_distribution_pkey PRIMARY KEY (award_distribution_id)
                );

                CREATE TABLE app.content_crew (
                    content_crew_id serial NOT NULL,
                    person_id int4 NULL,
                    content_id int4 NULL,
                    credit_as varchar NULL,
                    credit_category varchar NULL,
                    credit_order int4 NULL,
                    credit_episodes int4 NULL,
                    credit_start_year int4 NULL,
                    credit_end_year int4 NULL,
                    common_tags varchar[] NULL,
                    cum_experience_content int4 NULL,
                    cum_experience_years int4 NULL,
                    content_done_in_the_catg int4 NULL,
                    years_in_the_catg int4 NULL,
                    num_votes float8 NULL,
                    imdb_score float8 NULL,
                    metacritic_score float8 NULL,
                    tmdb_score float8 NULL,
                    tomato_meter float8 NULL,
                    nominations int4 NULL,
                    wins_to_nominations float8 NULL,
                    CONSTRAINT content_crew_pkey PRIMARY KEY (content_crew_id)
                );


                CREATE TABLE app.content_details (
                    content_id serial NOT NULL,
                    imdb_content_id varchar NULL,
                    title varchar NULL,
                    original_title varchar NULL,
                    "type" varchar NULL,
                    is_adult bool NULL,
                    in_production bool NULL,
                    release_year int4 NULL,
                    end_year int4 NULL,
                    episodes int4 NULL,
                    seasons int4 NULL,
                    runtime int4 NULL,
                    genres varchar[] NULL,
                    imdb_score float8 NULL,
                    num_votes int4 NULL,
                    scripting varchar NULL,
                    summary_text varchar NULL,
                    country varchar[] NULL,
                    "language" varchar[] NULL,
                    filming_location varchar NULL,
                    production_house varchar[] NULL,
                    budget varchar NULL,
                    opening_weekend_usa varchar NULL,
                    gross_usa varchar NULL,
                    gross_worldwide varchar NULL,
                    critic_review int4 NULL,
                    user_review int4 NULL,
                    award_wins int4 NULL,
                    award_nominations int4 NULL,
                    youtube_trailer_id varchar NULL,
                    cover varchar NULL,
                    poster varchar NULL,
                    metacritic_score int4 NULL,
                    tmdb_id int4 NULL,
                    tmdb_popularity float8 NULL,
                    tmdb_score float8 NULL,
                    tomato_id int4 NULL,
                    tomato_meter int4 NULL,
                    tomato_rating int4 NULL,
                    tomato_score int4 NULL,
                    tomato_userrating_meter int4 NULL,
                    tomato_userrating_rating int4 NULL,
                    tomato_userrating_score int4 NULL,
                    nudity int4 NULL,
                    violence int4 NULL,
                    profanity int4 NULL,
                    drugs int4 NULL,
                    intense_scenes int4 NULL,
                    avg_age_limit int4 NULL,
                    mpaa_age_limit int4 NULL,
                    tags varchar[] NULL,
                    similar_content int4[] NULL,
                    filtered_content int4[] NULL,
                    justwatch_rating int4 NULL,
                    website varchar NULL,
                    facebook varchar NULL,
                    instagram varchar NULL,
                    twitter varchar NULL,
                    where_to_watch_australia varchar NULL,
                    where_to_watch_brazil varchar NULL,
                    where_to_watch_canada varchar NULL,
                    where_to_watch_france varchar NULL,
                    where_to_watch_germany varchar NULL,
                    where_to_watch_india varchar NULL,
                    where_to_watch_indonesia varchar NULL,
                    where_to_watch_italy varchar NULL,
                    where_to_watch_japan varchar NULL,
                    where_to_watch_mexico varchar NULL,
                    where_to_watch_philippines varchar NULL,
                    where_to_watch_russia varchar NULL,
                    where_to_watch_spain varchar NULL,
                    where_to_watch_united_kingdom varchar NULL,
                    where_to_watch_united_states varchar NULL,
                    main_artists varchar[] NULL,
                    url_title varchar NULL,
                    CONSTRAINT content_details_pkey PRIMARY KEY (content_id)
                    );

                copy app.content_details
                (content_id, imdb_content_id, title, original_title, type, is_adult, in_production, release_year, end_year, episodes, seasons, runtime, genres, imdb_score, num_votes, scripting, summary_text, country, language, filming_location, production_house, budget, opening_weekend_usa, gross_usa, gross_worldwide, critic_review, user_review, award_wins, award_nominations, youtube_trailer_id, cover, poster, metacritic_score, tmdb_id, tmdb_popularity, tmdb_score, tomato_id, tomato_meter, tomato_rating, tomato_score, tomato_userrating_meter, tomato_userrating_rating, tomato_userrating_score, nudity, violence, profanity, drugs, intense_scenes, avg_age_limit, mpaa_age_limit, tags, similar_content, filtered_content, justwatch_rating, website, facebook, instagram, twitter, where_to_watch_australia,where_to_watch_brazil,where_to_watch_canada,where_to_watch_france,where_to_watch_germany,where_to_watch_india,where_to_watch_indonesia,where_to_watch_italy,where_to_watch_japan,where_to_watch_mexico,where_to_watch_philippines,where_to_watch_russia,where_to_watch_spain,where_to_watch_united_kingdom,where_to_watch_united_states,main_artists,url_title)
                FROM '/tmp/db_backup_content_details.csv'
                WITH DELIMITER AS '^'
                CSV HEADER;

                Copy (Select 1 as count) To '/tmp/db_backup_content_details.csv' WITH CSV DELIMITER '^' HEADER;

                copy app.awards_distribution
                (award_distribution_id, award_id, event_year, content_id, person_id, nomination_notes, won)
                FROM '/tmp/db_backup_awards_distribution.csv'
                WITH DELIMITER AS '^'
                CSV HEADER;

                Copy (Select 1 as count) To '/tmp/db_backup_awards_distribution.csv' WITH CSV DELIMITER '^' HEADER;

                copy app.content_crew
                (content_crew_id, person_id, content_id, credit_as, credit_category, credit_order, credit_episodes, credit_start_year, credit_end_year, common_tags, cum_experience_content, cum_experience_years, content_done_in_the_catg, years_in_the_catg, num_votes, imdb_score, metacritic_score, tmdb_score, tomato_meter, nominations, wins_to_nominations)
                FROM '/tmp/db_backup_content_crew.csv'
                WITH DELIMITER AS '^'
                CSV HEADER;

                Copy (Select 1 as count) To '/tmp/db_backup_content_crew.csv' WITH CSV DELIMITER '^' HEADER;

                SET work_mem = '25000MB';

                CREATE TABLE app.content_crew_temp
                (
                content_crew_id serial,
                person_id integer NULL,
                content_id integer NULL,
                credit_as varchar NULL,
                credit_category varchar NULL,
                credit_order int4 NULL,
                credit_episodes int4 NULL,
                credit_start_year int4 NULL,
                credit_end_year int4 NULL,
                common_tags varchar[] NULL,
                CONSTRAINT content_crew_temp_pkey PRIMARY KEY (content_crew_id)
                );


                copy app.content_crew_temp
                (person_id,content_id,credit_as,credit_category,credit_order,credit_episodes,credit_start_year,credit_end_year)
                FROM '/tmp/content_crew.csv'
                WITH DELIMITER AS '^'
                CSV HEADER;
                
                
                Copy (Select 1 as count) To '/tmp/content_crew.csv' WITH CSV DELIMITER '^' HEADER;


                insert into app.content_crew_temp
                (person_id,content_id,credit_as,credit_category,credit_order,credit_episodes,credit_start_year,credit_end_year)
                select person_id,content_id,credit_as,credit_category,credit_order,credit_episodes,credit_start_year,credit_end_year
                from app.content_crew
                except
                select person_id,content_id,credit_as,credit_category,credit_order,credit_episodes,credit_start_year,credit_end_year
                from app.content_crew_temp;


                truncate table app.content_crew;
                
                
                update app.content_crew_temp
                set credit_category = trim(credit_category);
                
                
                update app.content_crew_temp
                set credit_category = regexp_replace(credit_category, '^Series ', '');
                
                
                update app.content_crew_temp
                set credit_category = trim(credit_category);


                update app.content_crew_temp
                set credit_category = case when credit_category = 'Cast complete, awaiting verification' then 'Cast'
                                           when credit_category = 'Cast verified as complete' then 'Cast'
                                           when credit_category = 'Writing Credits (WGA)' then 'Writing Credits'
                                           else credit_category
                                      end;


                insert into app.content_crew
                (person_id,content_id,credit_as,credit_category,credit_order,common_tags,cum_experience_content,
                 cum_experience_years,credit_episodes,credit_start_year,credit_end_year,content_done_in_the_catg,
                 years_in_the_catg,num_votes,imdb_score,metacritic_score,tmdb_score,tomato_meter,nominations,wins_to_nominations)
                select t18.*, nominations, wins_to_nominations
                from (
                      select t13.*, content_done_in_the_catg, years_in_the_catg, num_votes, imdb_score, metacritic_score, tmdb_score, tomato_meter
                      from (
                            select t11.person_id, t11.content_id, t11.credit_as, t11.credit_category, t11.credit_order, t11.common_tags,
                                   experience_movies, experience_years, t11.credit_episodes, t11.credit_start_year, t11.credit_end_year
                            from (
                                  select t9.person_id, t9.content_id, credit_as, credit_category, credit_order,
                                         credit_episodes, credit_start_year, credit_end_year, common_tags, year
                                  from app.content_crew_temp t9
                                  left join (
                                            select content_id, release_year as year
                                            from app.content_details
                                            ) t10
                                  on t9.content_id = t10.content_id
                                  ) t11
                            left join
                                 (
                                  select year, person_id, credit_category, sum(count(*)) over (PARTITION BY person_id, credit_category order by person_id, credit_category, year) experience_movies, (year-min(career_start)) as experience_years
                                  from (
                                        select t3.content_id, t3.person_id, t3.credit_category, year, career_start
                                        from (
                                              select person_id, t1.content_id, credit_category, year
                                              from app.content_crew_temp t1
                                              left join (
                                                        select content_id, release_year as year
                                                        from app.content_details
                                                        ) t2
                                              on t1.content_id = t2.content_id
                                              ) t3
                                        left join
                                              (
                                              select person_id, credit_category, min(year) career_start
                                              from (
                                                    select *
                                                    from app.content_crew_temp t4
                                                    left join (
                                                              select content_id, release_year as year
                                                              from app.content_details
                                                              ) t5
                                                    on t4.content_id = t5.content_id
                                                    ) t6
                                              group by person_id, credit_category
                                              ) t7
                                        on t3.person_id = t7.person_id
                                        and t3.credit_category = t7.credit_category
                                        ) t8
                                  group by person_id, credit_category, year
                                  order by person_id, credit_category, year
                                  ) t12
                            on t11.year = t12.year
                            and t11.person_id = t12.person_id
                            and t11.credit_category = t12.credit_category
                            ) t13
                      left join
                            (
                            select person_id, credit_category, count(person_id) as content_done_in_the_catg,
                                   max(end_year)-min(release_year) as years_in_the_catg,
                                   avg(num_votes) as num_votes, avg(imdb_score) as imdb_score,
                                   avg(metacritic_score) as metacritic_score, avg(tmdb_score) as tmdb_score,
                                   avg(tomato_meter) as tomato_meter
                            from (
                                  select person_id,credit_category,imdb_score,num_votes,metacritic_score,tmdb_score,
                                         tomato_meter,release_year,case when end_year is not null then end_year
                                                                        when in_production is true and type='tv' then date_part('year', CURRENT_DATE)
                                                                        else release_year end as end_year
                                  from app.content_crew_temp t14
                                  left join app.content_details t15
                                  on t14.content_id = t15.content_id
                                  ) t16
                            group by person_id, credit_category
                            ) t17
                      on t13.person_id = t17.person_id
                      and t13.credit_category = t17.credit_category
                      ) t18
                left join
                      (
                      select t19.person_id, nominations, cast(coalesce(wins, 0) as float)/cast(nominations as float) as wins_to_nominations
                      from (
                            select person_id, count(*) as nominations
                            from app.awards_distribution
                            where person_id is not null
                            group by person_id
                            ) t19
                      left join
                           (
                            select person_id, count(*) as wins
                            from app.awards_distribution
                            where person_id is not null
                            and won is true
                            group by person_id
                            ) t20
                      on t19.person_id = t20.person_id
                      ) t21
                on t18.person_id = t21.person_id;

                \q
                """
        interact.send(query)
        try:
            interact.expect('\-bash\-4\.2\$\s+')
        except:
            print('Waiting for query to end (in next step)...')

        client.close()

    client = ssh_into_remote(public_dns, username, key_file)
    with SSHClientInteraction(client, timeout=90 * 60, display=True) as interact:
        default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)
        interact.expect(default_prompt)

        interact.send('sudo su - postgres')
        interact.expect('\-bash\-4\.2\$\s+')

        interact.send('psql -U postgres')
        interact.expect('postgres\=\#\s+')

        interact.send('\c flibo')
        interact.expect('flibo\=\#\s+')

        new_crew_count = 0
        while new_crew_count < current_crew_count:
            print('Sleeping for 2 minutes...')
            time.sleep(2*60)

            interact.send('select count(*) from app.content_crew;')
            interact.expect('flibo\=\#\s+')
            output = interact.current_output_clean

            match = re.findall(r'\d{8}', output)
            if match:
                new_crew_count = int(match[0])
            print('New count -', new_crew_count)

        query = """
                SET work_mem = '25000MB';

                \copy (Select person_id, content_id, credit_as, credit_category, credit_order, credit_episodes, credit_start_year, credit_end_year, common_tags, cum_experience_content, cum_experience_years, content_done_in_the_catg, years_in_the_catg, num_votes, imdb_score, metacritic_score, tmdb_score, tomato_meter, nominations, wins_to_nominations From app.content_crew) To '/tmp/final_content_crew.csv' WITH CSV DELIMITER '^' HEADER;
                
                \q
                """
        interact.send(query)
        interact.expect('\-bash\-4\.2\$\s+', timeout=10*60)

        interact.send('exit')
        interact.expect(default_prompt)

        interact.send('sudo chmod -R 777 /tmp/')
        interact.expect(default_prompt)

        interact.send('sudo chmod 600 /tmp/key.pem')
        interact.expect(default_prompt)

        print('\nUploading file final_content_crew.csv to prod server...')
        interact.send('scp -r -o StrictHostKeyChecking=no -i /tmp/key.pem /tmp/final_content_crew.csv ec2-user@ec2-13-59-44-163.us-east-2.compute.amazonaws.com:/tmp/')
        interact.expect(default_prompt)

        client.close()
        return True


################# functions for dumping data into SQL tables #################


def dump_movies(engine):
    con = engine.connect()
    trans = con.begin()
    sql = """
          copy """ + config['sql']['schema'] + """.content_details
          (content_id,imdb_content_id,title,original_title,is_adult,release_year,runtime,genres,imdb_score,num_votes,summary_text,country,language,filming_location,production_house,budget,opening_weekend_USA,gross_USA,gross_worldwide,critic_review,user_review,award_wins,award_nominations,youtube_trailer_id,cover,poster,metacritic_score,tmdb_id,tmdb_popularity,tmdb_score,tomato_id,tomato_meter,tomato_rating,tomato_score,tomato_userrating_meter,tomato_userrating_rating,tomato_userrating_score,nudity,violence,profanity,drugs,intense_scenes,avg_age_limit,mpaa_age_limit,tags,similar_content,filtered_content,justwatch_rating,website,facebook,instagram,twitter)
          FROM '/tmp/movies.csv'
          WITH DELIMITER AS '^'
          CSV HEADER;


          update """ + config['sql']['schema'] + """.content_details
          set summary_text = split_part(summary_text, ' See full summary', 1);


          update """ + config['sql']['schema'] + """.content_details
          set type = 'movie'
          where cast(content_id as varchar) like '1%%';


          update """ + config['sql']['schema'] + """.content_details
          set poster = 'https://flibo-images.s3-us-west-2.amazonaws.com/posters/no-poster.png'
          where poster is null;


          update """ + config['sql']['schema'] + """.content_details
          set cover = 'https://flibo-images.s3-us-west-2.amazonaws.com/covers/no-cover.jpg'
          where cover is null;
          """
    con.execute(sql)
    trans.commit()
    con.close()

    return True


def dump_tv_series(engine):
    con = engine.connect()
    trans = con.begin()
    sql = """
          copy """ + config['sql']['schema'] + """.content_details
          (content_id,imdb_content_id,title,original_title,is_adult,release_year,end_year,runtime,genres,imdb_score,num_votes,summary_text,country,language,filming_location,production_house,budget,opening_weekend_usa,gross_usa,gross_worldwide,critic_review,user_review,award_wins,award_nominations,youtube_trailer_id,cover,poster,metacritic_score,tmdb_id,tmdb_popularity,tmdb_score,tomato_id,tomato_meter,tomato_rating,tomato_score,tomato_userrating_meter,tomato_userrating_rating,tomato_userrating_score,nudity,violence,profanity,drugs,intense_scenes,avg_age_limit,mpaa_age_limit,tags,similar_content,filtered_content,justwatch_rating,website,facebook,instagram,twitter,episodes,seasons,in_production,scripting,type
)
          FROM '/tmp/tv_series.csv'
          WITH DELIMITER AS '^'
          CSV HEADER;


          update """ + config['sql']['schema'] + """.content_details
          set summary_text = split_part(summary_text, ' See full summary', 1);


          update """ + config['sql']['schema'] + """.content_details
          set type = 'tv'
          where cast(content_id as varchar) like '2%%';


          update """ + config['sql']['schema'] + """.content_details
          set poster = 'https://flibo-images.s3-us-west-2.amazonaws.com/posters/no-poster.png'
          where poster is null;


          update """ + config['sql']['schema'] + """.content_details
          set cover = 'https://flibo-images.s3-us-west-2.amazonaws.com/covers/no-cover.jpg'
          where cover is null;
          """
    con.execute(sql)
    trans.commit()
    con.close()

    return True


def dump_artists(engine):
    con = engine.connect()
    trans = con.begin()
    sql = """
          copy """ + config['sql']['schema'] + """.artists
          (imdb_person_id,name,picture,person_id)
          FROM '/tmp/artists.csv'
          WITH DELIMITER AS '^'
          CSV HEADER;
          """
    con.execute(sql)
    trans.commit()
    con.close()

    return True


def dump_content_tags(engine):
    con = engine.connect()
    trans = con.begin()
    sql = """
          SELECT setval('""" + config['sql'][
        'schema'] + """.content_tags_content_tag_id_seq', (select max(content_tag_id) from """ + config['sql'][
              'schema'] + """.content_tags), true);

          copy """ + config['sql']['schema'] + """.content_tags
          (content_id,tag,tag_order,total_votes,upvotes)
          FROM '/tmp/content_tags.csv'
          WITH DELIMITER AS '^'
          CSV HEADER;
          """
    con.execute(sql)
    trans.commit()

    try:
        trans = con.begin()
        con.execute('REINDEX INDEX ' + config['sql']['schema'] + '.content_tags_content_id_idx;')
    except:
        trans.commit()
        trans = con.begin()
        con.execute(
            'CREATE INDEX content_tags_content_id_idx ON ' + config['sql']['schema'] + '.content_tags(content_id);')
        trans.commit()

    con.close()

    return True


def dump_awards_master(engine):
    con = engine.connect()
    trans = con.begin()
    sql = """
          create table """ + config['sql']['schema'] + """.awards_master_temp (like """ + config['sql']['schema'] + """.awards_master);


          copy """ + config['sql']['schema'] + """.awards_master_temp
          (award_id, award_category, award_name, event_id, event_name, event_award_category)
          FROM '/tmp/awards_master.csv'
          WITH DELIMITER AS '^'
          CSV HEADER;


          insert into """ + config['sql']['schema'] + """.awards_master
          (award_id,award_category,event_id,event_name,award_name, event_award_category)
          select award_id,award_category,event_id,event_name,award_name, event_award_category
          from """ + config['sql']['schema'] + """.awards_master_temp
          except
          select award_id,award_category,event_id,event_name,award_name, event_award_category
          from """ + config['sql']['schema'] + """.awards_master;


          truncate table """ + config['sql']['schema'] + """.awards_master_temp;


          drop table """ + config['sql']['schema'] + """.awards_master_temp;
          """
    con.execute(sql)
    trans.commit()
    con.close()

    return True


def dump_awards_distribution(engine):
    con = engine.connect()
    trans = con.begin()
    sql = """
          truncate table """ + config['sql']['schema'] + """.awards_distribution;

          copy """ + config['sql']['schema'] + """.awards_distribution
          (award_id,event_year,content_id,person_id,nomination_notes,won)
          FROM '/tmp/awards_distribution.csv'
          WITH DELIMITER AS '^'
          CSV HEADER;
          """
    con.execute(sql)
    trans.commit()

    try:
        trans = con.begin()
        con.execute('REINDEX INDEX ' + config['sql']['schema'] + '.awards_distribution_award_id_idx;')
    except:
        trans.commit()
        trans = con.begin()
        con.execute('CREATE INDEX awards_distribution_award_id_idx ON ' + config['sql'][
            'schema'] + '.awards_distribution(award_id);')
        trans.commit()

    try:
        trans = con.begin()
        con.execute('REINDEX INDEX ' + config['sql']['schema'] + '.awards_distribution_content_id_idx;')
    except:
        trans.commit()
        trans = con.begin()
        con.execute('CREATE INDEX awards_distribution_content_id_idx ON ' + config['sql'][
            'schema'] + '.awards_distribution(content_id);')
        trans.commit()

    try:
        trans = con.begin()
        con.execute('REINDEX INDEX ' + config['sql']['schema'] + '.awards_distribution_person_id_idx;')
    except:
        trans.commit()
        trans = con.begin()
        con.execute('CREATE INDEX awards_distribution_person_id_idx ON ' + config['sql'][
            'schema'] + '.awards_distribution(person_id);')
        trans.commit()

    con.close()

    return True


def dump_content_certificates(engine):
    con = engine.connect()
    trans = con.begin()
    sql = """
          SELECT setval('""" + config['sql'][
        'schema'] + """.content_certificates_content_certificate_id_seq', (select max(content_certificate_id) from """ + \
          config['sql']['schema'] + """.content_certificates), true);

          copy """ + config['sql']['schema'] + """.content_certificates
          (content_id,certificate_by,rating,rating_cleaned,age_limit,parental_guide,banned)
          FROM '/tmp/content_certificates.csv'
          WITH DELIMITER AS '^'
          CSV HEADER;
          """
    con.execute(sql)
    trans.commit()

    try:
        trans = con.begin()
        con.execute('REINDEX INDEX ' + config['sql']['schema'] + '.content_certificates_content_id_idx;')
    except:
        trans.commit()
        trans = con.begin()
        con.execute('CREATE INDEX content_certificates_content_id_idx ON ' + config['sql'][
            'schema'] + '.content_certificates(content_id);')
        trans.commit()

    con.close()

    return True


def dump_live_search(engine):
    con = engine.connect()
    trans = con.begin()
    sql = """
          SELECT setval('""" + config['sql'][
        'schema'] + """.live_search_live_search_id_seq', (select max(live_search_id) from """ + config['sql'][
              'schema'] + """.live_search), true);

          truncate table """ + config['sql']['schema'] + """.live_search;


          copy """ + config['sql']['schema'] + """.live_search
          (image,popularity,subject,subject_id,subject_type,contents)
          FROM '/tmp/explore_single_word_tags.csv'
          WITH DELIMITER AS '^'
          CSV HEADER;


          update """ + config['sql']['schema'] + """.live_search
          set image = 'https://flibo-images.s3-us-west-2.amazonaws.com/posters/no-poster.png'
          where image is null
          and subject_type = 'content';
          """
    con.execute(sql)
    trans.commit()

    try:
        trans = con.begin()
        con.execute('REINDEX INDEX ' + config['sql']['schema'] + '.live_search_subject_idx;')
    except:
        trans.commit()
        trans = con.begin()
        con.execute('CREATE INDEX live_search_subject_idx ON ' + config['sql']['schema'] + '.live_search(subject);')
        trans.commit()

    try:
        trans = con.begin()
        con.execute('REINDEX INDEX ' + config['sql']['schema'] + '.live_search_subject_id_idx;')
    except:
        trans.commit()
        trans = con.begin()
        con.execute(
            'CREATE INDEX live_search_subject_id_idx ON ' + config['sql']['schema'] + '.live_search(subject_id);')
        trans.commit()

    try:
        trans = con.begin()
        con.execute('REINDEX INDEX ' + config['sql']['schema'] + '.live_search_subject_type_idx;')
    except:
        trans.commit()
        trans = con.begin()
        con.execute(
            'CREATE INDEX live_search_subject_type_idx ON ' + config['sql']['schema'] + '.live_search(subject_type);')
        trans.commit()

    con.close()

    return True


def dump_content_crew(engine):
    con = engine.connect()

    sql = """
                CREATE TABLE """ + config['sql']['schema'] + """.content_crew_temp (
                    content_crew_id serial NOT NULL,
                    person_id int4 NULL,
                    content_id int4 NULL,
                    credit_as varchar NULL,
                    credit_category varchar NULL,
                    credit_order int4 NULL,
                    credit_episodes int4 NULL,
                    credit_start_year int4 NULL,
                    credit_end_year int4 NULL,
                    common_tags varchar[] NULL,
                    cum_experience_content int4 NULL,
                    cum_experience_years int4 NULL,
                    content_done_in_the_catg int4 NULL,
                    years_in_the_catg int4 NULL,
                    num_votes float8 NULL,
                    imdb_score float8 NULL,
                    metacritic_score float8 NULL,
                    tmdb_score float8 NULL,
                    tomato_meter float8 NULL,
                    nominations int4 NULL,
                    wins_to_nominations float8 NULL,
                    CONSTRAINT content_crew_temp_pkey PRIMARY KEY (content_crew_id),
                    CONSTRAINT content_crew_content_id_fkey FOREIGN KEY (content_id) REFERENCES """ + config['sql'][
        'schema'] + """.content_details(content_id),
                    CONSTRAINT content_crew_person_id_fkey FOREIGN KEY (person_id) REFERENCES """ + config['sql'][
              'schema'] + """.artists(person_id)
                );


                copy """ + config['sql']['schema'] + """.content_crew_temp
                (person_id, content_id, credit_as, credit_category, credit_order, credit_episodes, credit_start_year, credit_end_year, common_tags, cum_experience_content, cum_experience_years, content_done_in_the_catg, years_in_the_catg, num_votes, imdb_score, metacritic_score, tmdb_score, tomato_meter, nominations, wins_to_nominations)
                FROM '/tmp/final_content_crew.csv'
                WITH DELIMITER AS '^'
                CSV HEADER;


                ALTER TABLE """ + config['sql']['schema'] + """.content_crew RENAME TO content_crew_old;


                ALTER TABLE """ + config['sql']['schema'] + """.content_crew_temp RENAME TO content_crew;
                
                
                truncate table """ + config['sql']['schema'] + """.content_crew_old;


                drop table """ + config['sql']['schema'] + """.content_crew_old;


                ALTER INDEX """ + config['sql']['schema'] + """.content_crew_temp_pkey RENAME TO content_crew_pkey;


                CREATE INDEX content_crew_content_id_idx ON """ + config['sql']['schema'] + """.content_crew(content_id);


                CREATE INDEX content_crew_person_id_idx ON """ + config['sql']['schema'] + """.content_crew(person_id);


                ALTER SEQUENCE """ + config['sql']['schema'] + """.content_crew_temp_content_crew_id_seq RENAME TO content_crew_content_crew_id_seq;
                
                
                update """ + config['sql']['schema'] + """.content_details
                set url_title = lower(regexp_replace(title, '[^a-zA-Z0-9]+', '-', 'g'));


                update """ + config['sql']['schema'] + """.content_details
                set main_artists = t5.main_artists
                from (
                        select content_id, array_agg(name order by credit_category desc, credit_order asc) as main_artists
                        from (
                                select *
                                from (
                                        select content_id, person_id, credit_category, credit_order, row_number() over (partition by content_id, credit_category order by credit_order) as my_rank
                                        from """ + config['sql']['schema'] + """.content_crew
                                        where credit_category in ('Directed by')
                                        and credit_order <= 2
                                        order by credit_order
                                    ) t1
                                where t1.my_rank = 1
                                union all
                                select *
                                from (
                                        select content_id, person_id, credit_category, credit_order, row_number() over (partition by content_id, credit_category order by credit_order) as my_rank
                                        from """ + config['sql']['schema'] + """.content_crew
                                        where credit_category in ('Cast')
                                        and credit_order <= 5
                                        order by credit_order
                                    ) t2
                                where t2.my_rank <= 2
                            ) t3
                        left join """ + config['sql']['schema'] + """.artists t4
                        on t3.person_id = t4.person_id
                        GROUP BY content_id
                     ) t5
                where """ + config['sql']['schema'] + """.content_details.content_id = t5.content_id
              """
    for query in sql.split(';'):
        print(query, '\n')
        trans = con.begin()
        con.execute(query)
        trans.commit()

    con.close()

    return True


def dump_similar_contents(engine):
    con = engine.connect()
    trans = con.begin()
    sql = """
          CREATE TABLE """ + config['sql']['schema'] + """.temp_contents (
          content_id int NOT NULL,
          similar_contents int[] NULL,
          filter_contents int[] NULL
          );


          copy """ + config['sql']['schema'] + """.temp_contents
          (content_id,filter_contents,similar_contents)
          FROM '/tmp/similar_contents.csv'
          WITH DELIMITER AS '^'
          CSV HEADER;


          update """ + config['sql']['schema'] + """.content_details
          set filtered_content = t1.filter_contents,
              similar_content = t1.similar_contents
          from """ + config['sql']['schema'] + """.temp_contents t1
          where """ + config['sql']['schema'] + """.content_details.content_id = t1.content_id;


          truncate table """ + config['sql']['schema'] + """.temp_contents;


          drop table """ + config['sql']['schema'] + """.temp_contents;
          """
    con.execute(sql)
    trans.commit()
    con.close()

    return True


def dump_streaming_info(engine):
    con = engine.connect()
    trans = con.begin()
    sql = """
          CREATE TABLE """ + config['sql']['schema'] + """.temp_streaming_info (
          imdb_id varchar not null,
          trailer_id varchar null,
          where_to_watch_australia varchar null,
          where_to_watch_brazil varchar null,
          where_to_watch_canada varchar null,
          where_to_watch_france varchar null,
          where_to_watch_germany varchar null,
          where_to_watch_india varchar null,
          where_to_watch_indonesia varchar null,
          where_to_watch_italy varchar null,
          where_to_watch_japan varchar null,
          where_to_watch_mexico varchar null,
          where_to_watch_philippines varchar null,
          where_to_watch_russia varchar null,
          where_to_watch_spain varchar null,
          where_to_watch_united_kingdom varchar null,
          where_to_watch_united_states varchar null
          );


          copy """ + config['sql']['schema'] + """.temp_streaming_info
          (imdb_id, trailer_id, where_to_watch_australia, where_to_watch_brazil, where_to_watch_canada, where_to_watch_france, where_to_watch_germany, where_to_watch_india, where_to_watch_indonesia, where_to_watch_italy, where_to_watch_japan, where_to_watch_mexico, where_to_watch_philippines, where_to_watch_russia, where_to_watch_spain, where_to_watch_united_kingdom, where_to_watch_united_states)
          FROM '/tmp/streaming_info.csv'
          WITH DELIMITER AS '^'
          CSV HEADER;

          update """ + config['sql']['schema'] + """.content_details
          set where_to_watch_australia = null,
              where_to_watch_brazil = null,
              where_to_watch_canada = null,
              where_to_watch_france = null,
              where_to_watch_germany = null,
              where_to_watch_india = null,
              where_to_watch_indonesia = null,
              where_to_watch_italy = null,
              where_to_watch_japan = null,
              where_to_watch_mexico = null,
              where_to_watch_philippines = null,
              where_to_watch_russia = null,
              where_to_watch_spain = null,
              where_to_watch_united_kingdom = null,
              where_to_watch_united_states = null;


          update """ + config['sql']['schema'] + """.content_details
          set where_to_watch_australia = t1.where_to_watch_australia,
              where_to_watch_brazil = t1.where_to_watch_brazil,
              where_to_watch_canada = t1.where_to_watch_canada,
              where_to_watch_france = t1.where_to_watch_france,
              where_to_watch_germany = t1.where_to_watch_germany,
              where_to_watch_india = t1.where_to_watch_india,
              where_to_watch_indonesia = t1.where_to_watch_indonesia,
              where_to_watch_italy = t1.where_to_watch_italy,
              where_to_watch_japan = t1.where_to_watch_japan,
              where_to_watch_mexico = t1.where_to_watch_mexico,
              where_to_watch_philippines = t1.where_to_watch_philippines,
              where_to_watch_russia = t1.where_to_watch_russia,
              where_to_watch_spain = t1.where_to_watch_spain,
              where_to_watch_united_kingdom = t1.where_to_watch_united_kingdom,
              where_to_watch_united_states = t1.where_to_watch_united_states
          from """ + config['sql']['schema'] + """.temp_streaming_info t1
          where """ + config['sql']['schema'] + """.content_details.imdb_content_id = t1.imdb_id;


          update """ + config['sql']['schema'] + """.content_details
          set youtube_trailer_id = t1.trailer_id
          from """ + config['sql']['schema'] + """.temp_streaming_info t1
          where """ + config['sql']['schema'] + """.content_details.imdb_content_id = t1.imdb_id
          and """ + config['sql']['schema'] + """.content_details.youtube_trailer_id is null;


          truncate table """ + config['sql']['schema'] + """.temp_streaming_info;


          drop table """ + config['sql']['schema'] + """.temp_streaming_info;
          """
    con.execute(sql)
    trans.commit()
    con.close()

    return True


def dump_content_images(engine):
    con = engine.connect()
    trans = con.begin()
    sql = """
          CREATE TABLE """ + config['sql']['schema'] + """.temp_contents (
          content_id int NOT NULL,
          tmdb_id int NULL,
          summary_text varchar NULL,
          poster varchar NULL,
          cover varchar NULL
          );


          copy """ + config['sql']['schema'] + """.temp_contents
          (content_id,tmdb_id,summary_text,poster,cover)
          FROM '/tmp/content_images.csv'
          WITH DELIMITER AS '^'
          CSV HEADER;


          update """ + config['sql']['schema'] + """.content_details
          set poster = null,
              cover = null;


          update """ + config['sql']['schema'] + """.content_details
          set poster = t1.poster,
              cover = t1.cover,
              tmdb_id = t1.tmdb_id
          from """ + config['sql']['schema'] + """.temp_contents t1
          where """ + config['sql']['schema'] + """.content_details.content_id = t1.content_id;


          update """ + config['sql']['schema'] + """.content_details
          set poster = 'https://flibo-images.s3-us-west-2.amazonaws.com/posters/no-poster.png'
          where poster is null;


          update """ + config['sql']['schema'] + """.content_details
          set cover = 'https://flibo-images.s3-us-west-2.amazonaws.com/covers/no-cover.jpg'
          where cover is null;


          update """ + config['sql']['schema'] + """.content_details
          set summary_text = t1.summary_text
          from """ + config['sql']['schema'] + """.temp_contents t1
          where """ + config['sql']['schema'] + """.content_details.content_id = t1.content_id
          and """ + config['sql']['schema'] + """.content_details.summary_text is null;


          update """ + config['sql']['schema'] + """.live_search
          set image = null
          where subject_type = 'content';


          update """ + config['sql']['schema'] + """.live_search
          set image = t1.poster
          from """ + config['sql']['schema'] + """.content_details t1
          where """ + config['sql']['schema'] + """.live_search.subject_id = t1.content_id
          and subject_type = 'content';


          truncate table """ + config['sql']['schema'] + """.temp_contents;


          drop table """ + config['sql']['schema'] + """.temp_contents;
          """
    con.execute(sql)
    trans.commit()
    con.close()

    return True


def dump_artist_images(engine):
    con = engine.connect()
    trans = con.begin()
    sql = """
          CREATE TABLE """ + config['sql']['schema'] + """.temp_artists (
          person_id int NOT NULL,
          picture varchar NULL
          );


          copy """ + config['sql']['schema'] + """.temp_artists
          (person_id,picture)
          FROM '/tmp/artist_images.csv'
          WITH DELIMITER AS '^'
          CSV HEADER;


          update """ + config['sql']['schema'] + """.artists
          set picture = null;


          update """ + config['sql']['schema'] + """.artists
          set picture = t1.picture
          from """ + config['sql']['schema'] + """.temp_artists t1
          where """ + config['sql']['schema'] + """.artists.person_id = t1.person_id;


          update """ + config['sql']['schema'] + """.artists
          set picture = 'https://flibo-images.s3-us-west-2.amazonaws.com/profile_pictures/artist.png'
          where picture is null;


          update """ + config['sql']['schema'] + """.live_search
          set image = null
          where subject_type = 'artist';


          update """ + config['sql']['schema'] + """.live_search
          set image = t1.picture
          from """ + config['sql']['schema'] + """.artists t1
          where """ + config['sql']['schema'] + """.live_search.subject_id = t1.person_id
          and subject_type = 'artist';


          truncate table """ + config['sql']['schema'] + """.temp_artists;


          drop table """ + config['sql']['schema'] + """.temp_artists;
          """
    con.execute(sql)
    trans.commit()
    con.close()

    return True
