# SCHATSI RANKING FUNCTION FOR DOCKER SCHATSI_RANKER
import pandas as pd

def ranking(functional_terms_input, terms_input):
    # local path

    # docker path
    # functional_terms_input = pandas.read_csv('/data/input/SCHATSI_functional_terms.csv', sep=';')
    # terms_input = pandas.read_csv('/data/output/SCHATSI_terms.csv', sep=';')

    # a list with all functional terms, which will be used later for the ranking
    # func = []
    # for index, row in functional_terms_input.iterrows():
    #     func.append(row['term'])

    # # finding all entries with a functional word as a term in "SCHATSI_terms.csv"
    # # And write them in the Pandas Dataframe "terms_df"
    # terms = []
    # for element in func:
    #     query_search = 'term == \"' + element + "\""
    #     term_found = terms_input.query(query_search)
    #     terms.append(term_found.values.tolist())
    #     pass
    # terms_df = pd.DataFrame(terms, columns=terms_input.columns)
    terms_df = functional_terms_input.join(terms_input.set_index('term'), on='term', how='inner')

    # preparation for building global sum of filtered words from "SCHATSI_terms.csv", for every file in the csv-file
    term_filenames_column = terms_input['filename']
    term_filenames = term_filenames_column.drop_duplicates()
    filename_list = []
    for index, value in term_filenames.items():
        filename_list.append(value)

    # global sum of FILTERED WORDS from SCHATSI_terms.csv
    global_sum = []
    # sum of FOUND FUNCTIONAL TERMS in SCHATSI_terms.csv
    sum_found_func_terms = []
    for element in filename_list:
        global_sum.append([element, 0])
        sum_found_func_terms.append([element, 0])
    global_sum_df = pd.DataFrame(global_sum, columns=['filename', 'sum_terms'])
    sum_found_func_terms_df = pd.DataFrame(sum_found_func_terms, columns=['filename', 'sum_functional_terms'])

    # fill Dataframe for global sum of filtered words for every file in "SCHATSI_terms.csv"
    for index, row in terms_input.iterrows():
        add = row['term count']
        global_sum_df.loc[global_sum_df.filename == row['filename'], 'sum_terms'] += add

    # fill dataframe for sum of functional words from "SCHATSI_functional_terms.csv" for every file in "SCHATSI_terms"
    for index, row in terms_df.iterrows():
        add = row['term count']
        sum_found_func_terms_df.loc[sum_found_func_terms_df.filename == row['filename'], 'sum_functional_terms'] += add

    # calculate the results by dividing the sum of functional terms by the global sum for each file
    merged_df = sum_found_func_terms_df.merge(global_sum_df, how='inner', on='filename')

    merged_df = merged_df.reindex(columns=['filename', 'sum_functional_terms', 'sum_terms', 'result'])
    merged_df['result'] = merged_df['sum_functional_terms'].div(merged_df['sum_terms'])

    # order them from the highest score to the smallest
    merged_df = merged_df.sort_values('result', ascending=False)

    # Drop out the Columns with the Sum of functional terms and the global sum of terms
    # merged_df.drop(['sum_functional_terms', 'sum_terms'], axis=1)
    return merged_df
