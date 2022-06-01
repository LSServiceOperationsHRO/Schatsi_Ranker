""" MAIN FILE OF THE SCHATSI_Ranker
This file calls the functions for ranking the files and for negative filtering the results
"""

# import all needed libraries
import csv
from datetime import datetime
from variables_ranking import *
import pandas as pd
import SCHATSI_RANKING
import SCHATSI_negative_filter


def main():

# timestamp
    start = datetime.now()
    print("Execution started at", start.isoformat())
# prepare dataframes for later output
    runtime = []
    datetime_format = "%m/%d/%Y %H:%M:%S"

# Find the file for functional terms, the negative terms and the found terms from SCHATSI_Data_Cleanser
    print("Fetching parameters...")
    try:
        functional_terms = pd.read_csv(SCHATSI_FUNCTIONAL_TERMS, sep=';')
        print("functional terms found")
        print(functional_terms)
    except:
        print("functional terms not found, or import of the terms not possible, moving on without them")
        pass

    try:
        terms_df = pd.read_csv(SCHATSI_TERMS, sep=';')
        print("schatsi_terms found")
    except:
        print("schatsi_terms.csv not found or import not possible, moving on without them")
        pass

    try:
        negative_terms = pd.read_csv(SCHATSI_NEGATIVE_TERMS, sep=';')
        print("negative_terms found")
    except:
        print("negative_terms.csv not found or import not possible, moving on without them ")
        pass
    print("done")

# timestamp for ranking
    start_ranking = datetime.now()

# call ranking function with the csv-files as parameters given
    try:
        ranking_df = SCHATSI_RANKING.ranking(functional_terms, terms_df)
    except:
        ranking_df = pd.DataFrame(columns=["X", "filename", "sum_functional_terms", "sum_terms", "result"])
        print("Ranking function was not successful, ranking file will be empty.")


# second timestamp for ranking
    finish_ranking = datetime.now()
    duration_ranking = (finish_ranking - start_ranking).seconds / 60
    runtime.append(['SCHATSI_ranking', start_ranking.strftime(datetime_format), finish_ranking.strftime(datetime_format), duration_ranking])

# timestamp for negative filtering
    start_neg_filter = datetime.now()

# call negative filtering function with csv-files given as parameters
    try:
        neg_filter = SCHATSI_negative_filter.negative_filter(negative_terms)
    except:
        neg_filter = [[]]

# second timestamp for negative filtering
    finish_neg_filter = datetime.now()
    duration_neg_filter = (finish_neg_filter - start_neg_filter).seconds / 60
    runtime.append(['SCHATSI_negative_filtering', start_neg_filter.strftime(datetime_format), finish_neg_filter.strftime(datetime_format), duration_neg_filter])

# convert results from lists into pandas dataframes and write them into a csv-file
    print("Saving output files...", end="", flush=True)
    outputs = [
        ['schatsi_ranking.csv', ranking_df],
        ['schatsi_Ranker_runtime.csv', pd.DataFrame(runtime, columns=['process', 'start processing', 'end processing', 'duration'])],
        ['schatsi_negative_filter.csv', pd.DataFrame(neg_filter, columns=['filename', 'negative term', 'negative term count'])]
    ]
    # Creating the output-files
    for output in outputs:
        output[1].to_csv(r"{}/{}".format(SCHATSI_OUTPUT_FOLDER, output[0]), mode="wb", encoding="utf-8", sep=';',
                         index=False)

    print("done")
#

if __name__ == "__main__":
    main()