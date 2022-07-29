
import os
import json

import pandas as pd
import streamlit as st


def initialize_session_state():
    with open("config_default.json") as config_file:
        config = json.load(config_file)

    if "subreddits" not in st.session_state:
        st.session_state["subreddits"] = config["subreddits"]

    if "keywords" not in st.session_state:
        st.session_state["keywords"] = []

    return config


def multiselect_with_addition(label1, label2, key_default, key_selections):
    """
    """
    new_addition = st.text_input(label1)
    if new_addition and new_addition not in st.session_state[key_default]:
        st.session_state[key_default].append(new_addition)

    _ = st.multiselect(
        label2, 
        options=st.session_state[key_default],
        default=st.session_state[key_default],
        key=key_selections
        )


def save_config_file(config):
    config["subreddits"] = st.session_state["subreddits_to_scrape"]
    config["keywords"] = list(set(st.session_state["keywords"]))

    with open("config.json", "w") as outfile:
        json.dump(config, outfile)


# def load_scraped_data(filename):
#     with open(filename) as output_file:
#         scraped_data = json.load(output_file)

#     documents = []
#     for record in scraped_data:
#         if len(record["title"]) > 0:
#             documents.append(record["title"])
#         documents.append(record["body"])
#     # documents = [record["title"] for record in scraped_data]
#     return documents


def display_scraped_data_in_tables(output_files):
    

    if len(output_files) == 1:
        df = load_dataframe_from_csv(output_files[0])
        st.table(df)

    else:
        filenames = {os.path.basename(path): path for path in output_files}

        for filename in filenames.keys():
            keyword = filename.split("_")[1]
            df = load_dataframe_from_csv(filenames[filename])
            st.write(f"{keyword}")
            st.dataframe(df)


def load_dataframe_from_csv(filepath):
    df = pd.read_csv(filepath)
    df.drop(columns=["Unnamed: 0"], inplace=True)
    return df
