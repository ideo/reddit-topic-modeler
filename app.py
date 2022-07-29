import os
import json

import pandas as pd
import streamlit as st

import logic as lg
from scrape import Scrape


st.set_page_config(
    page_title="Scrape Reddit",
    page_icon="img/reddit-logo.png"
)


st.header("Scrape Reddit")


config = lg.initialize_session_state()

st.write("")
st.subheader("Choose Subreddits")
new_one = st.text_input(
    "Add new subreddits to the list.",
)
if new_one and new_one not in st.session_state["subreddits"]:
    st.session_state["subreddits"].append(new_one)

_ = st.multiselect(
    "Which subreddits should we scrape?",
    options=st.session_state["subreddits"],
    default=config["subreddits"],
    key = "subreddits_to_scrape"
)


st.write("")
st.subheader("Enter Keywords")
label1 = "Enter a search phrase to scrape for, such as 'water' or 'drought risk'."
label2 = "Finalize selections."
key1 = "keywords"
key2 = "keywords_to_scrape"
lg.multiselect_with_addition(label1, label2, key1, key2)


st.write("")
st.subheader("Scrape the Internet!")
st.write("Actually, just Reddit.")
scraper = Scrape()
scrape = st.button("Scrape!")

if scrape:
    lg.save_config_file(config)
    scraper.scrape()

if scraper.documents:
    for kw, output_file in zip(st.session_state["keywords_to_scrape"], scraper.output_files):
        df = pd.read_csv(output_file)
        df.drop(columns=["Unnamed: 0"], inplace=True)
        st.markdown(f"**{kw}**")
        st.dataframe(df)
    # st.dataframe(scraper.documents)


