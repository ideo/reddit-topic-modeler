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
label1 = "Add new subreddits to the list."
label2 = "Which subreddits should we scrape?"
key1 = "subreddits"
key2 = "subreddits_to_scrape"
lg.multiselect_with_addition(label1, label2, key1, key2)


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
scraper = Scrape(skip_if_file_exists=False)
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
    
        st.download_button("Download CSV",
            df.to_csv().encode("utf-8"),
            f"{kw}.csv",
            "text/csv",
            key=f"{kw}_dwnld"
            )


