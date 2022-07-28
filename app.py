import os
import json
import streamlit as st

import logic as lg
from scrape import Scrape
from topic_model import topic_model


st.header("Scrape Reddit")


config = lg.initialize_session_state()

st.write("")
st.subheader("Subreddits")
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
# st.session_state["subreddits_to_scrape"] = chosen

# print(st.session_state["subreddits_to_scrape"])


st.write("")
st.subheader("Keywords")
search_term = st.text_input("Enter a keyword to search for.")
if search_term and search_term not in st.session_state["keywords"]:
    search_term = search_term.replace(" ", "+").lower()
    st.session_state["keywords"].append(search_term)

st.markdown(
    f"""
    We will be scraping for these terms: 
    ```
    {st.session_state["keywords"]}
    ```
    """
)

st.write("")
scraper = Scrape()

scrape = st.button("Scrape!")

if scrape:
    lg.save_config_file(config)
    scraper.scrape()


if scraper.documents:
    st.dataframe(scraper.documents)

    topics = topic_model(scraper.documents)
    st.dataframe(topics)


