import pandas as pd
from bertopic import BERTopic


def topic_model(docs):
    model = BERTopic(low_memory=True)
    topics, probs = model.fit_transform(docs)
    return topics


def clean_for_topic_modeling(output_files):
    documents = []
    for filepath in output_files:
        df = pd.read_csv(filepath)
        for ttl, body in zip(df["title"].values, df["body"].values):
            if isinstance(ttl, str) and ttl != "":
                documents.append(ttl)
            if isinstance(body, str) and body != "":
                documents.append(body)
    return documents



if __name__ == "__main__":
    output_files = [
        "data/reddit_water_2022-03-01_2022-03-30_complete.csv"
    ]

    documents = clean_for_topic_modeling(output_files)
    print(documents)
    topics = topic_model(documents)
    print(topics)
