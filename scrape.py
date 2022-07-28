import os
import json
from pathlib import Path
from datetime import datetime

import pandas as pd

from reddit import Reddit


CURRENT_DIR = os.getcwd()
OUTPUT_FOLDER = f"{CURRENT_DIR}/data"
OUTPUT_FOLDER_IN_PROGRESS = f"{OUTPUT_FOLDER}/in_progress"


class Scrape:
    def __init__(self):
        # self.parse_config_file()
        # self.create_output_folder()
        self.documents = []


    def scrape(self):
        self.parse_config_file()
        self.create_output_folder()

        self.output_files = []
        for n, keyword in enumerate(self.keywords):
            filename = self.scrape_keyword(n, keyword)
            self.output_files.append(filename)

        self.clean_for_topic_modeling()
        

    def parse_config_file(self):
        with open(Path("config.json")) as config_file:
            config = json.load(config_file)

        self.keywords = list(set(config["keywords"]))
        restart_from_file = config["restart_from_file"].lower()
        self.restart_from_file = restart_from_file == "true"

        self.start_date_str = config["start_date"]
        self.end_date_str = config["end_date"]
        self.start_date = datetime.strptime(self.start_date_str, '%Y-%m-%d')
        self.end_date = datetime.strptime(self.end_date_str, '%Y-%m-%d')

        include_comments = config["include_comments"].lower()
        self.include_comments = include_comments == "true"
        self.save_every = config.get('save_every')
        self.file_format = config.get("file_format")

        subreddits = config.get('subreddits', [])
        
        if isinstance(subreddits, str):
            self.subreddits = [subreddits]
        else:
            self.subreddits = subreddits

        print(f"""
            keywords = {self.keywords}
            include_comments = {self.include_comments}
            """)


    def create_output_folder(self):
        # creating folder if it doesn't exist
        for this_folder in [OUTPUT_FOLDER, OUTPUT_FOLDER_IN_PROGRESS]:
            if not os.path.exists(this_folder):
                os.makedirs(this_folder)


    def scrape_keyword(self, n, keyword):
        print(f"\nkeyword = {keyword} ({n} out of {len(self.keywords)})")

        # create filename for intermediate states file:
        filename = (
            f"{OUTPUT_FOLDER_IN_PROGRESS}/reddit_{keyword}_{self.start_date_str}_{self.end_date_str}"
        )
        
        #create filename for final file
        filename_complete = (
            f"{OUTPUT_FOLDER}/reddit_{keyword}_{self.start_date_str}_{self.end_date_str}_complete.{self.file_format}"
        )

        # skipping the rest of the loop if the file exists (this is to avoid re-downloading)
        if os.path.isfile(filename_complete):
            print(f"\n\n***\n{keyword} file present\n***\n\n")

        else:
            if not self.subreddits:
                my_reddit = Reddit(
                    self.restart_from_file, 
                    self.start_date, 
                    self.end_date, 
                    keyword, 
                    self.include_comments, 
                    self.save_every, 
                    filename)
                posts = my_reddit.posts
            else:
                all_posts = []
                for subreddit in self.subreddits:
                    my_reddit = Reddit(
                        self.restart_from_file, 
                        self.start_date, 
                        self.end_date, 
                        keyword, 
                        self.include_comments, 
                        self.save_every, 
                        filename, 
                        subreddit)
                    all_posts.append(my_reddit.posts.copy())

                posts = pd.concat(all_posts)

            if posts.empty:
                print(f"\n\n***\nNo posts for this keyword...\n***\n\n")
                # continue

            posts.drop_duplicates(inplace=True)

            if self.file_format == "pkl":
                posts.to_pickle(filename_complete)            
            elif self.file_format == "json":
                posts.to_json(filename_complete, orient="records", lines=False)
            elif self.file_format == "csv":
                posts.to_csv(filename_complete)
            
            #remove the intermediate files
            for f in [f"{filename}_main.pkl", f"{filename}_comments.pkl"]:
                if os.path.isfile(f):
                    os.remove(f)

        return filename_complete


    def clean_for_topic_modeling(self):
        # self.documents = []
        for filepath in self.output_files:
            df = pd.read_csv(filepath)
            for ttl, body in zip(df["title"].values, df["body"].values):
                if isinstance(ttl, str) and ttl != "":
                    self.documents.append(ttl)
                if isinstance(body, str) and body != "":
                    self.documents.append(body)