import datetime, calendar
import os.path

from dateutil.relativedelta import relativedelta
from os import stat
import requests
import pandas as pd
import json
import time

TODAY = datetime.datetime.utcnow()


class Reddit:
    def __init__(self, restart_from_file, start_date, end_date, keyword, include_comments, save_every=None, out_file=None, subreddit=None):
        """Class initialization

        Args:
            start_date (datetime): starting date for the data pull
            end_date (datetime): ending date for the data pull
            keyword (string): we'll pull posts containing this keyword
            include_comments (boolean): whether to include comments when pulling posts
            save_every (int, optional): save interstitial results every n calls
            out_file (str, optional): file to save interstitial results to
            subreddit (str, optional): which subreddit to filter on. if none, searches all subreddits.
        """
        self.start_date = start_date
        self.end_date = end_date
        self.keyword = keyword
        self.save_every = save_every

        self.subreddit = subreddit
        self.restart_from_file = restart_from_file

        self.out_file = f"{out_file}_main.pkl"
        df_main = self.get_posts_in_date_range(comment=False)

        if include_comments:
            self.out_file = f"{out_file}_comments.pkl"
            df_comments = self.get_posts_in_date_range(comment=True)
            self.posts = df_main.append(df_comments)
        else:
            self.posts = df_main


    def get_initial_df(self, comment):
        """This function loads the initial dataframe.

        Args:
            comment (boolean): whether we're pulling a main post or a comment

        Returns:
            dataframe
        """
        # fist data pull for a given keyword
        if self.restart_from_file and os.path.exists(self.out_file):
            print(f"Reading from file {self.out_file}")
            return pd.read_pickle(self.out_file)                
        else:
            return self.get_pull_df(comment, df_previous_pull=None)


    def get_posts_in_date_range(self, comment):
        """This function pulls posts in a user defined date range
        and containing a specific keyword.
        Because of Reddit rate limit, we can pull data in chunks.

        Args:
            comment (boolean): pulling main post or comments

        Returns:
            dataframe containing posts
        """

        # fist data pull for a given keyword
        df = self.get_initial_df(comment)

        if df.empty:
            print(f"\n\n***\nno submissions for keyword={self.keyword}\n***\n\n")
            return pd.DataFrame()
        # adding more data a little bit at the time.
        else:
            print(
                f"\n\n* starting data time interval from {df['created_utc'].min()} to {df['created_utc'].max()}"
            )

            for i in range(1000000):
                df_step = self.get_pull_df(comment, df_previous_pull=df)
                
                if df_step.empty:
                    break

                df = df.append(df_step, ignore_index=True)

                print(
                    f"\n* current data time interval from {df_step['created_utc'].min()} to {df_step['created_utc'].max()}, count = {len(df_step)}"
                )
                if self.save_every and self.out_file:
                    if i > 0 and i % self.save_every == 0:
                        print('Saving interstitial...')
                        if os.path.exists(self.out_file):
                            current_df = pd.read_pickle(self.out_file)
                            combined = current_df.append(df, ignore_index=True)
                            combined.drop_duplicates().to_pickle(self.out_file)
                        else:
                            df.to_pickle(self.out_file)                            

                # need a sleeper cause Reddit has a rate limit
                time.sleep(2)

            df.to_pickle(self.out_file)
            return df


    def get_pull_df(self, comment, df_previous_pull):
        """This function determines the time interval for
        a query based on previously pulled data.
        It then pulls data in that time interval and containing a given
        keyword.

        Args:
            comment (boolean): whether we are pulling main posts or comments
            df_previous_pull (dataframe): date pulled so far

        Returns:
            dataframe with data in new time interval
        """

        #during the first pull, these are the dates in the confi.json. 
        #for any subsequent pull, the start date is taken from the data pulled so dar
        start_date, end_date = self.get_dates(df_previous_pull)
        start_date_human_readable = datetime.datetime.utcfromtimestamp(start_date)        
        
        #FIXME I think this condition is never encountered...but anyway...
        if (
                start_date_human_readable > TODAY
                or start_date_human_readable > self.end_date
        ):
            print("\n\n***\nData pulled up to today...\n***\n\n")
            return pd.DataFrame()
        else:
            return Reddit.pull_posts(start_date, end_date, self.keyword, comment, self.subreddit)


    def get_dates(self, df_previous_pull):
        """This function computes the time interval for a data pull.
        - For the first data pull, the start and end dates are those provided in
        the configuration file.
        - For any subsequent pull, the start date is recalculated based on the
        last post from the previous pull.
        This approach is needed to get around Reddit rate limit.

        Args:
            df_previous_pull (dataframe): dataframe containing the data pulled so far

        Returns:
            tuple containing start and end dates in unix time
        """
        if df_previous_pull is None:
            my_date = datetime.datetime(
                self.start_date.year, 
                self.start_date.month, 
                self.start_date.day).date()
            start_date_unix = calendar.timegm(my_date.timetuple())

        else:
            # Using as start datetime the last of the previous pull.
            start_date_unix = df_previous_pull[f"created_utc_unix"].max()

        my_date = datetime.datetime(
                self.end_date.year, 
                self.end_date.month, 
                self.end_date.day).date()
        end_date_unix = calendar.timegm(my_date.timetuple())

        print(f"Start date = {datetime.datetime.utcfromtimestamp(start_date_unix)}")
        print(f"End date = {datetime.datetime.utcfromtimestamp(end_date_unix)}")

        return start_date_unix, end_date_unix


    @staticmethod
    def get_pushshift_data(url):
        """This function queries Reddit pushshift API.

        Args:
            url (string): the url to query

        Returns:
            json response
        """
        print(url)
        r = requests.get(url)
        try:
            json_respose = json.loads(r.text)            
            return pd.DataFrame(json_respose["data"])
        except:
            return pd.DataFrame()


    @staticmethod
    def pull_posts(start_date, end_date, keyword, comment, subreddit=None):
        """This function queries the Reddit pushshift API
        for posts containing a give keyword and in a given time range.

        Args:
            start_date (int): start datetime in unix time
            end_date (int): end datetime in unix time
            keyword (string): keyword that a post needs to contain
            comment (boolean): whether to pull main posts or comments
            subreddit (str, optional): which subreddit to filter on. if none, searches all subreddits.

        Returns:
            dataframe containing the posts
        """
        url = Reddit.create_url(start_date, end_date, keyword, comment, subreddit)
        df = Reddit.get_pushshift_data(url)
        
        if df.empty:
            return pd.DataFrame()

        df = Reddit.clean_df(df, keyword)

        if comment: df['is_comment'] = True
        else: df['is_comment'] = False

        return df


    @staticmethod
    def clean_df(df, keyword):
        """This function contains the cleaning rules for the posts.

        Args:
            df (dataframe): original dataframe
            keyword (string): keyword the search is based on

        Returns:
            dataframe with less and re-organized columns
        """

        cols_rename_dict = {"selftext": "body", "created_utc": "created_utc_unix"}
        cols_to_keep = [
            "title",
            "score",
            "id",
            "subreddit",
            "url",
            "num_comments",
            "body",
            "created_utc_unix",
        ]

        for c in ["title", "url"]:
            if c not in df.columns:
                df[c] = ""

        if "num_comments" not in df.columns:
            df["num_comments"] = 0

        new_df = df.rename(columns=cols_rename_dict).copy()

        # FIXME ADD A TEST HERE TO CHECK COLUMNS WERE MAPPED CORRECTLY
        if new_df.empty:
            return new_df

        new_df = new_df[cols_to_keep].copy()
        # FIXME ADD A TEST HERE TO CHECK TIME WAS CONVERTED CORRECTLY
        new_df["created_utc"] = new_df["created_utc_unix"].apply(
            lambda x: datetime.datetime.utcfromtimestamp(x).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        new_df["keyword"] = keyword
        new_df.reset_index(inplace=True, drop=True)
        return new_df


    @staticmethod
    def create_url(after, before, keyword, comment=False, subreddit=None):
        """This function creates a Reddit url.

        Args:
            after (int): only posts after this time in unix time will be pulled
            before (int): only posts before this time in unix time will be pulled
            keyword (string): keyword the search is based on
            comment (bool, optional): whether we're pulling main posts or their comments. Defaults to False.
            subreddit (str, optional): which subreddit to filter on. if none, searches all subreddits.

        Returns:
            string: a url for the posts to query
        """
        if comment:
            # pulling comments
            url = f"https://api.pushshift.io/reddit/comment/search/?q={keyword}&size=500&after={after}&before={before}"
        else:
            # pulling main post
            url = f"https://api.pushshift.io/reddit/search/submission/?q={keyword}&size=1000&after={after}&before={before}"

        if subreddit:
            url += f'&subreddit={subreddit}'

        return url
