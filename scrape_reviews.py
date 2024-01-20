import pandas as pd
from tqdm import tqdm
import os
import random
import time
import requests
from scrapy import Selector

# You should download this file from https://datasets.imdbws.com/ and unzip it here
movie_names_file = "./title.basics.tsv"
# We save the movie ids that we have already processed here so we can restart the script
saved_movie_ids_file = "./saved_movie_ids.txt"


# Get the movie ids that we have already processed from the file
def get_saved_movie_ids():
    saved_movie_ids = set()
    if not os.path.exists(saved_movie_ids_file):
        return saved_movie_ids
    with open(saved_movie_ids_file, "r") as f:
        for line in f:
            saved_movie_ids.add(line.strip())
    return saved_movie_ids


# Update the file with the movie id that we have just processed
def update_saved_movie_ids(movie_id):
    with open(saved_movie_ids_file, "a") as f:
        f.write(movie_id + "\n")


# Save reviews of a movie under ./reviews/movie_id.csv
def save_movie_reviews(movie_id, reviews_df):
    os.makedirs("./reviews", exist_ok=True)
    file_name = "./reviews/" + movie_id + ".csv"
    reviews_df.to_csv(file_name, sep="\t", index=False)  # Save reviews
    update_saved_movie_ids(movie_id)  # Save movie id


# Get the movie ids that we have already processed so we don't process them again
processed_movies = get_saved_movie_ids()

# Read the movie names
movie_names = pd.read_csv(movie_names_file, sep="\t")

# Randomly shuffle the movies because they are ordered weirdly
movie_names = movie_names.sample(frac=1).reset_index(drop=True)

# This is how we get the url for a movie
url_base = "https://www.imdb.com/title/"

# Iterate over all movies
for index, row in tqdm(movie_names.iterrows()):
    try:
        movie_id = row["tconst"]
        # Skip if we have already processed this movie
        if movie_id in processed_movies:
            continue
        # This is the url for the reviews of a movie
        url = url_base + movie_id + "/reviews"

        # Get the html of the page
        response = requests.get(url)

        # If we get an error, skip this movie
        if response.status_code != 200:
            print("Error: ", response.status_code)
            continue

        # Parse the html
        selector = Selector(text=response.text)

        # Get all reviews
        reviews = selector.css(".review-container")

        all_reviews = {
            "title": [],
            "text": [],
            "rating": [],
            "movie_id": [],
        }

        # Iterate over all reviews to populate all_reviews
        for review in reviews:
            # First get the title, text and rating of the review
            review_title = review.css(".title::text").get()
            review_text = review.css(".text::text").get()
            review_rating = review.css(".rating-other-user-rating span::text").get()

            # If they are empty or None, skip this review
            if review_title is None:
                continue
            if review_text is None:
                continue
            if review_rating is None:
                continue

            # strip all to remove the enter at the end and other spaces
            review_title = review_title.strip()
            review_text = review_text.strip()
            review_rating = review_rating.strip()

            # Add it to all_reviews
            all_reviews["title"].append(review_title)
            all_reviews["text"].append(review_text)
            all_reviews["rating"].append(review_rating)
            all_reviews["movie_id"].append(movie_id)

        # Mark this movie as processed
        processed_movies.add(movie_id)

        # Skip saving if no reviews to not spam the folder
        if len(all_reviews["title"]) == 0:
            continue

        # Create a pandas dataframe from all_reviews and then save it
        review_df = pd.DataFrame(all_reviews)
        save_movie_reviews(movie_id, review_df)

        # Pick random sleeping time between 100 and 1000 ms
        # I wanted to be careful not to spam the website and get banned
        time.sleep(random.randint(100, 400) / 1000)
    except Exception as e:
        print(e)
        time.sleep(
            1800
        )  # Sleep for 30 minutes if we get an error to not spam the website
        continue


# Find the files that we have saved
files = os.listdir("./reviews")

dfs = []

# Read them all and concatenate them into one dataframe
for file in files:
    cur_df = pd.read_csv("./reviews/" + file, sep="\t")
    dfs.append(cur_df)


all_reviews = pd.concat(dfs, ignore_index=True)

# Save the dataframe
all_reviews.to_csv("./scraped_reviews.csv", sep="\t", index=False)
