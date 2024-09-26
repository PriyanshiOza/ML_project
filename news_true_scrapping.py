import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re


# Function to fetch data from The Hindu National(India) section
def fetch_the_hindu_india():
    url = "https://www.thehindu.com/news/national/"
    return fetch_data_from_url(url, "India")


# Function to fetch data from The Hindu Live section
def fetch_the_hindu_live():
    url = "https://www.thehindu.com/news/"
    return fetch_data_from_url(url, "Live")


# Function to fetch data from The Hindu World section
def fetch_the_hindu_world():
    url = "https://www.thehindu.com/news/international"
    return fetch_data_from_url(url, "World")


# Function to fetch data from The Hindu States section
def fetch_the_hindu_state():
    url = "https://www.thehindu.com/news/states"
    return fetch_data_from_url(url, "States")


# Function to fetch data from The Hindu Cities section
def fetch_the_hindu_city():
    url = "https://www.thehindu.com/news/cities"
    return fetch_data_from_url(url, "Cities")


# Function to fetch data from The Hindu Sport section
def fetch_the_hindu_sport():
    url = "https://www.thehindu.com/sport"
    return fetch_data_from_url(url, "Sports")


# Function to fetch data from The Hindu Technology section
def fetch_the_hindu_technology():
    url = "https://www.thehindu.com/sci-tech/technology"
    return fetch_data_from_url(url, "Technology")


# Function to fetch data from The Hindu Science section
def fetch_the_hindu_science():
    url = "https://www.thehindu.com/sci-tech/science"
    return fetch_data_from_url(url, "Science")


# Function to fetch data from The Hindu Education section
def fetch_the_hindu_education():
    url = "https://www.thehindu.com/education"
    return fetch_data_from_url(url, "Education")


# Function to fetch data from The Hindu Business section
def fetch_the_hindu_business():
    url = "https://www.thehindu.com/business"
    return fetch_data_from_url(url, "Business")


# Function to fetch data from The Hindu Entertainment section
def fetch_the_hindu_entertainment():
    url = "https://www.thehindu.com/entertainment"
    return fetch_data_from_url(url, "Entertainment")


# Function to clean and preprocess headlines and content
def clean_and_preprocess(text):
    if pd.isna(text):
        return text

    # Remove extra whitespace
    text = text.strip()

    # Replace HTML entities and special characters
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&quot;", '"', text)
    text = re.sub(r"&#039;", "'", text)
    text = re.sub(r"\s+", " ", text)  # Replace multiple spaces with a single space

    return text


# Function to remove float values from the DataFrame
def remove_floats(dataframe):
    # Replace float values with an empty string or NaN
    return dataframe.applymap(lambda x: "" if isinstance(x, float) else x)


# Helper function to fetch data from a given URL
def fetch_data_from_url(url, category):
    response = requests.get(url)
    if response.status_code == 200:
        html_content = response.content
        soup = BeautifulSoup(html_content, "html.parser")

        headlines = []
        links = []
        categories = []
        contents, publication_dates = [], []

        for item in soup.find_all("h3", class_="title"):
            headline_text = item.get_text().strip()
            link = item.find("a").get("href")

            if headline_text and link:
                headlines.append(headline_text)
                links.append(link if link.startswith("http") else url + link)
                categories.append(category)  # Set label to the given label

        for link in links:
            article_response = requests.get(link)
            if article_response.status_code == 200:
                article_soup = BeautifulSoup(article_response.content, "html.parser")
                article_body = article_soup.find("h2", class_="sub-title")
                publication_date_element = article_soup.find(
                    "p", class_="publish-time-new"
                )

                contents.append(
                    article_body.get_text().strip()
                    if article_body
                    else "Content not found"
                )

                publication_date_span = (
                    publication_date_element.find("span")
                    if publication_date_element
                    else None
                )
                publication_date_text = (
                    publication_date_span.get_text().strip().replace("-", "")
                    if publication_date_span
                    else "Date not found"
                )
                # Extract only the date part from the publication date string
                # Example format: "August 11, 2024 12:12 pm IST  Dhaka"
                # We want only "August 11, 2024"
                date_only = re.search(r"\w+ \d{1,2}, \d{4}", publication_date_text)
                publication_dates.append(
                    date_only.group() if date_only else "Date not found"
                )
            else:
                contents.append("Failed to retrieve article")
                publication_dates.append("Failed to retrieve date")

        # Apply cleaning and preprocessing
        headlines = [clean_and_preprocess(h) for h in headlines]
        contents = [clean_and_preprocess(c) for c in contents]
        dataframe = pd.DataFrame(
            {
                "Category": categories,
                "Headline": headlines,
                "Content": contents,
                "Published Date": publication_dates,  # Change the column name here
            }
        )

        # Remove float values from the dataframe
        dataframe = remove_floats(dataframe)

        return dataframe

    else:
        print(f"Failed to retrieve webpage from {url}")
        return pd.DataFrame()


# Combine data from all sections
def fetch_data():
    dataframes = [
        fetch_the_hindu_india(),
        fetch_the_hindu_live(),
        fetch_the_hindu_world(),
        fetch_the_hindu_state(),
        fetch_the_hindu_city(),
        fetch_the_hindu_sport(),
        fetch_the_hindu_technology(),
        fetch_the_hindu_science(),
        fetch_the_hindu_education(),
        fetch_the_hindu_business(),
        fetch_the_hindu_entertainment(),
    ]
    return pd.concat(dataframes, ignore_index=True)


# Save data to CSV file without duplicates and without floats
def save_data_to_csv(data):
    file_path = "news_true.csv"

    # Add a new column "Label" with the value True
    data["Label"] = 1

    # Function to extract only the date from the published date
    def extract_date_only(date_text):
        date_only = re.search(r"\w+ \d{1,2}, \d{4}", date_text)
        return date_only.group() if date_only else date_text

    # Apply date extraction to the new data
    data["Published Date"] = data["Published Date"].apply(extract_date_only)

    if os.path.isfile(file_path):
        existing_data = pd.read_csv(file_path)

        # Rename the "Publication Date" column in the existing data to "Published Date"
        if "Publication Date" in existing_data.columns:
            existing_data.rename(
                columns={"Publication Date": "Published Date"}, inplace=True
            )

        # Apply cleaning and preprocessing to existing data
        existing_data["Headline"] = existing_data["Headline"].apply(
            clean_and_preprocess
        )
        existing_data["Content"] = existing_data["Content"].apply(clean_and_preprocess)

        # Ensure all existing data also has Label set to True
        existing_data["Label"] = True

        # Apply date extraction to the existing data
        existing_data["Published Date"] = existing_data["Published Date"].apply(
            extract_date_only
        )

        # Remove float values from the existing data
        existing_data = remove_floats(existing_data)

        # Combine new data with existing data
        combined_data = pd.concat([existing_data, data], ignore_index=True)

        # Drop duplicates based on the "Headline" column
        combined_data = combined_data.drop_duplicates(subset="Headline").reset_index(
            drop=True
        )

    else:
        combined_data = data

    # Remove float values from the combined data
    combined_data = remove_floats(combined_data)

    # Sort the combined data by the "Category" column
    combined_data = combined_data.sort_values(by="Category").reset_index(drop=True)

    combined_data.to_csv(file_path, index=False, encoding="utf-8-sig")
    print("Data saved to news_true.csv")


# Main execution
combined_data = fetch_data()
save_data_to_csv(combined_data)
