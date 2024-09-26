import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import os


# Function to clean and preprocess text
def clean_and_preprocess(text):
    if not text:
        return ""

    text = text.strip()
    text = re.sub(r"\u2019", "'", text)
    text = re.sub(r"\u2018", "'", text)
    text = re.sub(r"\u201c", '"', text)
    text = re.sub(r"\u201d", '"', text)
    text = re.sub(r"\u2013", "-", text)
    text = re.sub(r"\u2014", "-", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&quot;", '"', text)
    text = re.sub(r"&#039;", "'", text)
    text = re.sub(r"\s+", " ", text)

    return text


def convert_boom_live_date(date_text):
    formats = [
        "%d %b %Y %I:%M %p GMT",
        "%d %B %Y %I:%M %p GMT",
        "%d %b %Y %H:%M %p GMT",
        "%d %B %Y %H:%M %p GMT",
    ]

    for fmt in formats:
        try:
            date_obj = datetime.strptime(date_text, fmt)
            return date_obj.strftime("%d/%m/%Y")
        except ValueError:
            continue

    print(f"Date format not recognized for: {date_text}")
    return "Date not found"


def extract_date_natural_news(date_text):
    if not date_text:
        return "Date not found"

    date_text = re.split(r"/ By", date_text)[0].strip()

    date_formats = [
        "%B %d, %Y",
        "%m-%d-%Y",
        "%m/%d/%Y",
        "%b %d, %Y",
        "%d %b %Y",
        "%d %B %Y",
    ]
    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(date_text, fmt)
            return date_obj.strftime("%d/%m/%Y")
        except ValueError:
            continue

    print(f"Date format not recognized for: {date_text}")
    return "Date not found"


def convert_date_iso_to_mmddyyyy(date_text):
    try:
        date_obj = datetime.fromisoformat(date_text.split("T")[0])
        return date_obj.strftime("%d/%m/%Y")
    except Exception:
        return "Date not found"


# Helper function to fetch data from Boom Live
def fetch_data_from_boom_live(url, category, max_pages=1):
    headlines, contents, dates, categories = [], [], [], []

    for page_num in range(1, max_pages + 1):
        page_url = f"{url}/page/{page_num}" if page_num > 1 else url

        response = requests.get(page_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")

            # Extract headlines and links
            for item in soup.find_all("h4", class_="font-alt normal"):
                headline = clean_and_preprocess(item.get_text())
                link_tag = item.find("a", class_="heading_link")
                link = link_tag.get("href") if link_tag else None
                link = link if link.startswith("http") else url + link

                if headline and link:
                    article_response = requests.get(link)
                    if article_response.status_code == 200:
                        article_soup = BeautifulSoup(
                            article_response.content, "html.parser"
                        )
                        content = (
                            article_soup.find("p").get_text().strip()
                            if article_soup.find("p")
                            else "Content not found"
                        )
                        date_text = (
                            article_soup.find("span", class_="convert-to-localtime")
                            .get_text()
                            .strip()
                            if article_soup.find("span", class_="convert-to-localtime")
                            else "Date not found"
                        )
                        date = (
                            convert_boom_live_date(date_text)
                            if date_text != "Date not found"
                            else date_text
                        )
                    else:
                        content, date = (
                            "Failed to retrieve article",
                            "Failed to retrieve date",
                        )

                    headlines.append(clean_and_preprocess(headline))
                    contents.append(clean_and_preprocess(content))
                    dates.append(clean_and_preprocess(date))
                    categories.append(category)
        else:
            print(f"Failed to retrieve webpage from {page_url}")
            continue

    return pd.DataFrame(
        {
            "Category": categories,
            "Headline": headlines,
            "Content": contents,
            "Published Date": dates,  # Normalized the column name
            "Label": "0",
        }
    )


# Helper function to fetch data from Natural News
def fetch_data_from_natural_news(url, category, max_pages=1):
    headlines, descriptions, dates, categories = [], [], [], []

    for page_num in range(1, max_pages + 15):
        page_url = url if page_num == 1 else f"{url}page/{page_num}/"

        response = requests.get(page_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")

            articles = soup.find_all("div", class_="Text")
            for article in articles:
                headline = (
                    clean_and_preprocess(article.find("div", class_="Headline").text)
                    if article.find("div", class_="Headline")
                    else "Headline not found"
                )
                description = (
                    clean_and_preprocess(article.find("div", class_="Description").text)
                    if article.find("div", class_="Description")
                    else "Description not found"
                )
                date = (
                    extract_date_natural_news(article.find("div", class_="Date").text)
                    if article.find("div", class_="Date")
                    else "Date not found"
                )

                headlines.append(headline)
                descriptions.append(description)
                dates.append(date)
                categories.append(category)

        else:
            print(f"Failed to retrieve the webpage for {category} on page {page_num}")
            continue

    return pd.DataFrame(
        {
            "Category": categories,
            "Headline": headlines,
            "Content": descriptions,
            "Published Date": dates,
            "Label": "0",
        }
    )


# Helper function to fetch data from Fauxy website
def fetch_data_from_fauxy(url, category, max_pages=1):
    headlines = []
    links = []
    labels = []
    contents = []
    publication_dates = []

    for page_num in range(1, max_pages + 1):
        page_url = f"{url}/page/{page_num}"

        response = requests.get(page_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")

            for item in soup.find_all("h2", class_="entry-title"):
                headline = clean_and_preprocess(item.get_text())
                link_tag = item.find("a")
                link = link_tag.get("href") if link_tag else None

                if headline and link:
                    headlines.append(headline)
                    links.append(link if link.startswith("http") else url + link)
                    labels.append(category)

        else:
            print(f"Failed to retrieve webpage from {page_url}")
            continue

    for link in links:
        article_response = requests.get(link)
        if article_response.status_code == 200:
            article_soup = BeautifulSoup(article_response.content, "html.parser")

            content = (
                article_soup.find("p").get_text().strip()
                if article_soup.find("p")
                else "Content not found"
            )
            publication_date_element = article_soup.find(
                "li", class_="meta-updated-date"
            )
            if publication_date_element:
                time_element = publication_date_element.find("time")
                publication_date = (
                    time_element.get("datetime") if time_element else "Date not found"
                )
                # Convert date to desired format
                publication_date = convert_date_iso_to_mmddyyyy(publication_date)
            else:
                publication_date = "Date not found"

            contents.append(clean_and_preprocess(content))
            publication_dates.append(publication_date)
        else:
            contents.append("Failed to retrieve article")
            publication_dates.append("Failed to retrieve date")

    return pd.DataFrame(
        {
            "Category": labels,
            "Headline": headlines,
            "Content": contents,
            "Published Date": publication_dates,
            "Label": "0",
        }
    )


def remove_float_values(df):
    # Iterate over each column in the DataFrame
    for column in df.columns:
        # Check if the column's dtype is object (for strings and mixed types)
        if df[column].dtype == "object":
            # Use a regex to identify and remove rows where values are float-like
            df = df[~df[column].str.contains(r"^\d+\.\d+$", na=False)]
    return df


# Integrate the function after fetching the combined data
def fetch_combined_data():
    # Boom Live URLs
    boom_live_india = fetch_data_from_boom_live(
        "https://www.boomlive.in/India", "India", max_pages=8
    )

    # Natural News URLs
    science_url = "https://www.naturalnews.com/category/science/"
    technology_url = "https://www.naturalnews.com/category/technology/"
    natural_news_science = fetch_data_from_natural_news(
        science_url, "Science", max_pages=15
    )
    natural_news_technology = fetch_data_from_natural_news(
        technology_url, "Technology", max_pages=15
    )

    # Fauxy URLs
    fauxy_business = fetch_data_from_fauxy(
        "https://thefauxy.com/business", "Business", max_pages=13
    )
    fauxy_sports = fetch_data_from_fauxy(
        "https://thefauxy.com/sports", "Sports", max_pages=12
    )
    fauxy_entertainment = fetch_data_from_fauxy(
        "https://thefauxy.com/entertainment", "Entertainment", max_pages=15
    )
    fauxy_politics = fetch_data_from_fauxy(
        "https://thefauxy.com/politics", "Politics", max_pages=15
    )
    fauxy_world = fetch_data_from_fauxy(
        "https://thefauxy.com/global", "World", max_pages=15
    )

    # Combine all data
    combined_data = pd.concat(
        [
            boom_live_india,
            natural_news_science,
            natural_news_technology,
            fauxy_business,
            fauxy_sports,
            fauxy_entertainment,
            fauxy_politics,
            fauxy_world,
        ]
    )

    # Remove float values from the DataFrame
    combined_data = remove_float_values(combined_data)

    # Arrange in alphabetical order based on 'Category'
    combined_data.sort_values(by="Category", inplace=True)

    # Inspect data before saving
    print("Preview of cleaned data:")
    print(combined_data.head(10))  # Print first 10 rows as a sample

    # Save the combined data to a CSV file with encoding handling
    combined_data.to_csv("news_fake.csv", index=False, encoding="utf-8-sig")


# Run the function
fetch_combined_data()
