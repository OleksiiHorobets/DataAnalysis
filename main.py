import pandas as pd
import sqlalchemy
from sqlalchemy import text
import time
import concurrent.futures

pd.options.display.width = 0

source_array = ["data/hosts.csv", "data/listings.csv", "data/reviews.csv"]

SERVER = "localhost:1433"
DATABASE = "Airbnb"
DRIVER = "ODBC Driver 17 for SQL Server"
connection_string = f'mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}'

engine = sqlalchemy.create_engine(connection_string)


def extract_data():
    hosts = pd.read_csv(source_array[0], low_memory=False)
    listings = pd.read_csv(source_array[1], low_memory=False)
    reviews = pd.read_csv(source_array[2], low_memory=False)

    return {"hosts": hosts, "listings": listings, "reviews": reviews}


def load_to_stage():
    dataframes = extract_data()

    hosts = dataframes.get("hosts")
    listings = dataframes.get("listings")
    reviews = dataframes.get("reviews")

    start = time.time()

    hosts.to_sql(name="HostsStage", con=engine, if_exists='replace',
                 index=False,
                 schema='airbnb_stage'
                 )

    listings.to_sql(name="ListingStage", con=engine, if_exists='replace',
                    index=False,
                    schema='airbnb_stage',
                    )
    reviews.to_sql(name="ReviewsStage", con=engine, if_exists='replace',
                   index=False,
                   schema='airbnb_stage'
                   )

    print(f'Loading to stage area: {time.time() - start} sec')


def transform():
    hosts_query = "SELECT * FROM "

    hosts = pd.read_sql()
    pass


def load():
    pass


def run():
    load_to_stage()
    # transform()
    load()

    engine.dispose()


def split():
    hosts = pd.read_csv(source_array[1], usecols=[
        'id',
        'host_id', 'host_url', 'host_name', 'host_since',
        'host_location', 'host_about', 'host_response_time',
        'host_response_rate', 'host_acceptance_rate', 'host_is_superhost',
        'host_thumbnail_url', 'host_picture_url', 'host_neighbourhood',
        'host_listings_count', 'host_total_listings_count',
        'host_verifications', 'host_has_profile_pic', 'host_identity_verified'])

    hosts.rename(columns={'id': 'listing_id'}, inplace=True)

    new_listings = pd.read_csv(source_array[1], usecols=[
        'id', 'listing_url', 'scrape_id', 'last_scraped', 'name', 'summary',
        'space', 'description', 'experiences_offered', 'neighborhood_overview',
        'notes', 'transit', 'thumbnail_url', 'medium_url', 'picture_url',
        'xl_picture_url',
        'street', 'neighbourhood', 'neighbourhood_cleansed',
        'neighbourhood_group_cleansed', 'city', 'state', 'zipcode', 'market',
        'smart_location', 'country_code', 'country', 'latitude', 'longitude',
        'is_location_exact', 'property_type', 'room_type', 'accommodates',
        'bathrooms', 'bedrooms', 'beds', 'bed_type', 'amenities', 'square_feet',
        'price', 'weekly_price', 'monthly_price', 'security_deposit',
        'cleaning_fee', 'guests_included', 'extra_people', 'minimum_nights',
        'maximum_nights', 'calendar_updated', 'has_availability',
        'availability_30', 'availability_60', 'availability_90',
        'availability_365', 'calendar_last_scraped', 'number_of_reviews',
        'first_review', 'last_review', 'review_scores_rating',
        'review_scores_accuracy', 'review_scores_cleanliness',
        'review_scores_checkin', 'review_scores_communication',
        'review_scores_location', 'review_scores_value', 'requires_license',
        'license', 'jurisdiction_names', 'instant_bookable',
        'cancellation_policy', 'require_guest_profile_picture',
        'require_guest_phone_verification', 'calculated_host_listings_count',
        'reviews_per_month'
    ])

    hosts.to_csv("data/hosts.csv", index=False)
    new_listings.to_csv("data/listings.csv", index=False)

    # print(hosts)


if __name__ == '__main__':
    # split()
    run()
