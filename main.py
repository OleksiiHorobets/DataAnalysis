import numpy as np
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
    hosts_raw = transform_hosts()
    listings_raw = transform_listings()

    # print(listings_raw)


def transform_hosts():
    hosts_query = text("""
                SELECT listing_id, host_id, host_name,host_since, host_location,host_about,host_response_time,
                    host_response_rate, host_acceptance_rate, host_is_superhost, host_neighbourhood, host_listings_count,
                    host_total_listings_count, host_has_profile_pic, host_identity_verified
                FROM airbnb_stage.HostsStage
        """)

    hosts = pd.read_sql(hosts_query, engine.connect())
    return hosts


def transform_listings():
    listings_query = text("""
                SELECT id, listing_url, scrape_id, last_scraped, name, summary,
                space, description, experiences_offered, neighborhood_overview,
                notes, transit, thumbnail_url, medium_url, picture_url,
                xl_picture_url, street, neighbourhood, neighbourhood_cleansed,
                neighbourhood_group_cleansed, city, state, zipcode, market,
                smart_location, country_code, country, latitude, longitude,
                is_location_exact, property_type, room_type, accommodates,
                bathrooms, bedrooms, beds, bed_type, amenities, square_feet,
                price, weekly_price, monthly_price, security_deposit,
                cleaning_fee, guests_included, extra_people, minimum_nights,
                maximum_nights, calendar_updated, has_availability,
                availability_30, availability_60, availability_90,
                availability_365, calendar_last_scraped, number_of_reviews,
                first_review, last_review, review_scores_rating,
                review_scores_accuracy, review_scores_cleanliness,
                review_scores_checkin, review_scores_communication,
                review_scores_location, review_scores_value, requires_license,
                license, jurisdiction_names, instant_bookable,
                cancellation_policy, require_guest_profile_picture,
                require_guest_phone_verification, calculated_host_listings_count,
                reviews_per_month
                FROM airbnb_stage.ListingStage
        """)

    listings = pd.read_sql(listings_query, engine.connect())

    dim_apartment = transform_dim_apartment(listings)

    # dim_price = hosts.loc[:, '']

    return dim_apartment


def transform_dim_apartment(listings: pd.DataFrame):
    dim_apartment_raw = listings.loc[:, ['property_type', 'room_type', 'accommodates',
                                         'bathrooms', 'bedrooms', 'beds',
                                         'bed_type', 'square_feet']]

    dim_apartment_raw['id'] = np.arange(len(dim_apartment_raw)) + 1
    dim_apartment_raw.set_index('id', inplace=True)

    # print(dim_apartment_raw)
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS airbnb.DimApartment;"))
            query = text('''              
                    CREATE TABLE airbnb.DimApartment
                    (
                        id bigint,
                        property_type varchar(30),
                        room_type varchar(30),
                        accommodates int,
                        bathrooms int,
                        bedrooms int,
                        beds int,
                        bed_type varchar(30),
                        square_feet float
                    
                        CONSTRAINT PK_DimApartment PRIMARY KEY (id)
                    );''')
            conn.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


    # query = "INSERT INTO airbnb.DimApartment (id, property_type ,room_type, accommodates, bathrooms, bedrooms, beds, bed_type, square_feet)  VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    dim_apartment_raw.to_sql(name="DimApartment", con=engine, if_exists='append',
                             schema='airbnb',
                             )

    return dim_apartment_raw


def load():
    pass


def run():
    # load_to_stage()
    transform()
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
        'xl_picture_url', 'street', 'neighbourhood', 'neighbourhood_cleansed',
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
