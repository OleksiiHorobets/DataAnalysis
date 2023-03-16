import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import text
import time

pd.options.display.width = 0

source_array = ["data/hosts.csv", "data/listings.csv", "data/calendar.csv"]

SERVER = "localhost:1433"
DATABASE = "Airbnb"
DRIVER = "ODBC Driver 17 for SQL Server"
connection_string = f'mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}'

engine = sqlalchemy.create_engine(connection_string)


def extract_data():
    hosts = pd.read_csv(source_array[0], low_memory=False)
    listings = pd.read_csv(source_array[1], low_memory=False)
    calendar = pd.read_csv(source_array[2], low_memory=False)

    return {"hosts": hosts, "listings": listings, "calendar": calendar}


def clear_listings(listings: pd.DataFrame):
    listings['price'] = listings['price'].str.replace('[\$,]', '', regex=True).astype(float)
    listings['weekly_price'] = listings['weekly_price'].replace('[\$,]', '', regex=True).astype(float)
    listings['monthly_price'] = listings['monthly_price'].replace('[\$,]', '', regex=True).astype(float)
    listings['security_deposit'] = listings['security_deposit'].replace('[\$,]', '', regex=True).astype(float)
    listings['cleaning_fee'] = listings['cleaning_fee'].replace('[\$,]', '', regex=True).astype(float)
    listings['extra_people'] = listings['extra_people'].replace('[\$,]', '', regex=True).astype(float)


def load_to_stage():
    dataframes = extract_data()

    hosts = dataframes.get("hosts")
    listings = dataframes.get("listings")
    calendar = dataframes.get("calendar")

    clear_hosts(hosts)
    clear_listings(listings)
    calendar = clear_calendar(calendar)

    start = time.time()

    hosts.to_sql(name="HostsStage", con=engine, if_exists='replace',
                 index=False,
                 schema='airbnb_stage'
                 )

    listings.to_sql(name="ListingStage", con=engine, if_exists='replace',
                    index=False,
                    schema='airbnb_stage'
                    )
    calendar.to_sql(name="CalendarStage", con=engine, if_exists='replace',
                    index=False,
                    schema='airbnb_stage'
                    )

    print(f'Loading to stage area: {time.time() - start} sec')


def clear_calendar(calendar: pd.DataFrame):
    calendar =  calendar[calendar['available'] != 't']
    calendar = calendar[['listing_id', 'date']]
    return calendar


def clear_hosts(hosts: pd.DataFrame):
    hosts['host_response_rate'] = hosts['host_response_rate'].str.replace("%", "").astype(float)
    hosts['host_acceptance_rate'] = hosts['host_acceptance_rate'].str.replace("%", "").astype(float)

    hosts['host_is_superhost'] = np.where(hosts['host_is_superhost'] == 't', 1, 0)
    hosts['host_has_profile_pic'] = np.where(hosts['host_has_profile_pic'] == 't', 1, 0)
    hosts['host_identity_verified'] = np.where(hosts['host_identity_verified'] == 't', 1, 0)


def transform():
    dim_hosts = transform_hosts()
    listings_raw = transform_listings()

    # print(listings_raw)


def transform_hosts():
    hosts_query = text("""
                SELECT host_id, host_name, host_since,
                host_location, host_about, host_response_time,
                host_response_rate, host_is_superhost, host_neighbourhood,
                host_listings_count, host_total_listings_count,
                host_verifications, host_has_profile_pic, host_identity_verified
                FROM airbnb_stage.HostsStage
        """)

    dim_hosts = pd.read_sql(hosts_query, engine.connect())

    return dim_hosts


def transform_listings():
    transform_and_load_dim_apartment()

    load_dim_prices()
    load_dim_hosts()
    load_dim_prices()
    load_dim_location()



def transform_and_load_dim_apartment():
    # dim_apartment_raw['id'] = np.arange(len(dim_apartment_raw)) + 1
    # dim_apartment_raw.set_index('id', inplace=True)

    prepare_tables_apartment_dim()

    prepare_tables_hosts_dim()

    load_apartment_dim()

    load_listings()


def prepare_tables_apartment_dim():
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS airbnb.DimApartment;"))
            conn.execute(text("DROP TABLE IF EXISTS airbnb.DimPropertyType;"))
            conn.execute(text("DROP TABLE IF EXISTS airbnb.DimRoomType;"))

            query = text('''              
                               CREATE TABLE airbnb.DimPropertyType
                               (
                                   id bigint identity(1,1),
                                   property_type varchar(30),

                                   CONSTRAINT PK_DimPropertyType PRIMARY KEY (id)
                               );''')
            conn.execute(query)

            query = text('''
                CREATE TABLE airbnb.DimRoomType
                (
                    id            bigint identity(1,1),
                    room_type     varchar(30),

                    CONSTRAINT PK_DimRoomType PRIMARY KEY (id)
                );
           ''')
            conn.execute(query)

            query = text('''              
                    CREATE TABLE airbnb.DimApartment(
                        id bigint identity(1, 1),
                        property_type_id bigint,
                        room_type_id bigint,
                        accommodates int,
                        bathrooms int,
                        bedrooms int,
                        beds int,
                        square_feet float,

                        CONSTRAINT PK_DimApartment PRIMARY KEY (id),
                        CONSTRAINT FK_DimApartment_property_type_id FOREIGN KEY (property_type_id) REFERENCES airbnb.DimPropertyType(id),
                        CONSTRAINT FK_DimApartment_room_type_id FOREIGN KEY (room_type_id) REFERENCES airbnb.DimRoomType(id)
                    );''')
            conn.execute(query)

            conn.commit()
    except Exception as e:
        print(e)


def load_listings():
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS airbnb.DimListings;"))

            query = text('''              
                               CREATE TABLE airbnb.DimListings
                               (
                                   id bigint identity(1,1),
                                   latitude float,
                                   longitude float,

                                   CONSTRAINT PK_DimListings PRIMARY KEY (id)
                               );
            ''')
            conn.execute(query)

            conn.execute(text('''
                INSERT INTO airbnb.DimLocation (latitude, longitude) 
                SELECT latitude, longitude 
                FROM airbnb_stage.ListingStage
            '''))
            conn.commit()
    except Exception as e:
        print(e)


def load_dim_location():
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS airbnb.airbnb.DimLocation;"))

            query = text('''              
                               CREATE TABLE airbnb.DimLocation
                               (
                                   id bigint identity(1,1),
                                   latitude float,
                                   longitude float,

                                   CONSTRAINT PK_DimLocation PRIMARY KEY (id)
                               );
            ''')
            conn.execute(query)

            conn.execute(text('''
                INSERT INTO airbnb.DimLocation (latitude, longitude) 
                SELECT latitude, longitude 
                FROM airbnb_stage.ListingStage
            '''))
            conn.commit()
    except Exception as e:
        print(e)


def load_apartment_dim():
    try:
        with engine.connect() as conn:
            conn.execute(text('''
                        INSERT INTO airbnb.DimPropertyType (property_type)
                        SELECT DISTINCT property_type 
                        FROM  airbnb_stage.ListingStage
                        WHERE property_type is not NULL
                                    '''))

            conn.execute(text('''
                        INSERT INTO airbnb.DimRoomType (room_type)
                        SELECT DISTINCT room_type 
                        FROM  airbnb_stage.ListingStage
                         WHERE room_type is not NULL
                        '''))

            conn.execute(text('''
                INSERT INTO airbnb.DimApartment (property_type_id, room_type_id, accommodates, bathrooms, bedrooms, beds, square_feet)
                SELECT 
                    PT.id,
                    RT.id,
                    ListingStage.accommodates,
                    ListingStage.bathrooms, 
                    ListingStage.bedrooms, 
                    ListingStage.beds, 
                    ListingStage.square_feet
                FROM airbnb_stage.ListingStage
                INNER JOIN 
                airbnb.DimRoomType AS RT ON RT.room_type = airbnb_stage.ListingStage.room_type
                INNER JOIN
                airbnb.DimPropertyType AS PT ON PT.property_type = airbnb_stage.ListingStage.property_type
            '''))

            conn.commit()
    except Exception as e:
        print(e)


def prepare_tables_hosts_dim():
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS airbnb.DimHosts;"))
            conn.execute(text("DROP TABLE IF EXISTS airbnb.DimHostsSince;"))
            conn.execute(text("DROP TABLE IF EXISTS airbnb.DimHostsNeighbourhood;"))
            conn.execute(text("DROP TABLE IF EXISTS airbnb.DimHostsResponseTime;"))

            conn.commit()
            query = text('''              
                               CREATE TABLE airbnb.DimHostsSince
                               (
                                   id bigint identity(1,1),
                                   day int NOT NULL,
                                   month int NOT NULL,
                                   year int NOT NULL,

                                   CONSTRAINT PK_DimHostsSince PRIMARY KEY (id)
                               );
                        ''')
            conn.execute(query)

            query = text('''
                    CREATE TABLE airbnb.DimHostsNeighbourhood
                    (
                        id bigint identity(1, 1),
                        host_neighbourhood varchar(100),
                        
                        CONSTRAINT PK_DimHostsNeighbourhood PRIMARY KEY (id)
                    );
            ''')
            conn.execute(query)

            query = text('''
                                CREATE TABLE airbnb.DimHostsResponseTime
                                (
                                    id bigint identity(1, 1),
                                    host_response_time varchar(100),

                                    CONSTRAINT PK_DimHostsResponseTime PRIMARY KEY (id)
                                );
                        ''')
            conn.execute(query)

            query = text('''
            CREATE TABLE airbnb.DimHosts
            (
                id bigint identity(1, 1),
                host_name varchar(130),
                host_since_id bigint,
                host_response_time_id bigint,
                host_neighbourhood_id bigint,
                host_about varchar(5000),
                host_response_rate float,
                host_acceptance_rate float,
                host_is_superhost bit,
                host_has_profile_pic bit,
                host_identity_verified bit, 
                
                CONSTRAINT PK_DimHosts PRIMARY KEY(id),
                CONSTRAINT FK_DimHosts_host_since_id FOREIGN KEY (host_since_id) REFERENCES airbnb.DimHostsSince(id),
                CONSTRAINT FK_DimHosts_host_neighbourhood_id FOREIGN KEY (host_neighbourhood_id) REFERENCES airbnb.DimHostsNeighbourhood(id),
                CONSTRAINT FK_DimHosts_host_response_time_id FOREIGN KEY (host_response_time_id) REFERENCES airbnb.DimHostsResponseTime(id)
            );
            ''')
            conn.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def load_dim_hosts():
    # Load DimHostSince
    dates = pd.Series(pd.date_range('2008-01-01', '2016-01-01', freq='D'))
    dates_df = pd.DataFrame({'day': dates.dt.day, 'month': dates.dt.month, 'year': dates.dt.year})

    dates_df.to_sql(name="DimHostsSince", con=engine, if_exists='append', schema='airbnb', index=False)

    try:
        with engine.connect() as conn:
            conn.execute(text('''
                           INSERT INTO airbnb.DimHostsNeighbourhood (host_neighbourhood)
                           SELECT DISTINCT host_neighbourhood 
                           FROM airbnb_stage.HostsStage
                           WHERE host_neighbourhood IS NOT NULL
                       '''))

            conn.execute(text('''
                            INSERT INTO airbnb.DimHostsResponseTime (host_response_time)
                            SELECT DISTINCT host_response_time 
                            FROM airbnb_stage.HostsStage
                            WHERE host_response_time IS NOT NULL
                        '''))

            conn.commit()
            conn.execute(text('''
                INSERT INTO airbnb.DimHosts (host_name, host_since_id, host_response_time_id, host_neighbourhood_id, host_about, host_response_rate, host_acceptance_rate, host_is_superhost, host_has_profile_pic, host_identity_verified)
                SELECT 
                    host_name,
                    DHS.id as host_since_id,
                    DHRT.id as host_response_time_id,
                    DHN.id as host_neighbourhood_id,
                    host_about,
                    host_response_rate,
                    host_acceptance_rate,
                    host_is_superhost, 
                    host_has_profile_pic, 
                    host_identity_verified
                FROM airbnb_stage.HostsStage
                INNER JOIN airbnb.DimHostsResponseTime AS DHRT ON DHRT.host_response_time = airbnb_stage.HostsStage.host_response_time
                INNER JOIN airbnb.DimHostsNeighbourhood AS DHN ON DHN.host_neighbourhood = airbnb_stage.HostsStage.host_neighbourhood
                INNER JOIN airbnb.DimHostsSince AS DHS ON CONCAT(DHS.year, '-', DHS.month, '-', DHS.day) = airbnb_stage.HostsStage.host_since
                '''))
            conn.commit()

    except Exception as e:
        print(e)


def load_dim_prices():
    try:
        with engine.connect() as conn:
            prices_df = pd.read_sql(text("""SELECT
                       price, weekly_price, monthly_price, security_deposit, cleaning_fee, extra_people
                   FROM airbnb_stage.ListingStage
                   """), conn)

            conn.execute(text("""
                DROP TABLE IF EXISTS airbnb.DimListingPrice;
                CREATE TABLE airbnb.DimListingPrice
                (
                    id bigint identity (1, 1),
                    price float,
                    weekly_price float,
                    monthly_price float,
                    security_deposit float,
                    cleaning_fee float,
                    extra_people float
                           
                    CONSTRAINT PK_DimListingPrice PRIMARY KEY(id)
                )
            """))
            conn.commit()

            prices_df.to_sql(name="DimListingPrice", con=engine, if_exists='append', schema='airbnb', index=False)


    except Exception as e:
        print(e)


def load():
    pass


def run():
    # load_to_stage()
    transform()
    load()

    engine.dispose()


def split():
    hosts = pd.read_csv(source_array[1], usecols=[
        'host_id', 'host_url', 'host_name', 'host_since',
        'host_location', 'host_about', 'host_response_time',
        'host_response_rate', 'host_acceptance_rate', 'host_is_superhost',
        'host_thumbnail_url', 'host_picture_url', 'host_neighbourhood',
        'host_listings_count', 'host_total_listings_count',
        'host_verifications', 'host_has_profile_pic', 'host_identity_verified'])

    # hosts.rename(columns={'id': 'listing_id'}, inplace=True)

    new_listings = pd.read_csv(source_array[1], usecols=[
        'id', 'host_id', 'listing_url', 'scrape_id', 'last_scraped', 'name', 'summary',
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
