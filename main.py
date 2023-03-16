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

    hosts = hosts.drop_duplicates()


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
    calendar = calendar[calendar['available'] != 't']
    calendar = calendar[['listing_id', 'date']]
    return calendar


def clear_hosts(hosts: pd.DataFrame):
    hosts['host_response_rate'] = hosts['host_response_rate'].str.replace("%", "").astype(float)
    hosts['host_acceptance_rate'] = hosts['host_acceptance_rate'].str.replace("%", "").astype(float)

    hosts['host_is_superhost'] = np.where(hosts['host_is_superhost'] == 't', 1, 0)
    hosts['host_has_profile_pic'] = np.where(hosts['host_has_profile_pic'] == 't', 1, 0)
    hosts['host_identity_verified'] = np.where(hosts['host_identity_verified'] == 't', 1, 0)


def transform():
    transform_hosts()
    transform_listings()
    load_fact_table()


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
    load_dim_hosts()
    load_dim_location()



def transform_and_load_dim_apartment():

    drop_constraints()

    prepare_tables_apartment_dim()

    prepare_tables_hosts_dim()

    load_apartment_dim()

    load_listings()




def prepare_tables_apartment_dim():
    try:
        with engine.connect() as conn:

            query = text('''              
                               CREATE TABLE airbnb.DimPropertyType
                               (
                                   id bigint identity(1, 1),
                                   property_type varchar(30),

                                   CONSTRAINT PK_DimPropertyType PRIMARY KEY (id)
                               );''')
            conn.execute(query)

            query = text('''
                CREATE TABLE airbnb.DimRoomType
                (
                    id            bigint identity(1, 1),
                    room_type     varchar(30),

                    CONSTRAINT PK_DimRoomType PRIMARY KEY (id)
                );
           ''')
            conn.execute(query)

            query = text('''              
                    CREATE TABLE airbnb.DimApartments(
                        id bigint identity(1, 1),
                        accommodates float,
                        bathrooms float,
                        bedrooms float,
                        beds float,
                        square_feet float,

                        CONSTRAINT PK_DimApartments PRIMARY KEY (id)
                    );''')
            conn.execute(query)

            conn.commit()
    except Exception as e:
        print(e)


def load_listings():
    try:
        with engine.connect() as conn:

            conn.execute(text('''              
                               CREATE TABLE airbnb.DimListings
                               (
                                   id bigint,
                                   name varchar(100),
                                   summary varchar(1000),
                                   space varchar(1000),
                                   notes varchar(1000),
                                   minimum_nights float,
                                   maximum_nights float,
                                   number_of_reviews float,
                                   review_scores_rating float,
                                   require_guest_profile_picture bit,
                                   
                                   CONSTRAINT PK_DimListings PRIMARY KEY (id)
                               );
            '''))

            conn.execute(text('''
                INSERT INTO airbnb.DimListings 
                (id,name, summary, space, notes, minimum_nights, maximum_nights,
                 number_of_reviews, review_scores_rating, require_guest_profile_picture)
                SELECT id,
                   name,
                   summary,
                   space,
                   notes,
                   minimum_nights,
                   maximum_nights,
                   number_of_reviews,
                   review_scores_rating,
                   CAST(
                           CASE
                               WHEN require_guest_profile_picture = 't'
                                   THEN 1
                               ELSE 0
                               END AS bit
                       ) as require_guest_profile_picture
            
            FROM airbnb_stage.ListingStage
            '''))
            conn.commit()
    except Exception as e:
        print(e)


def load_fact_table():
    try:
        with engine.connect() as conn:

            conn.execute(text('''
                CREATE TABLE airbnb.FactRents
                (
                    id                         bigint identity (1, 1),
                    dim_apartment_id           bigint,
                    dim_host_id                bigint,
                    dim_hosts_neighbourhood_id bigint,
                    dim_response_time_id       bigint,
                    dim_listings_id            bigint,
                    dim_locations_id           bigint,
                    dim_property_type_id       bigint,
                    dim_room_type_id           bigint
                
                    CONSTRAINT PK_FactRents PRIMARY KEY (id),
                    CONSTRAINT FK_FactRents_dim_apartment_id FOREIGN KEY (dim_apartment_id) REFERENCES airbnb.DimApartments (id),
                    CONSTRAINT FK_FactRents_dim_host_id FOREIGN KEY (dim_host_id) REFERENCES airbnb.DimHosts (id),
                    CONSTRAINT FK_FactRents_dim_hosts_neighbourhood_id FOREIGN KEY (dim_hosts_neighbourhood_id) REFERENCES airbnb.DimHostsNeighbourhood (id),
                    CONSTRAINT FK_FactRents_dim_response_time_id FOREIGN KEY (dim_response_time_id) REFERENCES airbnb.DimHostsResponseTime (id),
                    CONSTRAINT FK_FactRents_dim_listings_id FOREIGN KEY (dim_listings_id) REFERENCES airbnb.DimListings (id),
                    CONSTRAINT FK_FactRents_dim_locations_id FOREIGN KEY (dim_locations_id) REFERENCES airbnb.DimLocations(id),
                    CONSTRAINT FK_FactRents_dim_property_type_id FOREIGN KEY (dim_property_type_id) REFERENCES airbnb.DimPropertyType (id),
                    CONSTRAINT FK_FactRents_dim_room_type_id FOREIGN KEY (dim_room_type_id) REFERENCES airbnb.DimRoomType (id)
                );
            '''))

            conn.execute(text('''
                
                INSERT INTO airbnb.FactRents (dim_apartment_id, dim_host_id, dim_hosts_neighbourhood_id, dim_response_time_id,
                                              dim_listings_id, dim_locations_id, dim_property_type_id, dim_room_type_id)
                SELECT DISTINCT
                    DA.id,
                    HS.host_id,
                    DHN.id,
                    DHRT.id,
                    LS.id,
                    DL.id,
                    DPT.id,
                    DROOMT.id
                FROM airbnb_stage.CalendarStage
                         INNER JOIN airbnb_stage.ListingStage AS LS ON LS.id = airbnb_stage.CalendarStage.listing_id
                         LEFT JOIN airbnb_stage.HostsStage AS HS ON HS.host_id = LS.host_id
                         INNER join airbnb.DimApartments AS DA
                                   ON
                                            ( DA.accommodates = LS.accommodates OR (DA.accommodates IS NULL AND LS.accommodates IS NULL)) AND
                                            ( DA.bathrooms = LS.bathrooms OR (DA.bathrooms IS NULL AND LS.bathrooms IS NULL)) AND
                                            ( DA.bedrooms = LS.bedrooms OR (DA.bedrooms IS NULL AND LS.bedrooms IS NULL)) AND
                                            ( DA.beds = LS.beds OR (DA.beds IS NULL AND LS.beds IS NULL)) AND
                                            ( DA.square_feet = LS.square_feet OR (DA.square_feet IS NULL AND LS.square_feet IS NULL))
                         LEFT JOIN airbnb.DimPropertyType AS DPT ON DPT.property_type = LS.property_type
                         LEFT JOIN airbnb.DimHostsNeighbourhood AS DHN ON DHN.host_neighbourhood = HS.host_neighbourhood
                         LEFT JOIN airbnb.DimHostsResponseTime AS DHRT ON DHRT.host_response_time = HS.host_response_time
                         LEFT JOIN airbnb.DimLocations AS DL
                                   ON DL.latitude = LS.latitude AND DL.longitude = LS.longitude
                         LEFT JOIN airbnb.DimRoomType AS DROOMT ON DROOMT.room_type = LS.room_type
            '''))

            conn.commit()
    except Exception as e:
        print(e)


def drop_constraints():
    try:
        with engine.connect() as conn:

            conn.execute(text('''
                            DROP TABLE IF EXISTS airbnb.FactRents;
                            DROP TABLE IF EXISTS airbnb.DimApartments;
                            DROP TABLE IF EXISTS airbnb.DimHosts;
                            DROP TABLE IF EXISTS airbnb.DimHostsNeighbourhood;
                            DROP TABLE IF EXISTS airbnb.DimHostsResponseTime;
                            DROP TABLE IF EXISTS airbnb.DimListings;
                            DROP TABLE IF EXISTS airbnb.DimLocations;
                            DROP TABLE IF EXISTS airbnb.DimPropertyType;
                            DROP TABLE IF EXISTS airbnb.DimRoomType;  
               '''))
            conn.commit()
    except Exception as e:
        print(e)


def load_dim_location():
    try:
        with engine.connect() as conn:
            query = text('''              
                               CREATE TABLE airbnb.DimLocations
                               (
                                   id bigint identity(1,1),
                                   latitude float,
                                   longitude float

                                   CONSTRAINT PK_DimLocations PRIMARY KEY (id)
                               );
            ''')

            conn.execute(query)
            conn.commit()
            conn.execute(text('''
                INSERT INTO airbnb.DimLocations (latitude, longitude) 
                SELECT DISTINCT 
                latitude, longitude 
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
                INSERT INTO airbnb.DimApartments (accommodates, bathrooms, bedrooms, beds, square_feet)
                SELECT DISTINCT
                    accommodates,
                    bathrooms, 
                    bedrooms, 
                    beds, 
                    square_feet
                FROM airbnb_stage.ListingStage
            '''))

            conn.commit()
    except Exception as e:
        print(e)


def prepare_tables_hosts_dim():
    try:
        with engine.connect() as conn:
            conn.commit()

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
                    id bigint,
                    host_name varchar(130),
                    host_about varchar(5000),
                    host_response_rate float,
                    host_acceptance_rate float,
                    host_is_superhost bit,
                    host_has_profile_pic bit,
                    host_identity_verified bit
                    
                    CONSTRAINT PK_DimHosts PRIMARY KEY(id)
                );
            ''')
            conn.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def load_dim_hosts():

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

            conn.execute(text('''
                    INSERT INTO airbnb.DimHosts (id, host_name, host_about, host_response_rate, host_acceptance_rate, host_is_superhost, host_has_profile_pic, host_identity_verified)
                    SELECT DISTINCT 
                        host_id,
                        host_name,
                        host_about,
                        host_response_rate,
                        host_acceptance_rate,
                        host_is_superhost, 
                        host_has_profile_pic, 
                        host_identity_verified
                    FROM airbnb_stage.HostsStage
                    '''))

            conn.commit()

    except Exception as e:
        print(e)


def load():
    pass


def run():
    # load_to_stage()
    transform()
    load()

    engine.dispose()

if __name__ == '__main__':
    run()
