import pandas as pd
import sqlalchemy
from sqlalchemy import text

pd.options.display.width = 0

source_array = ["data/calendar.csv", "data/listings.csv", "data/reviews.csv"]

SERVER = "localhost:1433"
DATABASE = "Airbnb"
DRIVER = "ODBC Driver 17 for SQL Server"
connection_string = f'mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}'


def extract_data():
    calendar = pd.read_csv(source_array[0], low_memory=False)
    listings = pd.read_csv(source_array[1], low_memory=False)
    reviews = pd.read_csv(source_array[2], low_memory=False)

    return [calendar, reviews, listings]


def load_to_stage():
    dataframes = extract_data()

    calendar = dataframes[0]
    listings = dataframes[1]
    reviews = dataframes[2]

    engine = sqlalchemy.create_engine(connection_string)

    calendar.to_sql(name="CalendarStage", con=engine, if_exists='replace',
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

    engine.dispose()


def transform():
    pass


def load():
    pass


# connection = engine.connect()

# cursor = connection.execute(text("SELECT * FROM "))
#
# for curr in cursor:
#     print(curr)


def run():
    load_to_stage()
    transform()
    load()


if __name__ == '__main__':
    run()
