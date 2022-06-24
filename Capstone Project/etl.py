import configparser
import os
import pandas as pd

# hide warnings
import warnings

warnings.simplefilter("ignore")

config = configparser.ConfigParser()
config.read('config/config.cfg')

os.environ['INPUT'] = config['FS']['INPUT']
os.environ['OUTPUT'] = config['FS']['OUTPUT']
os.environ['TEMPERATURE'] = config['DATASET']['TEMPERATURE']
os.environ['POLLUTION'] = config['DATASET']['POLLUTION']
os.environ['GENERATED'] = config['PARQUET']['GENERATED']


def process_pollution_temperature_change(mapping_temperature, mapping_pollution):
    """Process the pollution and temperature for New York City

    Parameters:
    -----------
    mapping_temperature (dataframe): temperature dataframe
    mapping_pollution (dataframe): pollution dataframe
    """

    new_df = mapping_pollution.drop(['Unnamed: 0', 'State Code', 'County Code', 'Site Num', 'Address',
                                     'State', 'County', 'NO2 Units', 'NO2 Mean',
                                     'NO2 1st Max Value', 'NO2 1st Max Hour', 'O3 Units',
                                     'O3 Mean', 'O3 1st Max Value', 'O3 1st Max Hour', 'SO2 Units',
                                     'SO2 Mean', 'SO2 1st Max Value', 'SO2 1st Max Hour', 'CO Units', 'CO Mean',
                                     'CO 1st Max Value',
                                     'CO 1st Max Hour'], axis=1)

    # Data Cleaning Pollution

    # For NO2

    # Get only the NO2 AQI values and Date Local
    no2_df = new_df.drop(['O3 AQI', 'SO2 AQI', 'CO AQI'], 1)

    # Every day has multiple values, so we will take only the maximum values of NO2 AQI every day
    no2_df = no2_df.sort_values('NO2 AQI', ascending=0).drop_duplicates(subset='Date Local', keep='first')

    # Convert Date Local to datatime format
    no2_df['Date Local'] = pd.to_datetime(no2_df['Date Local'])
    no2_df.index = no2_df['Date Local']
    del no2_df['Date Local']

    # Consider only New York City
    no2_df = no2_df[no2_df['City'] == 'New York']

    # Calculate mean N02 AQI per year
    no2_df = no2_df.resample("A").mean()
    no2_df = no2_df.sort_index()
    no2_df = no2_df.dropna()

    # For SO2
    so2_df = new_df.drop(['O3 AQI', 'NO2 AQI', 'CO AQI'], 1)
    so2_df = so2_df.sort_values('SO2 AQI', ascending=0).drop_duplicates(subset='Date Local', keep='first')
    so2_df['Date Local'] = pd.to_datetime(so2_df['Date Local'])
    so2_df.index = so2_df['Date Local']
    del so2_df['Date Local']
    so2_df = so2_df[so2_df['City'] == 'New York']
    so2_df = so2_df.resample("A").mean()
    so2_df = so2_df.sort_index()
    so2_df = so2_df.dropna()
    so2_df.head()

    # For CO
    co_df = new_df.drop(['SO2 AQI', 'NO2 AQI', 'O3 AQI'], 1)
    co_df = co_df.sort_values('CO AQI', ascending=0).drop_duplicates(subset='Date Local', keep='first')
    co_df['Date Local'] = pd.to_datetime(co_df['Date Local'])
    co_df.index = co_df['Date Local']
    del co_df['Date Local']
    co_df = co_df[co_df['City'] == 'New York']
    co_df = co_df.resample("A").mean()
    co_df = co_df.sort_index()
    co_df = co_df.dropna()

    # Data Cleaning for Temperature Data where we are only considering the Temperature for New York City
    mapping_temperature = mapping_temperature[mapping_temperature['Country'] == 'United States']
    mapping_temperature = mapping_temperature[mapping_temperature['City'] == 'New York']
    mapping_temperature = mapping_temperature.drop({"AverageTemperatureUncertainty", "Latitude", "Longitude"}, 1)

    # Convert the Date Local column to date time format
    mapping_temperature['Date Local'] = pd.to_datetime(mapping_temperature['dt'])
    # set first column (dt) as the index column
    mapping_temperature.index = mapping_temperature['Date Local']
    del mapping_temperature['dt'], mapping_temperature['City'], mapping_temperature['Country']
    mapping_temperature.dropna()

    # As we have only one value per day
    mapping_temperature = mapping_temperature.resample("A").mean()

    mapping_temperature = mapping_temperature.dropna()
    mapping_temperature = mapping_temperature.sort_index()

    # Joining different Pollution Data with the Temperature Data

    j1 = pd.merge(mapping_temperature, no2_df, left_index=True, right_index=True, how='inner')
    j2 = pd.merge(so2_df, j1, left_index=True, right_index=True, how='inner')
    j3 = pd.merge(co_df, j2, left_index=True, right_index=True, how='inner')

    # Normalize the data so that all the column data can be compared
    j3_norm = (j3 - j3.mean()) / (j3.max() - j3.min())

    return j3_norm


def process_parquet(df_generated, output_parquet):
    """Generated parquet file

    Parameters:
    -----------
    df_generated (dataframe): dataframe 
    output_data (string): output OS with path and file name
    """

    df_generated.to_parquet(output_parquet)


def main():
    print("-- Start ETL Process")

    # Extract

    input_data = os.environ['INPUT']
    output_data = os.environ['OUTPUT']
    output_parquet = output_data + os.environ['GENERATED']

    temperature_dataset = os.environ['TEMPERATURE']
    pollution_dataset = input_data + os.environ['POLLUTION']

    # Load

    mapping_temperature = pd.read_csv(temperature_dataset)
    mapping_pollution = pd.read_csv(pollution_dataset)

    print("-- Process Clean/Merge/Transform")
    df_generated = process_pollution_temperature_change(mapping_temperature, mapping_pollution)

    # Transform

    process_parquet(df_generated, output_parquet)

    print("-- End ETL Process")


if __name__ == "__main__":
    main()
