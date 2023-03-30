import statistics
import matplotlib.pyplot as plt

import pandas as pd

pd.options.display.width = 0


def read_data():
    df = pd.read_csv('resources/Data2.csv', encoding='windows-1251', sep=';')

    return df


def convert_column_to_float(dataframe, columns):
    for col in columns:
        dataframe[col] = dataframe[col].str.replace(',', '.').astype('float64').abs()


def replace_nan_with_avg(df: pd.DataFrame):
    df.fillna(df.mean(numeric_only=True), inplace=True)


def rename_columns(df, columns):
    df.rename(columns=columns, inplace=True)


def set_columns_type(df, columns, type):
    df[columns] = df[columns].astype(type)


def add_density(dataset):
    dataset['Population Density'] = dataset['Population'] / dataset['Area']


def boxplot(dataset, title, column):
    plt.figure()
    plt.title(title)
    plt.boxplot(dataset[column])


def hist(dataset, xlabel):
    plt.figure()
    plt.title(f'Гістограма {xlabel} від кількості')
    plt.xlabel(xlabel)
    plt.ylabel(f'Кількість {xlabel}')
    plt.hist(dataset[xlabel], bins=10)


def max_gdp_country(df):
    return df.loc[df['GDP per capita'].idxmax()]


def min_area_country(df):
    return df.loc[df['Area'].idxmin()]


def max_mean_area_by_region(df):
    area_by_reg = df.groupby('Region')['Area'].mean()
    return [area_by_reg.idxmax(), area_by_reg.loc[area_by_reg.idxmax()]]


def max_population_density_country(df):
    return df.iloc[df['Population Density'].idxmax()]


def max_population_density_by_region(df):
    pop_density = df[df['Region'] == 'Europe & Central Asia']

    return pop_density[pop_density['Population Density'] == pop_density['Population Density'].max()]


def check_same_mean_median_gdp(df):
    gdp_by_reg = df
    gdp_by_reg['Total GDP'] = gdp_by_reg['GDP per capita'] * gdp_by_reg['Population']

    gdp_by_reg = df.groupby('Region', as_index=False).agg({'Total GDP': ['mean', 'median']})

    return gdp_by_reg[gdp_by_reg[('Total GDP', 'mean')] == gdp_by_reg[('Total GDP', 'median')]]


def countries_sorted_by_gdp(df):
    return df.sort_values(by=['Total GDP'], ascending=[False])


def countries_sorted_by_co2_per_capita(df):
    df['CO2 per capita'] = df['CO2 emission'] / df['Population']
    return df.sort_values(by=['CO2 per capita'], ascending=[False])


def main():
    df = read_data()

    convert_column_to_float(df, ['CO2 emission', 'Area', 'GDP per capita'])

    replace_nan_with_avg(df)

    rename_columns(df, {'Populatiion': 'Population'})
    set_columns_type(df, ['Population'], int)
    add_density(df)

    boxplot(df, 'Діаграма розмаху для \'GDP per capita\'', 'GDP per capita')
    boxplot(df, 'Діаграма розмаху для \'Population\'', 'Population')
    boxplot(df, 'Діаграма розмаху для \'Area\'', 'Area')
    boxplot(df, 'Діаграма розмаху для \'Population Density\'', 'Population Density')

    hist(df, 'GDP per capita')
    hist(df, 'CO2 emission')
    hist(df, 'Area')
    hist(df, 'Population Density')

    max_gdp_row = max_gdp_country(df)
    country_name = max_gdp_row['Country Name']
    max_gpd = max_gdp_row['GDP per capita']
    print(f'\nMax gdp country: {country_name}| max gpt value: {max_gpd}')

    min_area_row = min_area_country(df)
    country_name = min_area_row['Country Name']
    min_area = min_area_row['Area']
    print(f'\nMin area country: {country_name}| min area value: {min_area}')

    # В якому регіоні середня площа країни найбільша?
    max_mean_region_row = max_mean_area_by_region(df)
    reg_name = max_mean_region_row[0]
    max_mean_area = max_mean_region_row[1]
    print(f'\nMax mean area region: {reg_name}| max mean area value: {max_mean_area}')

    # Знайдіть країну з найбільшою щільністю населення у світі
    max_pop_density_row = max_population_density_country(df)
    country_name = max_pop_density_row['Country Name']
    max_pop_density = max_pop_density_row['Population Density']
    print(f'\nMax pop density country: {country_name}| Max pop density value: {max_pop_density}')

    # Знайдіть країну з найбільшою щільністю населення у Європі та центральній Азії
    pop_density_in_europe_central_asia = max_population_density_by_region(df)
    country_name = pop_density_in_europe_central_asia['Country Name'].max()
    max_pop_density = pop_density_in_europe_central_asia['Population Density'].max()
    print(f'\nMax pop density country in Europe and Central Asia: '
          f'{country_name}. Max pop density value: {max_pop_density}')

    res = check_same_mean_median_gdp(df)

    if res.empty:
        print("\nThere is no region where GDP mean and median are the same.")
    else:
        print(res)

    res = countries_sorted_by_gdp(df)
    print('\nTop 5 countries with largest total GDP: ', ', '.join(res.head(5)['Country Name'].values))
    print('Top 5 countries with lowest total GDP: ', ', '.join(res.tail(5)['Country Name'].values))

    res = countries_sorted_by_co2_per_capita(df)
    print('\nTop 5 countries with largest CO2 per capita: ', ', '.join(res.head(5)['Country Name'].values))
    print('Top 5 countries with lowest CO2 per capita: ', ', '.join(res.tail(5)['Country Name'].values))

    plt.show()


if __name__ == '__main__':
    main()
