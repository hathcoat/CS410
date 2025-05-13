import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

us_state_to_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "Virgin Islands, U.S.": "VI",
}
# invert the dictionary
abbrev_to_us_state = dict(map(reversed, us_state_to_abbrev.items()))

cases_df = pd.read_csv("covid_confirmed_usafacts.csv")
deaths_df = pd.read_csv("covid_deaths_usafacts.csv")
census_df = pd.read_csv("acs2017_county_data.csv")

cases_df = cases_df[["County Name", "State", "2023-07-23"]]
deaths_df = deaths_df[["County Name", "State", "2023-07-23"]]
census_df = census_df[["County", "State", "TotalPop", "IncomePerCap", "Poverty", "Unemployment"]]

# print("\ncases_df information:")
# cases_df.info()
# print("\ndeaths_df information:")
# deaths_df.info()
# print("\ncensus_df information:")
# census_df.info()

#Fix county's and count Washington County
count_cases = 0
count_deaths = 0
for index, record in cases_df.iterrows():
    update_record = record["County Name"][:-1]
    cases_df.loc[index, "County Name"] = update_record
    if update_record == "Washington County":
        count_cases += 1

for index, record in deaths_df.iterrows():
    update_record = record["County Name"][:-1]
    deaths_df.loc[index, "County Name"] = update_record
    if update_record == "Washington County":
        count_deaths += 1

print(f"count_cases: {count_cases}")
print(f"count_deaths {count_deaths}")


#Drop bad counties
cases_df = cases_df.drop(cases_df[cases_df["County Name"] == "Statewide Unallocate"].index)
deaths_df = deaths_df.drop(deaths_df[deaths_df["County Name"] == "Statewide Unallocate"].index)
print(f"Cases Left: {cases_df.count()}")
print(f"Deaths Left: {deaths_df.count()}")


for index, row in cases_df.iterrows():
    state = abbrev_to_us_state[row["State"]]
    cases_df.loc[index, "State"] = state

for index, row in deaths_df.iterrows():
    state = abbrev_to_us_state[row["State"]]
    deaths_df.loc[index, "State"] = state

# print("\ncases_df head: ")
# print(cases_df.head())
# print("\ndeaths_df head: ")
# print(deaths_df.head())

cases_df["key"] = cases_df.apply(
    lambda row: row["County Name"] + row["State"],
    axis=1
)

deaths_df["key"] = deaths_df.apply(
    lambda row: row["County Name"] + row["State"],
    axis=1
)

census_df["key"] = census_df.apply(
    lambda row: row["County"] + row["State"],
    axis = 1
)

cases_df.set_index("key", inplace=True)
deaths_df.set_index("key", inplace=True)
census_df.set_index("key", inplace=True)

print("\n\n", cases_df.columns)
print("\n\n", deaths_df.columns)
print("\n\n", census_df.columns)

cases_df.drop(['County Name', 'State'], axis=1, inplace=True)
deaths_df.drop(['County Name', 'State'], axis=1, inplace=True)

# print(f"\ncases_df:")
# print(cases_df.head())
# print(f"\ndeaths_df:")
# print(deaths_df.head())
# print(f"\ncensus_df:")
# print(census_df.head())

cases_df.rename(columns={"2023-07-23" : "Cases"}, inplace=True)
deaths_df.rename(columns={"2023-07-23" : "Deaths"}, inplace=True)

print(f"cases_df: {cases_df.columns.values.tolist()}")
print(f"\ndeaths_df: {deaths_df.columns.values.tolist()}")

join_df = cases_df.join(deaths_df, lsuffix='_cases', rsuffix='_deaths').join(census_df, rsuffix='_census')
#print(join_df)
join_df['CasesPerCap'] = join_df['Cases'] / join_df['TotalPop']
join_df['DeathsPerCap'] = join_df['Deaths'] / join_df['TotalPop']

#print(join_df.columns)
numeric_df = join_df[['Cases', 'Deaths', 'TotalPop', 'IncomePerCap', 'Poverty', 'Unemployment', 'CasesPerCap', 'DeathsPerCap']]
correlation_matrix = numeric_df = numeric_df.corr()
print("\n\nCorrelation Matrix:\n", correlation_matrix)

sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)

plt.title('Correlation Matrix Heatmap')
#plt.show()
print(join_df)