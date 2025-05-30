import pandas as pd
import random
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from faker import Faker
from datetime import datetime, timedelta, date

fake = Faker()
Faker.seed(42)
np.random.seed(42)

#Handle IDs
def generate_unique_ids(n, digits=9):
    ids = set()
    lower = 10**(digits-1) #Smallest 9 digit num
    upper = 10**digits-1 #Max 9 digit num

    while len(ids) < n:
        ids.add(random.randint(lower, upper))
    return list(ids)

employee_ids = generate_unique_ids(10000)
emp_df = pd.DataFrame({'employeeID': employee_ids})

#Handle country
h1b_weights = {
    "India": 0.298,
    "China": 0.0472,
    "Canada": 0.004,
    "South  Korea": 0.0036,
    "Philippines": 0.0024,
    "Taiwan": 0.0024,
    "Mexico": 0.0024
}
scale = 0.4 / sum(h1b_weights.values())
for i in h1b_weights:
    h1b_weights[i] *= scale

countries = ["USA"] + list(h1b_weights.keys())
weights = [0.6] + [h1b_weights[country] for country in h1b_weights]
country_of_birth = np.random.choice(countries, size=10000, p=weights)
emp_df["CountryOfBirth"] = country_of_birth

#print(emp_df["CountryOfBirth"].value_counts(normalize=True).round(4))

#Handle name
location_map = {
    "USA": "en_US",
    "India": "en_IN",
    "China": "zh_CN",
    "Canada": "es_MX",
    "South  Korea": "en_CA",
    "Philippines": "en_PH",
    "Taiwan": "zh_TW",
    "Mexico": "ko_KR"
}
faker_generators = {} 
for country, location in location_map.items():
    faker_generators[country] = Faker(location)

names = []
for country in emp_df["CountryOfBirth"]:
    faker = faker_generators[country]
    full_name = faker.first_name() + " " + faker.last_name()
    names.append(full_name)

emp_df["name"] = names
#print(emp_df[["CountryOfBirth", "name"]].head(10))

#Handle Phone number
us_faker = Faker("en_US")
phone_numbers = [us_faker.phone_number() for _ in range(10000)]
emp_df["phone"] = phone_numbers
#print(emp_df[["name", "phone"]].head(10))

#Handle Email
emails = [fake.email() for _ in range(10000)]
emp_df["email"] = emails
#print(emp_df[["name", "email"]].head(10))

#Handle gender
gender_weights = {
    "male": 0.49,
    "female": 0.49,
    "nonbinary": 0.02
}

genders = list(gender_weights.keys())
weights = [gender_weights[gender] for gender in genders]
all_genders = np.random.choice(genders, size=10000, p=weights)
emp_df["gender"] = all_genders
#print(emp_df[["name", "gender"]].head(10))

#Handle age
today = datetime.today()

#Date bounds
max_birth = today - timedelta(days=365.25 * 20)
min_birth = today - timedelta(days=365.25 * 65)

def random_birthdata():
    days_range = (max_birth-min_birth).days
    rand_days = random.randint(0, int(days_range))
    return(min_birth + timedelta(days=rand_days)).date()

birthdates = [random_birthdata() for _ in range(len(emp_df))]
emp_df["birthdate"] = birthdates
# print(emp_df[["name", "birthdate"]].head())
# print(emp_df["birthdate"].min(), emp_df["birthdate"].max())

#Handle Hire date
today = date.today()
company_start = date(2010, 1, 1)

def random_hire_date(birthdate):
    age_20_date = birthdate.replace(year=birthdate.year + 20)
    start_date = max(age_20_date, company_start)
    end_date = today

    if start_date >= end_date:
        return end_date #Turned 20 very recently

    delta_days = (end_date - start_date).days #Pick random date
    rand_days = random.randint(0, delta_days)
    return start_date + timedelta(days=rand_days)
hire_dates = [random_hire_date(bd) for bd in emp_df["birthdate"]]
emp_df["hiredate"] = hire_dates
# # No one hired before age 20?
# print((emp_df["hiredate"] < emp_df["birthdate"].apply(lambda d: d.replace(year=d.year + 20))).sum())  # Should be 0
# # No one hired before 2010?
# print((emp_df["hiredate"] < date(2010, 1, 1)).sum())  # Should be 0
# # No one hired in the future?
# print((emp_df["hiredate"] > today).sum())  # Should be 0

#Handle departments
dept_df = pd.read_csv("Departments.csv")
dept_df["Percentage"] = dept_df["Percentage"].str.rstrip('%').astype(float)
dept_probs = (dept_df["Percentage"] / 100).tolist()
dept_names = dept_df["Department"].tolist()

departments = np.random.choice(dept_names, size=len(emp_df), p=dept_probs)
emp_df["department"] = departments
#print(emp_df["department"].value_counts(normalize=True).round(3))

#Handle Roles
role_df = pd.read_csv("Roles_Salaries.csv")
role_df = role_df[["Department", "Role"]]

#Group by department, collect all roles in a list, convert to dict
dept_to_roles = (
    role_df.groupby("Department")["Role"].apply(list).to_dict()
)
#Pick a role for a department
def pick_role(department):
    return random.choice(dept_to_roles.get(department, ["Unknonw"]))
#Apply pick_role to every column
emp_df["role"] = emp_df["department"].apply(pick_role)
#print(emp_df[["name", "department", "role"]].head())

#Handle salary
roles_df = pd.read_csv("Roles_Salaries.csv")
money_cols = ["Lower", "Upper", "Average Salary"]
for col in money_cols:
    roles_df[col] = roles_df[col].replace(r'[\$,]', "", regex=True).astype(float)
roles_df = roles_df[["Department", "Role", "Average Salary", "Lower", "Upper"]]

#Map each entry to a tuple
salary_lookup = {
    (row["Department"], row["Role"]): (row["Lower"], row["Upper"], row["Average Salary"])
    for _, row in roles_df.iterrows()
}

#Given a department and role, generate a salary in range
def generate_salary(dept, role):
    bounds = salary_lookup.get((dept, role))
    if bounds:
        lower, upper, avg = map(int, bounds)
        return int(random.triangular(lower, upper, avg))
    else:
        raise ValueError

#Apply  
emp_df["salary"] = emp_df.apply(
    lambda row: generate_salary(row["department"], row["role"]), axis=1
)
#print(emp_df[["name", "department", "role", "salary"]].sample(10))


#Handle SSN
ssn_set = set()
ssn_list = []
while len(ssn_list) < len(emp_df):
    ssn = us_faker.ssn()
    if ssn not in ssn_set:
        ssn_set.add(ssn)
        ssn_list.append(ssn)

emp_df["SSID"] = ssn_list
# print(emp_df[["name", "SSID"]].head())
# print(emp_df["SSID"].nunique())  # Should be 10000

#print(emp_df.describe(include='all'))
#print(emp_df.head(10))
total_payroll = emp_df["salary"].sum()
#print(f"Total yearly payroll: ${total_payroll:,.2f}")

#Country plot
# country_counts = emp_df["CountryOfBirth"].value_counts()
# plt.figure(figsize=(10, 6))
# country_counts.plot(kind="bar")
# plt.title("Employee Count by Country of Birth")
# plt.xlabel("Country")
# plt.ylabel("Number of Employees")
# plt.xticks(rotation=45)
# plt.tight_layout()
# # plt.show()

# #Department plot

# dept_counts = emp_df["department"].value_counts()
# plt.figure(figsize=(12, 6))
# dept_counts.plot(kind="bar")
# plt.title("Employee Count by Department")
# plt.xlabel("Department")
# plt.ylabel("Number of Employees")
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.show()

# #Hired employee days
# #Add a column for day of the week
# emp_df["hire_day"] = emp_df["hiredate"].apply(lambda d: d.strftime("%A"))

# days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
# hire_day_counts = emp_df["hire_day"].value_counts().reindex(days_order, fill_value=0)

# plt.figure(figsize=(10, 6))
# hire_day_counts.plot(kind="bar")

# plt.title("Employees Hired by Day of the Week")
# plt.xlabel("Day of the Week")
# plt.ylabel("Number of Employees Hired")
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.show()


# #KDE of salaries
# plt.figure(figsize=(10, 6))
# sns.kdeplot(emp_df["salary"],  fill=True)
# plt.title("Salary Distribution (KDE)")
# plt.xlabel("Salary (USD)")
# plt.ylabel("Density")
# plt.tight_layout()
# plt.show()

# #Born each year
# emp_df["birth_year"] = emp_df["birthdate"].apply(lambda d: d.year)
# birth_year_counts = emp_df["birth_year"].value_counts().sort_index()
# import matplotlib.pyplot as plt

# plt.figure(figsize=(12, 6))
# plt.plot(birth_year_counts.index, birth_year_counts.values, marker='o')

# plt.title("Number of Employees Born Each Year")
# plt.xlabel("Year of Birth")
# plt.ylabel("Number of Employees")
# plt.grid(True)
# plt.tight_layout()
# plt.show()

# #KDE grouped by appartment
# plt.figure(figsize=(12, 6))

# palette = sns.color_palette("tab10", n_colors=emp_df["department"].nunique())
# # Plot KDEs grouped by department
# for i, (dept, group) in enumerate(emp_df.groupby("department")):
#     sns.kdeplot(
#         data=group,
#         x="salary",
#         fill=True,          # Fill under each curve
#         common_norm=False,  # Each KDE is normalized independently
#         alpha=0.5,          # Transparency for overlap
#         linewidth=1.5,
#         label=dept,
#         color=palette[i % len(palette)]
#     )

# plt.title("Salary Distribution by Department (KDE)")
# plt.xlabel("Salary (USD)")
# plt.ylabel("Density")
# plt.legend(title="Department", bbox_to_anchor=(1.05, 1), loc='upper left')
# plt.tight_layout()
# plt.show()

#Sample data
emp_df["age"] = emp_df["birthdate"].apply(lambda d: today.year - d.year - ((today.month, today.day) < (d.month, d.day)))
emp_df["sample_weight"] = emp_df["age"].apply(lambda age: 3 if 40 <= age <= 49 else 1)
smpl_df = emp_df.sample(n=500, weights="sample_weight", random_state=42)
#print(smpl_df["age"].value_counts(bins=[0, 29, 39, 49, 59, 69], sort=False))
#print(smpl_df.describe(include='all'))
#print(smpl_df.head(10))


#Perturbing
prtrb_df = emp_df.copy()

#Find std deviation of existing salaries
salary_std = emp_df["salary"].std()
#Choose noise scale, 10% of std dev
noise_std = 0.10 * salary_std

np.random.seed(42)
noise = np.random.normal(loc=0, scale=noise_std, size=len(emp_df))
prtrb_df["salary"] = (emp_df["salary"] + noise).round().astype(int)
#print(prtrb_df.describe(include='all'))
print(prtrb_df.head(10))