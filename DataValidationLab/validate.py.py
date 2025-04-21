import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("employees.csv")

def non_null_firstname():
    result = []

    for index, row in df.iterrows():
        if pd.isnull(row["name"]):
            result.append(row)
    return result
    #return df[df["name"].isnull()] # Return all rows from df where firstname is null

def hired_pre_2015():
    result = []
    bad_dates = ["2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"]
    for index, row in df.iterrows():
        year = row["hire_date"][:4]
        if year in bad_dates:
            result.append(row)
    return result

def born_before_hired():
    result = []
    for index, row in df.iterrows():
        birth = row["birth_date"][:4]
        hire = row["hire_date"][:4]
        if int(birth) > int(hire):
            result.append(row)
        elif int(birth) == int(hire):
            birth = row["birth_date"][6:7]
            hire = row["hire_date"][6:7]

            if int(birth) > int(hire):
                result.append(row)
            elif int(birth) == int(hire):
                birth = row["birth_date"][9:10]
                hire = row["hire_date"][9:10]

                if int(birth) > int(hire):
                    result.append(row)
    return result

def known_manager():
    result = []
    valid_ids = []
    for index, row in df.iterrows():
        valid_ids.append(row["eid"])

    valid_ids.sort()

    for index, row in df.iterrows():
        did_find = search_id(valid_ids, row["reports_to"])
        if did_find == False:
            result.append(row)
    return result

def search_id(valid_ids, search):
    if len(valid_ids) == 0 or (len(valid_ids) == 1 and valid_ids[0] != search):
        return False

    if len(valid_ids) == 1 and valid_ids[0] == search:
        return True

    mid = len(valid_ids) // 2
    if valid_ids[mid] >= search:
        return search_id(valid_ids[0:mid], search) 
    else:
        return search_id(valid_ids[mid:], search)
    
def city_more_then_one_employee():
    result = []
    cities = {}

    for index, row in df.iterrows():
        if row["city"] not in cities:
            cities[row["city"]] = 1
        else:
            cities[row["city"]] += 1
    
    for entry in cities:
        if cities[entry] == 1:
            result.append(entry)
    return result

def is_salary():
    salaries = df["salary"]

    plt.figure(figsize=(8,5))
    plt.hist(salaries, bins=20, edgecolor="black", alpha=0.7, log=True)
    plt.title("Histogram of Salaries")
    plt.xlabel("Salary")
    plt.ylabel("Frequency")
    plt.grid(True)

    plt.show()


def main():
    # print(df.head())
    # print(df.columns)
    print(df.dtypes)

    # null_firstname = non_null_firstname()
    # print("Null Firstname Violations:", len(null_firstname))

    # pre_2015 = hired_pre_2015()
    # print("Hired earlier then 2015:", len(pre_2015))

    # born_after = born_before_hired()
    # print("People born after being hired:", len(born_after))

    # missing_managers = known_manager()
    # print("People with unknown managers:", len(missing_managers))

    # one_employee_cities = city_more_then_one_employee()
    # print("Cities with only one person:", len(one_employee_cities))

    print("Plotting salaries now")
    is_salary()




if __name__ == "__main__":
    main()

