import csv
import random
import datetime
import pandas as pd
from faker import Faker
from datetime import datetime
from itertools import islice


## Start claim_id ,customer_id ,

claim_id, agent_id, customer_id = 1, 1, 1
count = 0
claim_cat = ["Dental", "Vision", "Medical", "Life"]
fake = Faker()
agents = ()


def randAge():
    age = random.randint(18, 100)
    return age


def assigned_claims():
    # {claim_id , claim_category, reason , amount ,datetime}

    claim_category = random.choice(claim_cat)
    amount = round(random.uniform(25.00, 15000.00), 2)

    date_time = fake.date_between(start_date="-30y", end_date="today")

    approval = random.choice(["Approved", "Not Approved"])

    claim_tuple = {claim_id, claim_category, amount, date_time, approval}

    return claim_tuple


def numOfClaims():
    claims = ()
    num = random.randint(1, 15)

    for x in range(num):
        new_claim = assigned_claims()
        claims = claims + (new_claim,)

        # df = pd.read_csv("project_customer_csv.csv")

        # df[""] = claims
        # df.to_csv("project_customer_csv.csv", index=False)
    return claims


with open("5000_random_names.csv") as cliNames:
    csv_reader = csv.reader(cliNames, delimiter=",")

    counter = 1
    with open("project_customer_csv.csv", mode="w") as customer_file:
        client_writer = csv.writer(customer_file, delimiter=",")

        for name in csv_reader:
            age = randAge()
            claims = numOfClaims()
            agent_id = "null"

            while counter <= 50:
                agent_id = counter
                counter += 1
                agents.append(name)

            client_writer.writerow([customer_id, name[0], age, claims, agent_id])
            customer_id += 1
