import os
import random
from faker import Faker
import logging

fake = Faker()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# 1. Generate Fake User Profiles
def generate_user_profile():
    profile = {
        "user_id": fake.uuid4(),
        "username": fake.user_name(),
        "email": fake.email(),
        "created_at": fake.date_time_this_decade(),
    }
    logging.debug(f"Generated user profile: {profile}")
    return profile


# 2. Generate Fake Product Data
def generate_product():
    product = {
        "product_id": fake.uuid4(),
        "product_name": fake.word().capitalize(),
        "category": random.choice(["Electronics", "Clothing", "Books", "Toys"]),
        "price": round(random.uniform(10, 1000), 2),
        "stock": random.randint(0, 500),
        "description": fake.text(max_nb_chars=200),
        "created_at": fake.date_time_this_year(),
    }
    logging.debug(f"Generated product: {product}")
    return product


# 3. Generate Fake Financial Transactions
def generate_transaction(user_ids, product_ids):
    transaction = {
        "transaction_id": fake.uuid4(),
        "user_id": random.choice(user_ids),  # Use a user_id from the user_profiles
        "product_id": random.choice(product_ids),  # Use a product_id from the products
        "amount": round(random.uniform(10, 10000), 2),
        "transaction_type": random.choice(["Credit", "Debit"]),
        "date": fake.date_time_this_year(),
        "description": fake.sentence(nb_words=6),
    }
    logging.debug(f"Generated transaction: {transaction}")
    return transaction
