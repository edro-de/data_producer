from faker import Faker

fake = Faker()
transaction = {
    "id": fake.uuid4(),
    "timestamp": fake.date_time_this_year().isoformat(),
    "amount": round(fake.pyfloat(left_digits=3, right_digits=2, positive=True), 2),
    "currency": "USD",
    "sender": fake.name(),
    "receiver": fake.name(),
}
