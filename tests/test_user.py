from tests.example.app import User


def test_user(database_connection):

    for _ in range(2):
        new_user = User(name="John Doe", age=30)
        database_connection.add(new_user)
    database_connection.commit()

    users = database_connection.query(User).all()
    for user in users:
        print(user.name, user.age)
