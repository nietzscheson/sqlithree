import pytest
from tests.example.app import session as session_base, Base, engine

@pytest.fixture
def session():

    Base.metadata.create_all(engine)
    # yield session_base

    # session_base.close()
    return session_base

@pytest.fixture
def database_connection(session):

    #for table in reversed(Base.metadata.sorted_tables):
    #    session.execute(table.delete())

    # session.commit()

    yield session

    session.close()

    # return session
