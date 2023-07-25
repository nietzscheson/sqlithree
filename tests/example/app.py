from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import os
import json

from sqlalchemy.dialects import registry

import sqlithree

registry.register("sqlithree", "sqlithree.dialect", "SQLithreeDialect")

engine = create_engine("sqlithree:///db.sqlite")
# engine = create_engine("sqlite:///db.sqlite")

Base = declarative_base()

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)
