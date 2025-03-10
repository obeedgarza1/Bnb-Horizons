import os
import duckdb
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
DATABASE_DUCK = os.getenv("DATABASE_DUCK")


def get_sqlalchemy_session():
    """Return a new SQLAlchemy session"""
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    Session = sessionmaker(bind=engine)

    return engine, Session()


def get_duckdb_connection():
    """Return the DuckDB connection"""
    con = duckdb.connect()

    # Install and load the PostgreSQL extension
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")

    # Attach the AWS Aurora PostgreSQL database
    con.execute(f"ATTACH '{DATABASE_URL}' AS pgdb (TYPE postgres)")
    return con


import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_sqlite(path):
    Base = declarative_base()
    """Return a new SQLAlchemy session using SQLite in the data folder"""

    engine = create_engine(f"sqlite:///{path}", echo=True)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    return engine, session


import duckdb


def get_duckdb_sqlite(path):
    """Return the DuckDB connection and attach an SQLite database"""
    # Create a DuckDB connection
    con = duckdb.connect()

    # Attach the SQLite database
    sqlite_db_path = path  # Path to your SQLite database file
    con.execute(f"ATTACH '{sqlite_db_path}' AS sqlite_db")

    return con
