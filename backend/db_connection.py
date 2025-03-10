import os
import duckdb
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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
