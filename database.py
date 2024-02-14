from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text, inspect
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import os
import pandas as pd
from contextlib import contextmanager
import streamlit as st


load_dotenv()
external_db_url = os.getenv("EXTERNAL_DB_URL")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")


def create_database():
    engine = create_engine(external_db_url)
    metadata = MetaData()

    # Define the table structure
    entities_table = Table(
        "Entities",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("Name", Text),
        Column("Alias", Text),
        Column("Type", Text),
        Column("Attribution", Text),
        Column("Attribution_links", Text),
        Column("Attribution_type", Text),
        Column("Attribution_confidence", Integer),
        Column("Label", Text),
        Column("Parent_actor", Text),
        Column("Subsidiary_actors", Text),
        Column("Threat_Actor", Text),
        Column("TTPs", Text),
        Column("Description_of_TTPs", Text),
        Column("Description_TTPs_Link", Text),
        Column("Master_Narratives", Text),
        Column("Master_Narrative_Description", Text),
        Column("Master_Narrative_Links", Text),
        Column("Summary", Text),
        Column("External_links", Text),
        Column("Language", Text),
        Column("Country", Text),
        Column("Sub_region", Text),
        Column("Region", Text),
        Column("Website", Text),
        Column("Twitter", Text),
        Column("Twitter_ID", Text),
        Column("Facebook", Text),
        Column("Threads", Text),
        Column("YouTube", Text),
        Column("YouTube_ID", Text),
        Column("TikTok", Text),
        Column("Instagram", Text),
        Column("LinkedIn", Text),
        Column("Reddit", Text),
        Column("VK", Text),
        Column("Telegram", Text),
        Column("Substack", Text),
        Column("Quora", Text),
        Column("Patreon", Text),
        Column("GoFundMe", Text),
        Column("Paypal", Text),
        Column("Twitch", Text),
        Column("Mastadon", Text),
        Column("Wechat", Text),
        Column("QQ", Text),
        Column("Douyin", Text),
    )

    # Check if table exists and has the required columns
    inspector = inspect(engine)
    try:
        existing_columns = [col["name"] for col in inspector.get_columns("Entities")]
        required_columns = [column.name for column in entities_table.columns]
        if not all(col in existing_columns for col in required_columns):
            print("Required columns not found. Creating/Updating table...")
            metadata.create_all(engine)
        else:
            print("All required columns are present.")
    except NoSuchTableError:
        print("Table not found. Creating new table...")
        metadata.create_all(engine)


def connect_to_database():
    return create_engine(external_db_url)


# Context manager for SQLAlchemy engine
@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    engine = connect_to_database()
    connection = engine.connect()
    try:
        yield connection
        connection.commit()
    except:
        connection.rollback()
        raise
    finally:
        connection.close()


# Refactored functions
def get_entity(id):
    try:
        with session_scope() as conn:
            return pd.read_sql_query(f"SELECT * FROM Entities WHERE id = {id}", conn)
    except Exception as e:
        print("Error in get_entity:", e)
        return None


def add_entity(data):
    try:
        with session_scope() as conn:
            placeholders = ", ".join(["%s"] * len(data))
            conn.execute(f"INSERT INTO Entities VALUES (DEFAULT, {placeholders})", data)
    except Exception as e:
        print("Error in add_entity:", e)


def delete_entity(id):
    try:
        if not entity_exists(id):
            return st.error(f"Entity with id {id} does not exist.")

        with session_scope() as conn:
            conn.execute(f"DELETE FROM Entities WHERE id = {id}")
    except Exception as e:
        print("Error in delete_entity:", e)


def entity_exists(id):
    with session_scope() as conn:
        result = conn.execute("SELECT 1 FROM Entities WHERE id = %s", (id,))
        return result.fetchone() is not None


def get_entities_df():
    try:
        with session_scope() as conn:
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'Entities'"
            )
            if result.fetchone():
                return pd.read_sql_query("SELECT * FROM Entities", conn)
            else:
                print(
                    "The 'Entities' table does not exist. Please run `create_database()` to create it."
                )
                return pd.DataFrame()
    except Exception as e:
        print("Error in get_entities_df:", e)
        return pd.DataFrame()


def bulk_insert_entities(df):
    try:
        with session_scope() as conn:
            df.columns = [col.replace(" ", "_") for col in df.columns]
            df.to_sql("Entities", conn, if_exists="append", index=False)
    except Exception as e:
        print("Error in bulk_insert_entities:", e)


def update_entity(id, data):
    try:
        if not entity_exists(id):
            return st.error(f"Entity with id {id} does not exist.")

        with session_scope() as conn:
            placeholders = ", ".join(f"{col} = %s" for col in data)
            conn.execute(
                f"UPDATE Entities SET {placeholders} WHERE id = {id}",
                list(data.values()),
            )
    except Exception as e:
        print("Error in update_entity:", e)
