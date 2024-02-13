import psycopg2
from dotenv import load_dotenv
import os
import os
import pandas as pd
import psycopg2
from contextlib import contextmanager
import streamlit as st

load_dotenv()
external_db_url = os.getenv("EXTERNAL_DB_URL")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")


def create_database():
    external_db_url = os.getenv("EXTERNAL_DB_URL")
    conn = psycopg2.connect(
        f"postgres://{db_user}:{db_password}@{external_db_url}/friki_db"
    )
    c = conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS Entities (
            id SERIAL PRIMARY KEY,
            Name TEXT,
            Alias TEXT,
            Type TEXT,
            Attribution TEXT,
            Attribution_links TEXT,
            Attribution_type TEXT,
            Attribution_confidence INTEGER,
            Label TEXT,
            Parent_actor TEXT,
            Subsidiary_actors TEXT,
            Threat_Actor TEXT,
            TTPs TEXT,
            Description_of_TTPs TEXT,
            Description_TTPs_Link TEXT,
            Master_Narratives TEXT,
            Master_Narrative_Description TEXT,
            Master_Narrative_Links TEXT,
            Summary TEXT,
            External_links TEXT,
            Language TEXT,
            Country TEXT,
            Sub_region TEXT,
            Region TEXT,
            Website TEXT,
            Twitter TEXT,
            Twitter_ID TEXT,
            Facebook TEXT,
            Threads TEXT,
            YouTube TEXT,
            YouTube_ID TEXT,
            TikTok TEXT,
            Instagram TEXT,
            LinkedIn TEXT,
            Reddit TEXT,
            VK TEXT,
            Telegram TEXT,
            Substack TEXT,
            Quora TEXT,
            Patreon TEXT,
            GoFundMe TEXT,
            Paypal TEXT,
            Twitch TEXT,
            Mastadon TEXT,
            Wechat TEXT,
            QQ TEXT,
            Douyin TEXT
        )
    """
    )

    conn.commit()
    conn.close()
    print("Database created!")


def connect_to_database():
    external_db_url = os.getenv("EXTERNAL_DB_URL")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    conn = psycopg2.connect(
        f"postgres://{db_user}:{db_password}@{external_db_url}/friki_db"
    )
    return conn


@contextmanager
def connect_db():
    conn = psycopg2.connect(
        f"postgres://{db_user}:{db_password}@{external_db_url}/{db_name}"
    )
    try:
        yield conn
    finally:
        conn.close()


def get_entity(id):
    try:
        with connect_db() as conn:
            return pd.read_sql_query(f"SELECT * FROM Entities WHERE id = {id}", conn)
    except Exception as e:
        print("Error in get_entity:", e)
        return None


def add_entity(data):
    try:
        with connect_db() as conn:
            placeholders = ", ".join(["%s"] * len(data))
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO Entities VALUES (DEFAULT, {placeholders})", data
            )
            conn.commit()
    except Exception as e:
        print("Error in add_entity:", e)


def delete_entity(id):
    try:
        if not entity_exists(id):
            return st.error(f"Entity with id {id} does not exist.")

        with connect_db() as conn:
            conn.execute(f"DELETE FROM Entities WHERE id = {id}")
            conn.commit()
            print("Deleted entity with id:", id)
    except Exception as e:
        print("Error in delete_entity:", e)


def entity_exists(id):
    with connect_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM Entities WHERE id = %s", (id,))
        return cursor.fetchone() is not None


def get_entities_df():
    try:
        with connect_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'Entities'"
            )
            if cursor.fetchone():
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
        with connect_db() as conn:
            df.columns = [col.replace(" ", "_") for col in df.columns]
            df.to_sql("Entities", conn, if_exists="append", index=False)
            conn.commit()
    except Exception as e:
        print("Error in bulk_insert_entities:", e)


def get_entities_df_with_selection():
    df = get_entities_df()
    if not df.empty:
        df.insert(0, "Select", [False] * len(df))
    return df


def update_entity(id, data):
    try:
        if not entity_exists(id):
            return st.error(f"Entity with id {id} does not exist.")

        with connect_db() as conn:
            placeholders = ", ".join(f"{col} = ?" for col in data)
            conn.execute(
                f"UPDATE Entities SET {placeholders} WHERE id = {id}",
                list(data.values()),
            )
            conn.commit()
    except Exception as e:
        print("Error in update_entity:", e)


if __name__ == "__main__":
    create_database()
