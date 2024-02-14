import streamlit as st
import pandas as pd
from database import (
    create_database,
    add_entity,
    get_entities_df,
    get_entity,
    update_entity,
    delete_entity,
    bulk_insert_entities,
)

create_database()
st.title("Admin Panel")

with st.form("add_entity"):
    st.subheader("Add New Entity")
    col1, col2, col3 = st.columns(3)
    data = [
        col1.text_input("Name"),
        col1.text_input("Alias"),
        col1.text_input("Type"),
        col1.text_area("Attribution"),
        col1.text_input("Attribution links"),
        col1.text_input("Attribution type"),
        col1.slider("Attribution confidence 1-5", 1, 5, 3),
        col1.text_input("Label"),
        col1.text_input("Parent actor"),
        col1.text_input("Subsidiary actors"),
        col2.text_input("Threat Actor"),
        col2.text_input("TTPs"),
        col2.text_area("Description of TTPs"),
        col2.text_input("Description TTPs Link"),
        col2.text_input("Master Narratives"),
        col2.text_area("Master Narrative Description"),
        col2.text_input("Master Narrative Links"),
        col2.text_area("Summary"),
        col2.text_input("External links (articles, wikipedia page etc)."),
        col2.text_input("Language"),
        col3.text_input("Country"),
        col3.text_input("Sub-region"),
        col3.text_input("Region"),
        col3.text_input("Website"),
        col3.text_input("Twitter"),
        col3.text_input("Twitter ID"),
        col3.text_input("Facebook"),
        col3.text_input("Threads"),
        col3.text_input("YouTube"),
        col3.text_input("YouTube ID"),
        col3.text_input("TikTok"),
        col3.text_input("Instagram"),
        col3.text_input("LinkedIn"),
        col3.text_input("Reddit"),
        col3.text_input("VK"),
        col3.text_input("Telegram"),
        col3.text_input("Substack"),
        col3.text_input("Quora"),
        col3.text_input("Patreon"),
        col3.text_input("GoFundMe"),
        col3.text_input("Paypal"),
        col3.text_input("Twitch"),
        col3.text_input("Mastadon"),
        col3.text_input("Wechat"),
        col3.text_input("QQ"),
        col3.text_input("Douyin"),
    ]
    submit_entity = st.form_submit_button("Add Entity")
    if submit_entity:
        add_entity(tuple(data))

uploaded_file = st.file_uploader("Upload CSV", type="csv")
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file, encoding="utf-8")
    bulk_insert_entities(df)
    st.success("CSV data uploaded successfully!")

# Displaying entities in a DataTable
st.write("Existing Entities:")
entities_df = get_entities_df()
if not entities_df.empty:
    edited_df = st.data_editor(
        entities_df,
        hide_index=True,
        column_config={"Select": st.column_config.CheckboxColumn(required=True)},
        disabled=entities_df.columns[
            1:
        ],  # Disable editing for all columns except 'Select'
    )

    # Get selected rows
    selected_rows = edited_df[edited_df["Select"]]
    selected_ids = selected_rows["id"].values

    if len(selected_ids) > 0:
        # For simplicity, editing the first selected entity
        selected_id = selected_ids[0]
        selected_entity = get_entity(selected_id).iloc[0]

        st.sidebar.title("Edit Entity")
        edited_data = {}
        for col in selected_entity.index:
            edited_data[col] = st.sidebar.text_input(col, str(selected_entity[col]))

        col1, col2 = st.sidebar.columns(2)

        # Update button
        with col1:
            if st.button("Update Entity"):
                update_entity(selected_id, edited_data)
                st.sidebar.success("Entity updated successfully!")
                st.rerun()

        # Delete button
        with col2:
            if st.button("Delete Entity", key="delete", help="Delete this entity"):
                delete_entity(selected_id)
                st.rerun()

else:
    st.write("No entities found in the database.")
