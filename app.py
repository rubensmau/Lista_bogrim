import streamlit as st
from google.cloud import bigquery
import pandas as pd
import numpy as np

# --- Page Configuration ---
st.set_page_config(
    page_title="Dror - Lista de Bogrim",
    page_icon="🧊",
    layout="wide",
)

# --- BigQuery Configuration ---
PROJECT_ID = "documents-464020"
DATASET_ID = "bogrim"
TABLE_ID = "v2"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


# --- Helper Functions ---
@st.cache_resource
def get_bigquery_client():
    """Returns a BigQuery client object."""
    try:
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Could not create BigQuery client: {e}")
        return None


@st.cache_data(ttl=3600)
def get_table_schema(_client):
    """Fetches the table schema and returns the schema object."""
    if not _client:
        return []
    try:
        table = _client.get_table(TABLE_REF)
        return table.schema
    except Exception as e:
        st.error(f"Could not get table schema: {e}")
        return []


# <<< NEW FUNCTION >>>
def get_next_id(_client):
    """
    Queries the table to find the maximum existing ID and returns the next ID.
    Handles the case of an empty table by starting from 1.
    """
    if not _client:
        return None
    # COALESCE handles the case where the table is empty and MAX(id) would be NULL.
    query = f"SELECT COALESCE(MAX(id), 0) as max_id FROM `{TABLE_REF}`"
    try:
        query_job = _client.query(query)
        results = query_job.result()
        row = next(results)
        next_id = row.max_id + 1
        return next_id
    except Exception as e:
        st.error(f"Could not determine next ID: {e}")
        return None


def get_data(_client, filters):
    """Fetches data from the BigQuery table based on filters (case-insensitive)."""
    if not _client:
        return pd.DataFrame()

    # We always select all columns including 'id' for backend logic
    query = f"SELECT * FROM `{TABLE_REF}`"
    where_clauses = []
    query_params = []

    for column, value in filters.items():
        if value:
            param_name = f"{column.replace('.', '_')}"
            where_clauses.append(f"LOWER(CAST({column} AS STRING)) LIKE @{param_name}")
            search_value = f"%{value.lower()}%"
            query_params.append(
                bigquery.ScalarQueryParameter(param_name, "STRING", search_value)
            )

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += " ORDER BY id DESC LIMIT 1000"  # Good practice to have a consistent order

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        df = _client.query(query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        st.error(f"An error occurred while fetching data: {e}")
        return pd.DataFrame()


def insert_row(_client, row_data):
    """Inserts a new row into the BigQuery table."""
    if not _client:
        return False, "BigQuery client not available."
    try:
        errors = _client.insert_rows_json(TABLE_REF, [row_data])
        if not errors:
            return True, "Row inserted successfully!"
        else:
            return False, f"Failed to insert row: {errors}"
    except Exception as e:
        return False, f"An error occurred while inserting data: {e}"


def update_row(_client, new_data, unique_id_column, unique_id_value, schema):
    """Updates an existing row in the BigQuery table."""
    if not _client:
        return False, "BigQuery client not available."
    # The logic here is already robust and doesn't need changes,
    # as it correctly separates the WHERE clause ID from the SET data.
    unique_id_field = next(
        (field for field in schema if field.name == unique_id_column), None
    )
    if not unique_id_field:
        return False, f"Unique ID column '{unique_id_column}' not found."
    unique_id_type = unique_id_field.field_type

    # new_data from the form will not contain 'id', which is correct.
    set_clauses = ", ".join([f"{key} = @{key}" for key in new_data.keys()])
    query = f"UPDATE `{TABLE_REF}` SET {set_clauses} WHERE {unique_id_column} = @{unique_id_column}_val"

    params = []
    for key, value in new_data.items():
        field_type = next(
            (field.field_type for field in schema if field.name == key), "STRING"
        )
        converted_value = None
        if value is None or str(value).strip() == "":
            converted_value = None
        elif field_type == "INT64":
            try:
                converted_value = int(float(value))
            except (ValueError, TypeError):
                converted_value = None
        elif field_type == "FLOAT64":
            try:
                converted_value = float(value)
            except (ValueError, TypeError):
                converted_value = None
        elif field_type == "BOOL":
            converted_value = str(value).lower() in ("true", "1", "t", "y", "yes")
        else:
            converted_value = str(value)
        params.append(bigquery.ScalarQueryParameter(key, field_type, converted_value))

    params.append(
        bigquery.ScalarQueryParameter(
            f"{unique_id_column}_val", unique_id_type, unique_id_value
        )
    )
    job_config = bigquery.QueryJobConfig(query_parameters=params)

    try:
        _client.query(query, job_config=job_config).result()
        return True, "Row updated successfully!"
    except Exception as e:
        return False, f"An error occurred while updating data: {e}"


# --- Main Application ---
st.title("Lista de Bogrim - Busca e Atualização")
st.write(f"Interacting with `{TABLE_REF}`")

if "status_message" in st.session_state:
    success, message_text = st.session_state.status_message
    if success:
        st.success(message_text)
    else:
        st.error(message_text)
    del st.session_state.status_message

client = get_bigquery_client()
if client:
    schema = get_table_schema(client)
    if not schema:
        st.stop()

    # <<< MODIFIED >>>
    # Get all column names, and a separate list for display (without 'id')
    all_column_names = [field.name for field in schema]
    display_column_names = [col for col in all_column_names if col != "id"]

    # Assume 'id' is the unique key for updates.
    unique_id_column = "id"
    if unique_id_column not in all_column_names:
        st.error(
            f"The required unique key '{unique_id_column}' was not found in the table. The app cannot function."
        )
        st.stop()

    with st.sidebar:
        st.header("Busca")
        with st.expander("**Filtros de Busca**", expanded=True):
            filters = {}
            # <<< MODIFIED: Loop over display_column_names for the UI
            for col in display_column_names:
                filters[col] = st.text_input(f"Filtro por {col}", key=f"filter_{col}")

        with st.expander("**Adicionar Novo Registo**"):
            # <<< MODIFIED: clear_on_submit is now handled by the rerun
            with st.form("add_row_form"):
                new_row_data = {}
                # <<< MODIFIED: Loop over display_column_names for the UI
                for col in display_column_names:
                    new_row_data[col] = st.text_input(f"Enter {col}", key=f"add_{col}")

                submitted = st.form_submit_button("Add Row")
                if submitted:
                    # <<< MODIFIED: Get the next ID and add it to the data
                    next_id = get_next_id(client)
                    if next_id is not None:
                        new_row_data[unique_id_column] = (
                            next_id  # Add the id to the dict
                        )
                        success, message = insert_row(client, new_row_data)
                        st.session_state.status_message = (success, message)
                        if success:
                            if "data_df" in st.session_state:
                                del st.session_state["data_df"]  # Force a data refresh
                        st.rerun()

    st.header("Resultados da Busca")
    if any(filters.values()):
        if "data_df" not in st.session_state:
            st.session_state.data_df = get_data(client, filters)

        if not st.session_state.data_df.empty:
            st.info(
                "Edit cells directly in the table. The update form will appear below."
            )

            # <<< MODIFIED: Use column_config to hide the 'id' column
            edited_df = st.data_editor(
                st.session_state.data_df,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                key="data_editor",
                disabled=[unique_id_column],  # Also make it non-editable as a safeguard
                column_config={
                    unique_id_column: None  # This is the command to hide the column
                },
            )

            st.header("Atualizar Registro")

            if st.session_state.get("data_editor", {}).get("edited_rows"):
                edited_row_index = list(
                    st.session_state["data_editor"]["edited_rows"].keys()
                )[0]
                original_row = st.session_state.data_df.iloc[edited_row_index]
                edited_data_for_row = st.session_state["data_editor"]["edited_rows"][
                    edited_row_index
                ]

                selected_row_for_form = original_row.to_dict()
                selected_row_for_form.update(edited_data_for_row)

                original_unique_id_value = original_row[unique_id_column]
                if isinstance(original_unique_id_value, np.generic):
                    original_unique_id_value = original_unique_id_value.item()

                st.session_state["update_info"] = {
                    "unique_id_value": original_unique_id_value,
                    "form_data": selected_row_for_form,
                }

            if "update_info" in st.session_state:
                info = st.session_state["update_info"]
                st.write(
                    f"Updating row where **{unique_id_column}** is **{info['unique_id_value']}**."
                )

                with st.form("update_row_form"):
                    updated_data = {}
                    # <<< MODIFIED: Loop over display_column_names for the UI
                    for col in display_column_names:
                        default_val = info["form_data"].get(col, "")
                        updated_data[col] = st.text_input(
                            f"{col}", value=str(default_val), key=f"update_{col}"
                        )

                    update_submitted = st.form_submit_button("Update Row")
                    if update_submitted:
                        success, message = update_row(
                            client,
                            updated_data,
                            unique_id_column,
                            info["unique_id_value"],
                            schema,
                        )
                        st.session_state.status_message = (success, message)
                        if success:
                            del st.session_state["update_info"]
                            del st.session_state["data_df"]
                        st.rerun()
            else:
                st.warning(
                    "Atualize uma linha na tabela acima para habilitar o formulário de atualização. Não esqueça de clicar no botão de atualização."
                )  # <<< MODIFIED: Use the correct column name Edit a row in the table above to enable the update form.")
        else:
            st.warning("No data found for the given filters.")
            if "data_df" in st.session_state:
                del st.session_state["data_df"]
    else:
        st.info("👈 Enter one or more filters in the sidebar to search for data.")
        if "data_df" in st.session_state:
            del st.session_state["data_df"]
        if "update_info" in st.session_state:
            del st.session_state["update_info"]
