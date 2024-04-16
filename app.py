import streamlit as st

import pandas as pd
import numpy as np
import subprocess
import os
from core import pubmed_batch_download
from core import pubmed_search


# Define your search_pubmed, fetch_article_details, and run_streamlit_app functions here...

def run_upload_script(search_term, min_date, max_date):
    min_date = str(min_date)
    max_date = str(max_date)
    venv_path = os.path.join(os.path.dirname(__file__), 'pyenv', 'Scripts', 'activate')
    command = f"\"{venv_path}\" && python ingest.py \"{search_term}\" {min_date} {max_date}"
    # st.write("Passed Params " + min_date + "  " + max_date)
    # st.write("Command  " + "  " + command)
    subprocess.Popen(command, shell=True, creationflags=subprocess.CREATE_NEW_CONSOLE)
    # subprocess.Popen(["python", "ingest.py", search_term], creationflags=subprocess.CREATE_NEW_CONSOLE)


# Define function to fetch article details from PubMed using Biopython
@st.cache_data
def fetch_article_details(search_term, search_results, batch_size):
    try:
        return pubmed_batch_download(search_term, search_results, batch_size)
    except Exception as e:
        st.error(e)
        return None


# PubMed search function
# @st.cache_data
def search_pubmed(search_term, min_date, max_date):
    try:
        return pubmed_search(search_term, min_date, max_date)
    except Exception as e:
        st.error(f"Error occurred during PubMed search: {e}")
        return []


def set_state():
    st.session_state["Upload"] = True


# Streamlit app code
def run_streamlit_app():
    display_only = 31
    st.set_page_config(
        page_title="BioPharm Communications Pubmed",
        page_icon="chart_with_upwards_trend",
        layout="wide",
        initial_sidebar_state="collapsed")

    hide_menu_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
    st.markdown(hide_menu_style, unsafe_allow_html=True)
    st.markdown("<h1 style='text-align: center; color: red;'> PUBMED SEARCH </h1>", unsafe_allow_html=True)
    st.markdown(
        """
    <style>
    button {
        height: auto;
        padding-top: 20px !important;
        padding-bottom: 20px !important;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )

    for __ in range(3):
        st.write("\n")

        # Create three columns layout
    _, col2, col3 = st.columns([1, 3, 1])
    # Add label and text input in the second column
    with col2:
        st.markdown("<div style='font-size: 25px;color: Blue;'>Enter Your Search Term</span></div>",
                    unsafe_allow_html=True)
        search_term = st.text_input(" ", key="search term", value="", help='Type your search term here.')
        srchbtn = st.button("Search", key="Srchkey", use_container_width=True)
    with col3:
        advanced_search = st.toggle("Advanced Search", key="dateinput", value=False)
        min_date = None
        max_date = None
        if advanced_search:
            dc1, dc2 = st.columns(2)
            with dc1:
                min_date = st.date_input("FROM")
            with dc2:
                max_date = st.date_input("TO")

    if search_term:
        search_term = search_term.capitalize()  # Optionally capitalize the input

    state = st.session_state.get("Upload", None)
    if state is None:
        state = True and srchbtn
    else:
        state = state or srchbtn
    # Perform search when the search button is clicked
    if state and search_term:
        with st.spinner("Searching..."):
            count, search_results = search_pubmed(search_term, min_date, max_date)
            _, col2, col3, col4, _ = st.columns(5)
            reserved = st.container()
            with col2:
                st.markdown(
                    f"<div style='font-size: 20px;'>Total Available PMID'S for {search_term} are  <span style='color: red; font-size: 24px;'><b> {count}</b></span></div>",
                    unsafe_allow_html=True)
            if count > 0:
                display_only = min(count + 1, display_only)
                with col3:
                    st.markdown(
                        f"<div style='font-size: 20px;'>Displaying Top ↗️📈 <span style='color: red; font-size: 24px;'><b>{display_only - 1}</b></span></div>",
                        unsafe_allow_html=True)
                results = fetch_article_details(search_term, search_results, 30)
                if results:
                    with col4:
                        if count > 9999:
                            st.warning(f"Please use advanced search since the output is greater than the API limit 9999")
                        else:
                            uploadbtn = st.button("Upload", key="Uploadkey", use_container_width=True, on_click=set_state)
                            with reserved:
                                if uploadbtn:
                                    st.balloons()
                                    run_upload_script(search_term, min_date, max_date)
                                    st.success("Upload Started")
                                    st.session_state["Upload"] = False
                    st.balloons()
                    df = pd.DataFrame(results)
                    df.index = np.arange(1, len(df) + 1)
                    # st.dataframe(df,height=800,width=2000,use_container_width=True)
                    st.table(df)

                else:
                    st.warning("Unable to fetch Details")
            else:
                st.write("No search results found.")


if __name__ == '__main__':
    try:
        run_streamlit_app()
    except KeyboardInterrupt:
        exit(0)