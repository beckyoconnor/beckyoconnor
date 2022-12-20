# streamlit run src/streamlit/App.py

from snowflake.snowpark.session import Session
import streamlit as st
import logging ,sys
from util_fns import exec_sql_script

# Import the commonly defined utility scripts using
# dynamic path include
import sys
sys.path.append('src/python/lutils')
import sflk_base as L

# Define the project home directory, this is used for locating the config.ini file
PROJECT_HOME_DIR='.'

logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
logger = logging.getLogger('exec_sql_script')

st.markdown(f"# Execute SQL Script")
st.write("""
    This page is used for running a sample SQL script. These SQL scripts would typically involve
    such activities like creating database, stored procs, roles ,stage etc..
""")

# Initialize a session with Snowflake
# sp_session = None
# if "snowpark_session" not in st.session_state:
#     sp_session = L.connect_to_snowflake(PROJECT_HOME_DIR)
#     st.session_state['snowpark_session'] = sp_session
# else:
#     sp_session = st.session_state['snowpark_session']

#-----------------------------------------------------
# Run the Setup scripts


with st.expander("Step 1- Setup database and schemas"):
    script_output = st.empty()
    btn_run_script = st.button('Setup database'
            ,on_click=exec_sql_script
            ,args = ('./src/sql-script/1_setup.sql' ,script_output)
        )

with st.expander("Step 2- Define functions and procedures" , False):
    script_output_2 = st.empty()
    with script_output_2.container():
        st.button('Define functions and procedures'
            ,on_click=exec_sql_script
            ,args = ('./src/sql-script/2_define_fns.sql' ,script_output_2)
        )