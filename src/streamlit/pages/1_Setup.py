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

config = None
sp_session = None
if "snowpark_session" not in st.session_state:
    config = L.get_config(PROJECT_HOME_DIR)
    sp_session = L.connect_to_snowflake(PROJECT_HOME_DIR)
    sp_session.use_role(f'''{config['APP_DB']['role']}''')
    sp_session.use_schema(f'''{config['APP_DB']['database']}.{config['APP_DB']['schema']}''')
    sp_session.use_warehouse(f'''{config['APP_DB']['snow_opt_wh']}''')
    st.session_state['snowpark_session'] = sp_session
else:
    sp_session = st.session_state['snowpark_session']

#-----------------------------------------------------
# Run the Setup scripts
import os
import tarfile

# def package_images_into_archive:
# #declaring the filename
# name_of_file= "TutorialsPoint.tar"

# #opening the file in write mode
# file= tarfile.open(name_of_file,"w")

# #Adding other files to the tar file
# file.add("sql python create table.docx")
# file.add("trial.py")
# file.add("Programs.txt")

# #closing the file
# file.close()   


def upload_images_to_data_stage():
    logger.info(f" Uploading images to stage: data_stg ... ")
    l_stage = 'data_stg'
    l_stage_dir = '/images'
    l_data_dir = os.path.join(PROJECT_HOME_DIR ,'data')
    for path, currentDirectory, files in os.walk(l_data_dir):
        for file in files:
            # build the relative paths to the file
            local_file = os.path.join(path, file)

            if local_file.endswith('jpeg') == False:
                continue
        
            elif '/.' in local_file:
                continue
            
            # build the path to where the file will be staged
            stage_dir = path.replace(l_data_dir , l_stage_dir)

            print(f'    {local_file} => @{l_stage}/{stage_dir}')
            sp_session.file.put(
                local_file_name = local_file
                ,stage_location = f'{l_stage}{stage_dir}'
                ,auto_compress=False ,overwrite=True)
    
    sp_session.sql(f'alter stage {l_stage} refresh; ').collect()

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

with st.expander("Step 3- Upload sample images to stage" , False):
    script_output_2 = st.empty()
    with script_output_2.container():
        st.button('Upload'
            ,on_click=upload_images_to_data_stage
        )