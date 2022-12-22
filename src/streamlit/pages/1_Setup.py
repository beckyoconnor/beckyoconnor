# streamlit run src/streamlit/App.py

from snowflake.snowpark.session import Session
import streamlit as st
import logging ,sys
import util_fns as U

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

def archive_images(p_archive_flname: str):
    logger.info(f'Archiving images {p_archive_flname}...')
    with tarfile.open(p_archive_flname, "w:gz") as tar:
        tar.add(f'{PROJECT_HOME_DIR}/data', arcname=os.path.basename(p_archive_flname))
    
    return p_archive_flname


# def extract_images_in_stage(p_session: Session ,p_archive_flname: str) -> dict:
#     l_local_dir = '/tmp/sample_images'
#     l_stage_dir = '/images'
#     import _snowflake

#     ret = {}
#     status = False
#     try:
#         # extract archive to local dir
#         os.makedirs(os.path.dirname(l_local_dir), exist_ok=True)
#         with tarfile.open( _snowflake.open(p_archive_flname), "r:gz") as tar_f:
#             tar_f.extractall(l_local_dir)

#         # Upload to stage
#         for path, currentDirectory, files in os.walk(l_local_dir):
#             for file in files:
#                 # build the relative paths to the file
#                 local_file = os.path.join(path, file)

#                 # build the path to where the file will be staged
#                 l_stage_dir = path.replace(l_local_dir , l_stage_dir)
#                 p_session.file.put(
#                     local_file_name = local_file
#                     ,stage_location = f'data_stg/{l_stage_dir}'
#                     ,auto_compress=False ,overwrite=True)
    
#         sp_session.sql(f'alter stage data_stg refresh; ').collect()
#         status = True
#     except Exception as e:
#         ex = str(e)
#         ret['exception'] = ex

#     ret['status'] = status
#     return ret

def upload_images_to_data_stage():
    l_archive_baseflname = 'sample_pneumonia.tar.gz'
    l_archive_flname = f'.app_store/{l_archive_baseflname}'
    l_stage = 'data_stg'

    # archive_images(l_archive_flname)
    
    # logger.info(f" Uploading images to stage: ... ")
    # sp_session.file.put(
    #             local_file_name = l_archive_flname
    #             ,stage_location = f'{l_stage}'
    #             ,auto_compress=False ,overwrite=True)
    
    logger.info(f" Extracting archived images in stage: ... ")
    # sp_session.add_packages('snowflake-snowpark-python')
    # sp_session.sproc.register(extract_images_in_stage 
    #     ,name="extract_images_in_stage" ,replace=True 
    #     ,is_permanent=True ,stage_location="@model_stg/sproc")
    sp_session.sql(f'''  call extract_images_in_stagecreate('@data_stg/{l_archive_baseflname}'); ''').collect()

    stg_df = U.list_stage(sp_session ,'data_stg')
    st.dataframe(stg_df)

    sp_session.sql(f'alter stage {l_stage} refresh; ').collect()




# =======
with st.expander("Step 1- Setup database and schemas"):
    script_output = st.empty()
    btn_run_script = st.button('Setup database'
            ,on_click=U.exec_sql_script
            ,args = ('./src/sql-script/1_setup.sql' ,script_output)
        )

with st.expander("Step 2- Define functions and procedures" , False):
    script_output_2 = st.empty()
    with script_output_2.container():
        st.button('Define functions and procedures'
            ,on_click=U.exec_sql_script
            ,args = ('./src/sql-script/2_define_fns.sql' ,script_output_2)
        )

with st.expander("Step 3- Upload sample images to stage" , False):
    script_output_3 = st.empty()
    with script_output_3.container():
        st.button('Upload'
            ,on_click=upload_images_to_data_stage
        )