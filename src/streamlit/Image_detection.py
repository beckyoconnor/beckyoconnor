import streamlit as st

from IPython.display import display, HTML, Image , Markdown
from snowflake.snowpark.session import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
import os ,configparser ,json ,logging
from snowflake.snowpark.functions import *
from snowflake.snowpark import *
from snowflake.snowpark.functions import call_udf

# Import the commonly defined utility scripts using
# dynamic path include
import sys
sys.path.append('../python/lutils')
import sflk_base as L

display(Markdown("### Initialization"))
logging.basicConfig(stream=sys.stdout, level=logging.ERROR)

# Source various helper functions
#%run ./scripts/notebook_helpers.py

# Define the project home directory, this is used for locating the config.ini file
PROJECT_HOME_DIR = '../../'
config = L.get_config(PROJECT_HOME_DIR)
sp_session = L.connect_to_snowflake(PROJECT_HOME_DIR)

if(sp_session == None):
    raise Exception(f'Unable to connect to snowflake. Validate connection information ')

sp_session.use_role(f'''{config['APP_DB']['role']}''')
sp_session.use_schema(f'''{config['APP_DB']['database']}.{config['APP_DB']['schema']}''')
sp_session.use_warehouse(f'''{config['APP_DB']['snow_opt_wh']}''')

df = sp_session.sql('select current_user() ,current_role() ,current_database() ,current_schema();').to_pandas()
display(df)

st.write("# Image Recognition On Chest XRays 👋")

#st.table(df)
st.subheader('This is an illustration of parsing an image to binary arrays in order to make a an estimation of \
                whether or not the patient has pneumonia or not')




SQL3 = '''select RELATIVE_PATH FROM DIRECTORY(@data_stg)'''




files = sp_session.sql(SQL3).to_pandas()
choosefile = st.selectbox('Choose Scan to analyse',files)

if choosefile:
    SQL2 = f''' select SKIMAGE_PARSER_FN(BUILD_SCOPED_FILE_URL(@data_stg,RELATIVE_PATH)) PARSED_IMAGE_INFO,* from (


            select GET_PRESIGNED_URL(@data_stg, RELATIVE_PATH) URL, FILE_URL, RELATIVE_PATH 
            FROM DIRECTORY(@data_stg) WHERE RELATIVE_PATH = '{choosefile}' limit 1)  '''
    
    SQL = f'''select GET_PRESIGNED_URL(@data_stg, RELATIVE_PATH) URL FROM DIRECTORY(@data_stg) 
            WHERE RELATIVE_PATH = '{choosefile}' limit 1;'''
    
    URL = sp_session.sql(SQL).to_pandas().iloc[0].URL
    
    st.image(URL)
    

    with st.spinner("Parsing Image for analysis"):
        def parse_image(SQLCode):
            df2 = sp_session.sql(SQLCode)
            df2 = df2.select(\
            col("FILE_URL").cast(T.StringType()).alias('IMAGE_FILE_PATH'),\
            col("URL").cast(T.StringType()).alias('DOWNLOADABLE_LINK'),\
            split("RELATIVE_PATH",lit("/"))[0].cast(T.StringType()).alias('CLASS_LABEL'),\

            col("PARSED_IMAGE_INFO")["image_array"].cast(T.VariantType()).alias('IMAGE_ARRAY'),\
            col("PARSED_IMAGE_INFO")["image_array_shape_0"].cast(T.IntegerType()).alias('IMAGE_ARRAY_SHAPE_0'),\
            col("PARSED_IMAGE_INFO")["image_array_shape_1"].cast(T.IntegerType()).alias('IMAGE_ARRAY_SHAPE_1'),\
            col("PARSED_IMAGE_INFO")["normalized_image_array"].cast(T.VariantType()).alias('NORMALIZED_IMAGE_ARRAY'),\
            col("PARSED_IMAGE_INFO")["parsing_execption"].cast(T.StringType()).alias('PARSING_EXECPTION'),\
            col("PARSED_IMAGE_INFO")["parsing_status"].cast(T.BooleanType()).alias('STATUS'),\
            col("PARSED_IMAGE_INFO")["resized_feature"].cast(T.VariantType()).alias('RESIZED_FEATURE')\
            )
            return df2

        facts = parse_image(SQL2).to_pandas().iloc[0]
        st.caption("Downloadable Link: " + facts.DOWNLOADABLE_LINK)
        st.caption("Array Shape 0: " + str(facts.IMAGE_ARRAY_SHAPE_0))
        st.caption("Array Shape 1: " + str(facts.IMAGE_ARRAY_SHAPE_1))
        st.caption("Parsing Status: " + str(facts.STATUS))

    with st.spinner("Running Predicted Diagnosis"):
        def prediction(any):
            df3 = parse_image(SQL2).select(call_udf("INFER_PNEUMONIA"\
            ,col("IMAGE_ARRAY_SHAPE_0")\
            ,col("IMAGE_ARRAY_SHAPE_1")\
            ,col("RESIZED_FEATURE")).alias('Prediction'))

            if df3.to_pandas().iloc[0].PREDICTION == 1:
                diagnosis = st.markdown('The Image above is to believed to be **:blue[NORMAL]**')
            else:
                diagnosis = st.markdown('The Predictive Model has detected a high probability that the patient has **:red[pneumonia]**')
            return diagnosis

    
        prediction(1)
