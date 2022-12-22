import sys ,os ,io ,json ,logging 
from snowflake.snowpark.session import Session
import _snowflake
import tarfile

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("skimage_parser_fn")


def main(p_session: Session ,p_archive_flname: str) -> dict:
    l_local_dir = '/tmp/sample_images'
    l_stage_dir = '/images'

    ret = {}
    status = False
    try:
        # extract archive to local dir
        os.makedirs(os.path.dirname(l_local_dir), exist_ok=True)

        with tarfile.open(_snowflake.open(p_archive_flname), 'r|') as input_tar:
            
tarinfo = input_tar.next()
fileobj = input_tar.extractfile(tarinfo)



        # with tarfile.open( _snowflake.open(p_archive_flname), "r:gz") as tar_f:
        with tarfile.open( _snowflake.open(p_archive_flname), "r|") as tar_f:
            tar_f.extractall(l_local_dir)

        # Upload to stage
        for path, currentDirectory, files in os.walk(l_local_dir):
            for file in files:
                # build the relative paths to the file
                local_file = os.path.join(path, file)

                # build the path to where the file will be staged
                l_stage_dir = path.replace(l_local_dir , l_stage_dir)
                p_session.file.put(
                    local_file_name = local_file
                    ,stage_location = f'data_stg/{l_stage_dir}'
                    ,auto_compress=False ,overwrite=True)
    
        p_session.sql(f'alter stage data_stg refresh; ').collect()
        status = True
    except Exception as e:
        ex = str(e)
        ret['exception'] = ex

    ret['status'] = status
    return ret
