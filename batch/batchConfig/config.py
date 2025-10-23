
from dotenv import load_dotenv
import os 
import yaml 
load_dotenv('/project/.env')



class Config :
    with open('/project/batch/batchConfig/config.yml','r') as file:
        config_data = yaml.load(file , Loader=yaml.FullLoader)

        landing_path = config_data['LANDING_PATH']
        bucket_name = config_data['BUCKET']
        bronze_prefix = config_data['BRONZE']

        access_key = os.getenv('ACCESS_KEY')
        secret_key = os.getenv('SECRET_KEY')
        aws_s3_endpoint = os.getenv('AWS_S3_ENDPOINT')
        nessie_uri = os.getenv('NESSIE_URI')
        aws_access_key = os.getenv('AWS_ACCESS_KEY')
        aws_secret_key = os.getenv('AWS_SECRET_KEY')
        warehouse = os.getenv('WAREHOUSE')






config = Config()    
#print(config.access)
