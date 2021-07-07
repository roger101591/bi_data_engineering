from dotenv import load_dotenv, find_dotenv
from ga_uploader.GA360 import *

# create dictionary for all query files and customDataSourceId
reports = {
		   1:['pdp_info',''],
		   2:['ecom_product_data',''],
		   3:['attribute_plp_data',''],
		   4:['brand_info_pageviews','']
		  }
# Get DB credentials from environmental variables
load_dotenv(find_dotenv())
host = os.getenv("DB4_RPT_HOST")
user = os.getenv("DB4_RPT_USER")
passw = os.getenv("DB4_RPT_PASS")
schema = os.getenv("DB4_RPT_SCHEMA")
creds = (host, user, passw, schema)

api_name = 'analytics'
api_version = 'v3'
#uploader_scopes = ['https://www.googleapis.com/auth/analytics.edit']

def main():
	service = GA360.build_service(ga_account_info,
								  api_name,
								  api_version)
	for report in reports:
		GA360.delete_uploaded_files(reports[report][0], service, reports[report][1])
		GA360.upload_files(reports[report][0], creds, service, reports[report][1])

	# Delete files in archive older than 5 days
	GA360.clear_upload_archive(5, 'sitefiles/')

if __name__ == "__main__":
	main()