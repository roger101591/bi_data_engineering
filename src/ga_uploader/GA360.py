from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import time
from tools.tools import *
from dotenv import load_dotenv, find_dotenv
import json

reporting_scopes = ['https://www.googleapis.com/auth/analytics.readonly']
load_dotenv(find_dotenv())
ga = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
ga_account_info = json.loads(ga,strict=False)


class GA360:
	# vars
	accountId = ''
	webPropertyId = ''
	# Build the service object.
	def build_service(ga_account_info,
					  api_name,
					  api_version):
		credentials = Credentials.from_service_account_info(
			ga_account_info,
		)
		service = build(api_name, api_version, credentials=credentials)
		return service

	def delete_uploaded_files(queryname, service, customDataSourceId):
		# list files in customDataSourceId
		upload_list = []
		try:
			list_uploads = service.management().uploads().list(
				accountId=GA360.accountId,
				webPropertyId=GA360.webPropertyId,
				customDataSourceId=customDataSourceId,
			).execute()

				  # Append file IDs to list
			for upload in list_uploads.get('items', []):
				upload_list.append(upload.get('id'))

		except TypeError:
			# Handle errors in constructing a query.
			print('There was an error in constructing your list query : %s' % TypeError)

		except HttpError:

			# Handle API errors.
			print('There was a list API call error : %s : %s' %
				  (HttpError.resp.status, HttpError.resp.reason))


	  # Delete files in list

		try:
			delete_uploads = service.management().uploads().deleteUploadData(
				accountId=GA360.accountId,
				webPropertyId=GA360.webPropertyId,
				customDataSourceId=customDataSourceId,
				body={
					'customDataImportUids': upload_list
				    }
			    ).execute()

			print('GA Report ' + queryname + ' files deleted.')

		except TypeError:
			# Handle errors in constructing a query.
			print('There was an error in constructing your delete query : %s' % TypeError)

		except HttpError:
			# Handle API errors.
			print('There was an delete API call error : %s : %s' %
				  (HttpError.resp.status, HttpError.resp.reason))

		time.sleep(1)

	def upload_files(queryname, creds, service, customDataSourceId):
		query = open(queryname + '.sql', 'r').read().rstrip()
		result = get_mysql(creds, query)
		now = time.strftime('%Y-%m-%d_%H-%M-%S')
		result.to_csv('sitefiles/' + now + '_' + queryname + '.csv', index=False)
		csv = 'sitefiles/' + now + '_' + queryname + '.csv'

		try:
			media = MediaFileUpload(csv,
									mimetype='application/octet-stream',
									resumable=False,
									)
			daily_upload = service.management().uploads().uploadData(
				accountId=GA360.accountId,
				webPropertyId=GA360.webPropertyId,
				customDataSourceId=customDataSourceId,
				media_body=media
			).execute()

			print(queryname + ' file upload complete.')

		except TypeError:
			# Handle errors in constructing a query.
			print('There was an error in constructing your upload query : %s' % TypeError)

		except HttpError:
			# Handle API errors.
			print('There was an upload API call error : %s : %s' %
				  (HttpError.resp.status, HttpError.resp.reason))

		time.sleep(1)

	def clear_upload_archive(number_of_days, path):

		time_in_secs = time.time() - (number_of_days * 24 * 60 * 60)
		for file in os.listdir(path):
			if os.stat(os.path.join(path, file)).st_mtime < time_in_secs:
				os.remove(os.path.join(path, file))

		time_in_secs = time.time() - (number_of_days * 24 * 60 * 60)
		for file in os.listdir(path):
			if os.stat(os.path.join(path, file)).st_mtime < time_in_secs:
				print(os.stat(os.path.join(path, file)))
				os.remove(os.path.join(path, file))

	def get_report(analytics, viewId, startdate, enddate, metrics_list, dimensions_list,segmentVal=None,filter_dict=None):
		return analytics.reports().batchGet(
			body={
				'reportRequests': [
					{
						'viewId': viewId,
						'dateRanges': [
							{'startDate': startdate, 'endDate': enddate}
						],
						'metrics': [
							metrics_list
						],
						'dimensions': [
							dimensions_list
						],
						"dimensionFilterClauses": [
							{
								"filters": [
									filter_dict
								]
							}
						],
						'segments': [
							segmentVal
						]
					,
					}
				]
			}
		).execute()

	def load_to_dataframe(response):
		list = []
		# get report data
		for report in response.get('reports', []):
			# set column headers
			columnHeader = report.get('columnHeader', {})
			dimensionHeaders = columnHeader.get('dimensions', [])
			metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
			rows = report.get('data', {}).get('rows', [])

		for row in rows:
			# create dict for each row
			dict = {}
			dimensions = row.get('dimensions', [])
			dateRangeValues = row.get('metrics', [])

			# fill dict with dimension header (key) and dimension value (value)
			for header, dimension in zip(dimensionHeaders, dimensions):
				dict[header] = dimension

			# fill dict with metric header (key) and metric value (value)
			for i, values in enumerate(dateRangeValues):
				for metric, value in zip(metricHeaders, values.get('values')):
					# set int as int, float as float
					if ',' in value or '.' in value:
						dict[metric.get('name')] = float(value)
					else:
						dict[metric.get('name')] = int(value)
			list.append(dict)
		df = pd.DataFrame(list)
		return df
