import sys
from googleapiclient import discovery
import httplib2
from oauth2client.client import GoogleCredentials

DISCOVERY_URL = ('https://{api}.googleapis.com/'
                '$discovery/rest?version={apiVersion}')

def scoreText( text ):

 http = httplib2.Http()

 content = text
 print content

 credentials = GoogleCredentials.get_application_default().create_scoped(
     ['https://www.googleapis.com/auth/cloud-platform'])

 http=httplib2.Http()
 credentials.authorize(http)

 service = discovery.build('language', 'v1beta1',
                           http=http, discoveryServiceUrl=DISCOVERY_URL)

 service_request = service.documents().analyzeSentiment(
   body={
     'document': {
        'type': 'PLAIN_TEXT',
	'content': content
     }
   })

 response = service_request.execute()

 polarity = response['documentSentiment']['polarity']
 magnitude = response['documentSentiment']['magnitude']

 return polarity

if __name__ == '__main__':
 main()

