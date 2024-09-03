import requests
import pandas as pd
import json
import os
import dotenv
import zipfile

#path to .env file must be specified if not in the same directory as the python script
#pulls the environment variables from the .env file
env_var = os.path.abspath('.env')
dotenv.load_dotenv(env_var)

OWN_ACCESS_TOKEN = os.getenv('OWN_ACCESS_TOKEN')
AUTH_URL = 'https://auth.owndata.com/oauth2/aus4c3z3l8FqrbqDU4h7/v1/token'
CLIENT_ID = '0oa4c413eq8wwcEzP4h7'
DOMAIN = {
    'app1' : 'https://app1.owndata.com/api/v1/',
    'useast2' : 'https://useast2.owndata.com/api/v1/',
    'hipaa1' : 'https://hipaa1.owndata.com/api/v1/',
    'hipaa4' : 'https://hipaa4.owndata.com/api/v1/',
    'usgov2' : 'https://usgov2.owndata.com/api/v1/',
    'ca1' : 'https://ca1.owndata.com/api/v1/',
    'ca4' : 'https://ca4.owndata.com/api/v1/',
    'emea1' : 'https://emea1.owndata.com/api/v1/',
    'emea2' : 'https://emea2.owndata.com/api/v1/',
    'emea4' : 'https://emea4.owndata.com/api/v1/',
    'uk1' : 'https://uk1.owndata.com/api/v1/',
    'au1' : 'https://au1.owndata.com/api/v1/',
    'au4' : 'https://au4.owndata.com/api/v1/'
    }
PAYLOAD = {
    'grant_type': 'refresh_token',
    'scope' : 'api:access',
    'refresh_token' : OWN_ACCESS_TOKEN,
    'client_id' : CLIENT_ID
    }

def own_login():
    headers = {'Content-Type': 'application/x-www-form-urlencoded',
               'Accept': 'application/json'}

    response = requests.request("POST", AUTH_URL, headers=headers, data=PAYLOAD)
    res = response.json()
    
    access_token = res.get('access_token')
    print(f'Status Code: {response.status_code}')
    
    return access_token

class own_api:
    def __init__(self):
        self.get_access_token = own_login()

    def get_audit_logs(self, event_id = None, get_all_logs = False):
        '''Get the account event history from the beginning of time (default route)
        or all events that follow a specific event (use parameter with the id of an event),
        results are limited to 1,000 events per request.
        
        Parameters:
            event_id: gets events from the event id provided - type: integer
            get_all_logs: if set to True, all logs will be retrieved - type: boolean (True, False)
        Returns:
            When event_id is not provided, and get_all_logs = False
                CSV and JSON response containing up to the first 1000 events
            When event_id is provided and get_all_logs = False
                CSV and JSON response containing up to 1000 events from the event id
            When event_id is not provided and get_all_logs = True
                CSV and JSON response containing all events from the beginning of time
            When event_id is provided and get_all_logs = True
                CSV and JSON response containing all events after the event id provided
        '''
        
        last_event_list = []
        
        #Gets the first 1000 events
        if event_id == None and get_all_logs == False:
            url = DOMAIN.get('app1') + 'events'
            
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers)

            res = response.json()
            clean_response = json.dumps(res, indent = 2)
            row_count = len(res)
            
            print(f'These are the first 1000 Logs: ')
            df = pd.DataFrame(res)
            res_csv = df.to_csv('audit_log.csv', index=False)

            print(clean_response)
            print('audit_log.csv is now ready')
            return res_csv
        
        #Gets a list of 1000 events starting from the event_Id provided
        elif event_id != None and get_all_logs == False:
            
            url = DOMAIN.get('app1') + f'events?from={event_id}'
            
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers)
            
            res = response.json()
            clean_response = json.dumps(res, indent = 2)
            
            print(f'These are the 1000 events following event id: {event_id}')
            df = pd.DataFrame(res)
            res_csv = df.to_csv('audit_log.csv', index=False)

            print(clean_response)
            print('audit_log.csv is now ready')
            return res_csv
            
        #Gets all logs from the beginning to end
        elif event_id == None and get_all_logs == True:
            
            url = DOMAIN.get('app1') + 'events'
            payload={}
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers, data=payload)
            res = response.json()
            row_count = len(res)
            
        #Gets the last event in the Response
            last_event = res[-1]['event_id']
        
        #Adds the last event to the list to be used if iterating
            last_event_list.append(last_event)
            page = 0
            final_res = res
            for i in last_event_list:
                page+=1
                
                if row_count == 1000:
                    url = DOMAIN.get('app1') + f'events?from={last_event}'
                    
                    headers = {'Authorization': f'Bearer {self.get_access_token}'}
                    response = requests.request("GET", url, headers=headers)
                    
                    res = response.json()
                    row_count = len(res)
                    last_event = res[-1]['event_id']
                    last_event_list.append(last_event)
                    
                    print(f'last event ids: {last_event_list}')
                    print(f'page: {page}')
                    print(f'Series continued from event id: {i}')
                    
                    final_res.extend(res)
                    continue
            
                elif row_count < 1000:
                    print(f'This is the count of logs on the last page: {row_count}\n This is the last event Id: {last_event}')
                    break
                
            df = pd.DataFrame(final_res)
            res_csv = df.to_csv('audit_log.csv', index=False)
        
            print('audit_log.csv is now ready')
            return res_csv

        #Gets all events following the event_id provided
        else: 
            url = DOMAIN.get('app1') + f'events?from={event_id}'
            
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers)
            res = response.json()
            row_count = len(res)

        #Gets the last event in the Response
            last_event = res[-1]['event_id']
            print(f'This is the last event: {last_event}')
            last_event_list.append(last_event)
            
            page = 0
            final_res = res
        #Adds the last event to the list to be used if iterating
            for i in last_event_list:
                page+=1
                
                if row_count == 1000:
                    url = DOMAIN.get('app1') + f'events?from={last_event}'
                    payload={}
                    headers = {'Authorization': f'Bearer {self.get_access_token}'}
                    response = requests.request("GET", url, headers=headers, data=payload)
                    res = response.json()
                    row_count = len(res)
                    last_event = res[-1]['event_id']
                    last_event_list.append(last_event)
                    
                    print(f'last event ids: {last_event_list}')
                    print(f'page: {page}')
                    print(f'Series continued from event id: {i}')
                    
                    final_res.extend(res)
                    continue
            
                elif row_count < 1000:
                    print(f'This is the count of logs on the last page: {row_count}\nThis is the last event Id: {last_event}')
                    break
                
            df = pd.DataFrame(final_res)
            res_csv = df.to_csv('audit_log.csv', index=False)
        
            print('audit_log.csv is now ready')
            return res_csv
    
    def get_backups(self, service_id, backup_id = None):
        '''Get a list of all available backups for a given service or,
        get a specific backup by the Service id and Backup id. 
        Parameter backup_id can be 'last' and will return the last completed backup (with or without errors).\n
        Parameters:
            Required: service_id
            backup_id: returns information for the specific backup (backup_id = 'last' returns the latest backup)
        Returns:
            JSON response with a list of all backups, or specific backup
        '''
        
        if service_id == None:
            raise Exception('No Service ID detected, please enter a service_id to retrieve its backups')
        
        #If no Backup_id is provided, a list of all backups is provided
        if backup_id == None:
            url = DOMAIN.get('app1') + f'services/{service_id}/backups'
            
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers)
            res = response.json()
            clean_response = json.dumps(res, indent=2)
            print(clean_response)
            return clean_response
        
        #If a backup_id is provided information for the specific backup is returned
        #The backup_id can be set to 'last' to get the last backup
        else:
            url = DOMAIN.get('app1') + f'services/{service_id}/backups/{backup_id}'
        
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers)
            res = response.json()
            clean_response = json.dumps(res, indent=2)
            print(clean_response)
            return clean_response
 
    def start_manual_backup(self, service_id):
        '''Start backup a service manually now by Service id.\n
        Parameters:
            Required: service_id
        Returns:
            status code
        '''
        url = DOMAIN.get('app1') + f'services/{service_id}/backup_now'
            
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("POST", url, headers=headers)
        res = response.json()
        
        if response.status_code == 200:
            print('Manual backup initializing')
        else:
            print(f'Something went wrong - Status code: {response.status_code}')
        return res
    
    def list_backup_objects(self, service_id, backup_id, name = None, download_all = False,
                            download_link = False, download_added_link = False, 
                            download_changed_link = False, download_removed_link = False):
        '''Get list of objects in a backup by Service id and Backup id. Parameter backup_id can be 'last'.
        The following fields: download_link, download_added_link, download_removed_link and download_changed_link 
        contain links to the associated data of that backup, stored in CSV format.
        To download these CSVs, issue an HTTPS GET request (using a valid session) to these URLs.
        (Only one download parameter can be set at a time to download the csv or zip)\n
        Parameters:
            Required: service_id, backup_id 
            name: required if downloading an object's csv or zip; name is the object api name in plural form e.g. Accounts/Custom_Object__cs
            download_all: will download all csvs for the given object as a zip file - type: boolean(True, False)
            download_link: downloads the csv of all records - type: boolean(True, False)
            download_added_link: downloads only added records - type: boolean(True, False)
            download_changed_link: downloads only changed records - type: boolean(True, False)
            download_removed_link: downloads only deleted records - type: boolean(True, False)
        Returns:
            JSON response containing a list of objects in a backup
            CSV or ZIP file returned if any of the download parameters are set to true
        '''
        
        if service_id == None or backup_id == None:
            raise Exception('No Service ID/Backup ID detected, please enter a service ID and backup ID as arguments to get the backup info')
        
        url = DOMAIN.get('app1') + f'services/{service_id}/backups/{backup_id}/objects'
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("GET", url, headers=headers)
        res = response.json()
        clean_response = json.dumps(res, indent=2)

        obj_name = None
        object_info = None
        
        try:
            if name != None and download_all == True:
                for i in res:
                    if name and name.lower() == i['name']:
                        obj_name = i['name']
                        object_info = i
                        object_download = {
            'download_link' : object_info['download_link'],
            'download_added_link' : object_info['download_added_link'],
            'download_changed_link' : object_info['download_changed_link'],
            'download_removed_link' : object_info['download_removed_link']
                        }
                        print(object_download) 
                    
                with zipfile.ZipFile("backup_download.zip", "w", compression=zipfile.ZIP_DEFLATED) as download_zip:
                    for key in object_download:
                        print(key)
                        print(object_download[key])
                        get_download = requests.get(object_download[key],headers=headers, stream=True)
                        print(f'Status Code: {get_download}')
                        download_zip.writestr(f'{obj_name}_{key}.csv', get_download.content)
                        print('Adding backup download to zip...')
                        
            elif name != None and download_link == True:
                for i in res:
                    if name and name.lower() == i['name']:
                        obj_name = i['name']
                        object_info = i
                        break
                    
                get_link = requests.get(i['download_link'], headers=headers, stream=True)
                with open(f'{name}.csv', 'wb') as dload:
                    dload.write(get_link.content)
                print(f'{name} csv generated')
                
            elif name != None and download_added_link == True:
                for i in res:
                    if name and name.lower() == i['name']:
                        obj_name = i['name']
                        object_info = i
                        break
                    
                get_link = requests.get(i['download_added_link'], headers=headers, stream=True)
                with open(f'{name}_download_added_link.csv', 'wb') as dload:
                    dload.write(get_link.content)
                print(f'{name}_download_added_link csv generated')
                
            elif name != None and download_changed_link == True:
                for i in res:
                    if name and name.lower() == i['name']:
                        obj_name = i['name']
                        object_info = i
                        break
                    
                get_link = requests.get(i['download_changed_link'], headers=headers, stream=True)
                with open(f'{name}_download_changed_link.csv', 'wb') as dload:
                    dload.write(get_link.content)
                print(f'{name}_download_changed_link csv generated')
                
            elif name != None and download_removed_link == True:
                for i in res:
                    if name and name.lower() == i['name']:
                        obj_name = i['name']
                        object_info = i
                        break
                    
                get_link = requests.get(i['download_removed_link'], headers=headers, stream=True)
                with open(f'{name}_download_removed_link.csv', 'wb') as dload:
                    dload.write(get_link.content)
                print(f'{name}_download_removed_link csv generated') 
        except:
            print(f'Something went wrong... verify the object name and make sure it is plural for example: "object_name__cs')
                    
        return clean_response

    def export_to_file(self, service_id = None, backup_id = None, export_format = 'csv', include_attachments = 'false', 
                       sql_dialect = None, objects = None):
        '''Export a data backup into CSV or SQL by Service id and Backup id. Parameter backup_id can be 'last'\n
        Parameters:
            Required: service_id, backup_id
            export_format: Required export format. (This can only be one of csv,sql)
            include_attachments: Should the export include attachments. e.g true, false
            sql_dialect: SQL dialect to use when exporting to SQL. 
                (This can only be one of mysql,oracle,postgresql,mssql)
            objects: Name of object (in plural) to export. Can be multiple objects in a list []
        Returns:
            JSON response with the job_id of the job that was created
        '''
        
        valid_sql_dialect = {None, 'mysql','oracle','postgresql','mssql'}
        valid_export_format = {'csv', 'sql'}
        if service_id == None or backup_id == None:
            raise Exception('No Service_id/Backup ID detected, please enter a service_id and backup_id')
        if sql_dialect not in valid_sql_dialect:
            raise ValueError(f'results: sql_dialect must be one of {valid_sql_dialect}.')
        if export_format not in valid_export_format:
            raise ValueError(f'results: export_format must be one of {valid_export_format}.' )
        
        payload = {
            'export_format' : export_format,
            'include_attachments' : include_attachments,
            'sql_dialect' : sql_dialect,
            'objects' : objects
        }
        
        url = DOMAIN.get('app1') + f'services/{service_id}/backups/{backup_id}/export'
          
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("POST", url, headers=headers, data=payload)
        res = response.json()
        clean_response = json.dumps(res, indent=2)
        print(clean_response)
        return clean_response
        
    def export_to_endpoint(self, service_id = None, backup_id = None, endpoint_id = None, objects = []):
        '''Export a data backup to Endpoint by Service id and Backup id. Parameter backup_id can be 'last'\n
        Parameters:
            Required: service_id, backup_id, endpoint_id
            endpoint_id: Required endpoint ID. The UUID can be obtained from the Own Account Settings page on the Endpoints tab.
                To get the unique identifier, click Edit beside the endpoint type.
            objects:["<string>","<string>"] Object names to export, if empty all objects 
        Returns:
            JSON response with the job_id of the job that was created
        '''
        if service_id == None or backup_id == None or endpoint_id == None:
            raise Exception('No service_id, backup_id or endpoint_id detected, please enter the service_id, backup_id, and endpoint_id to export to an endpoint')
        
        payload = {
            'endpoint_id' : endpoint_id,
            'objects' : objects
        }
        url = DOMAIN.get('app1') + f'services/{service_id}/backups/{backup_id}/export_to_endpoint'
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("POST", url, headers=headers, data=payload)
        res = response.json()
        clean_response = json.dumps(res, indent=2)
        print(clean_response)
        return clean_response

    def gdpr_rectify(self, service_id = None, record_id = None, table_name = None,
                     field = None, value = None, comment = None):
        '''Submitting a GDPR rectify request\n
        Parameters:
            Required: service_id, record_id, table_name, field, value
            record_id: Required the id of the relevant record
            table_name: Name of the object/table
            field: the field to rectify
            value: the new value
            comment: comments 
        Returns:
            JSON response with the job_id of the gdpr request
        '''
        if service_id == None or record_id == None or table_name == None or field == None or value == None :
            raise Exception('service_id, record_id, table_name, field, value are required')
        
        payload = {
            'record_id' : record_id,
            'table_name' : table_name,
            'field' : field,
            'value' : value,
            'comment' : comment
        }
        url = DOMAIN.get('app1') + f'services/{service_id}/gdpr/rectify'
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("POST", url, headers=headers, data=payload)
        res = response.json()
        clean_response = json.dumps(res, indent=2)
        print(clean_response)
        return clean_response

    def gdpr_forget(self, service_id = None, record_id = None, table_name = None, 
                    comment = None):
        '''Submitting a GDPR forget request\n
        Parameters:
            Required: service_id, record_id, table_name, field, value
            record_id: Required the id of the relevant record
            table_name: Name of the object/table
            comment: comments
            bulk: Set to True to send multiple requests up to rectify multiple records   
        Returns:
            JSON response with the job_id of the gdpr request
        '''
        if service_id == None or record_id == None or table_name == None :
            raise Exception('service_id, record_id, table_name are required')
        
        payload = {
            'record_id' : record_id,
            'table_name' : table_name,
            'comment' : comment
        }
        url = DOMAIN.get('app1') + f'services/{service_id}/gdpr/forget'
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("POST", url, headers=headers, data=payload)
        res = response.json()
        clean_response = json.dumps(res, indent=2)
        print(clean_response)
        return clean_response

    def get_jobs(self):
        '''Get a list of all jobs'''
        
        url = DOMAIN.get('app1') + 'jobs'
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("GET", url, headers=headers)
        res = response.json()
        clean_response = json.dumps(res, indent=2)
        print(clean_response)
        return clean_response

    def get_specific_job(self, job_id):
        '''Get specific job
        Parameters:
            Required: job_id
        '''
        
        if job_id == None:
            raise Exception('job_id is required')
        
        url = DOMAIN.get('app1') + 'jobs'
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("GET", url, headers=headers)
        res = response.json()
        clean_response = json.dumps(res, indent=2)
        print(clean_response)
        return clean_response    

    def get_seeding_templates(self, template_name = None):
        '''Get all seeding templates that the user has access to.\n
        Parameters: 
            template_name: Allows you to search for a template by name and return its information
        Returns:
            JSON response of all templates, or a specified template
        '''
        
        url = DOMAIN.get('app1') + 'seeding/templates'
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("GET", url, headers=headers)
        res = response.json()
        
        if template_name != None:
            for name in res:
                if template_name in name['name']:
                    clean_response = json.dumps(name, indent=2)
                    print(clean_response)
                    return clean_response
        else:
            clean_response = json.dumps(res, indent=2)
            print(clean_response)
            return clean_response
        
    def get_service_ids(self, *service_name):
        '''Get all services that can be used as destination for seeds.\n
        Parameters:
            *service_name: The name of a service 
                (multiple service names separated by commas can be provided to search for multiple ids)  
        Returns:
            JSON Response containing a list of all services.\n
            If a service_name is provided, a dictionary is returned containing each service name provided and its id. 
        '''
        url = DOMAIN.get('app1') + 'services'
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("GET", url, headers=headers)
        res = response.json()
        service_ids = {}
        invalid_services = set()
        if len(service_name) != 0:
            for sn in service_name:
                print(f'service name: {sn}')
                for name in res:
                    if sn in name['displayed_name']:
                        print(json.dumps(name, indent=2))
                        service_ids[name['displayed_name']] = name['id']
                        break
                if sn not in name['displayed_name']:
                    print(f'{sn} not found')
                    invalid_services.add(sn)
                    continue  
            print(service_ids) 
            print(f'{invalid_services} - backup services not found' ) 
            return service_ids     
        else:
            clean_response = json.dumps(res, indent=2)
            print(clean_response)
            return clean_response
    
    def get_specific_service_id(self, service_id):
        '''Get a specific service by Service id.\n
        Parameters:
            *service_id: The service id
        Returns:
            JSON Response of specific service information.\n
        '''
        url = DOMAIN.get('app1') + f'services/{service_id}'
        
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("GET", url, headers=headers)
        res = response.json()
        clean_response = json.dumps(res, indent=2)
        print(clean_response)
        return clean_response
        
    def start_seed_job(self, template_id, destination_id,
                    seeding_method = 'incremental', disable_automations = False,
                    reindex = None, disable_validation_rules = None, backup_id = None, service_id = None):
        '''Start a new seed from the template's source service to the specified destination service.\n
        Parameters:
            Required: template_id, destination_id, disable_automations 
            seeding_method: Allowed argument values (incremental, upsert, or clean_and_insert)
            backup_id: The backup id you would like to seed from
            service_id: The backup service you would like to seed from
        Returns:
            JSON response containing information on the seed job that was created
        '''
        valid_seeding_methods = {None, 'incremental', 'upsert', 'clean_and_insert'}
        if template_id == None or destination_id == None or seeding_method == None or disable_automations == None:
            raise Exception(f'Required arguments missing: template_id, destination_id, seeding_method, disable_automations')
        if seeding_method not in valid_seeding_methods:
            raise ValueError(f"results: seeding_method must be one of {valid_seeding_methods}.")
        
        url = DOMAIN.get('app1') + f'seeding/templates/{template_id}/seed'
        payload = {
            'destination' : destination_id,
            'seeding_method' : seeding_method,
            'disable_automations' : disable_automations,
            'disable_validation_rules' : disable_validation_rules,
            'reindex' : reindex,
            'backup_id' : backup_id,
            'service_id' : service_id
        } 
        
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("POST", url, headers=headers, data=payload)
        response_json = response.json()
        clean_response = json.dumps(response_json, indent = 2)
        print(clean_response)
        return clean_response
        
    def get_seed_logs(self, seed_id):
        '''Get information for the specified seed.\n
        Parameters: 
            Required: seed_id 
            seed_id: id of the seed to retrieve logs from.
        Returns:
            JSON response of the seed logs
        '''
        
        url = DOMAIN.get('app1') + f'seeding/seed/{seed_id}'
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("GET", url, headers=headers)
        res = response.json()
        clean_response = json.dumps(res, indent = 2)
        print(clean_response)
        return clean_response
    
    def start_anonymize_job(self, template_id, destination_service_id,
                    disable_automations = False, backup_id = None,
                    automations_to_disable = {}):
        '''Run anonymization template on a specific sandbox.
        Parameters:
            Required: template_id, destination_service_id, disable_automations
            template_id: template used to anonymize the data
            destination_service_id: service id to be anonymized - type: integer
            disable_automations: disables automations in the environment - values are True or False - type: string
            automations_to_disable: (optional) Can select which automations to disable e.g. "ApexTrigger": ["AccountApexTrigger", "TaskApexTrigger"] - type: dictionary
            backup_id: (optional) used when fetching from backup - type: string
        Returns:
            JSON response containing information the job id of the anonymize job created
        '''
        
        url = DOMAIN.get('app1') + f'seeding/templates/{template_id}/anonymize'
        payload = {
            'destination_service_id' : destination_service_id,
            'backup_id' : backup_id,
            'disable_automations' : disable_automations,
            'automations_to_disable' : automations_to_disable
        } 
        print(payload)
        
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("POST", url, headers=headers, data=payload)
        res = response.json()
        job_id = res['job_id']
        
        clean_response = json.dumps(res, indent = 2)
        print(clean_response)
        print(f'job id: {job_id}')
        return job_id
    
    def get_anonymize_logs(self, anonymize_job_id):
        '''Get information for the specified anonymization job.
        Parameters:
            Required: anonymize_id
            anonymize_id: the job id of the anonymize job
        Returns:
            JSON response containing information on the anonymize job that was created
        '''
        
        url = DOMAIN.get('app1') + f'seeding/anonymize/{anonymize_job_id}'
        
        headers = {'Authorization': f'Bearer {self.get_access_token}'}
        response = requests.request("GET", url, headers=headers)
        res = response.json()        
        clean_response = json.dumps(res, indent = 2)
        print(clean_response)
        
        return clean_response
