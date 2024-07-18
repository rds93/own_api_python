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
AUTH_URL = os.getenv('AUTH_URL')
CLIENT_ID = os.getenv('CLIENT_ID')
#worth using if there are multiple regions on an Own Account
DOMAIN = {'app1' : 'https://app1.ownbackup.com/api/v1/', 'useast2' : 'https://useast2.ownbackup.com/api/v1/'}

#payload = 'grant_type=' + 'refresh_token' + '&scope=' + 'api:access' + '&refresh_token=' + OWN_ACCESS_TOKEN + '&client_id=' + CLIENT_ID 
PAYLOAD = {'grant_type': 'refresh_token',
           'scope' : 'api:access',
           'refresh_token' : OWN_ACCESS_TOKEN,
           'client_id' : CLIENT_ID}

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

    #Auditing
    def get_audit_logs(self, event_id = None, get_all_logs = False):
        last_event_list = []
        
        #Gets the first 1000 events
        if event_id == None and get_all_logs == False:
            url = f"https://app1.ownbackup.com/api/v1/events"
            
            payload={}
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers, data=payload)

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
            
            url = f"https://app1.ownbackup.com/api/v1/events?from={event_id}"
            
            payload={}
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers, data=payload)
            
            res = response.json()
            clean_response = json.dumps(res, indent = 2)
            
            print(f'These are the 1000 Logs following event id: {event_id}')
            df = pd.DataFrame(res)
            res_csv = df.to_csv('audit_log.csv', index=False)

            print(clean_response)
            print('audit_log.csv is now ready')
            return res_csv
            
        #Gets all logs from the beginning to end
        elif event_id == None and get_all_logs == True:
            
            url = f"https://app1.ownbackup.com/api/v1/events"
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
                    url = f"https://app1.ownbackup.com/api/v1/events?from={last_event}"
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
                    print(f'This is the count of logs on the last page: {row_count}\n This is the last event Id: {last_event}')
                    break
                
            df = pd.DataFrame(final_res)
            res_csv = df.to_csv('audit_log.csv', index=False)
        
            print('audit_log.csv is now ready')
            return res_csv

        #Gets all events following the event_id provided
        else: 
            url = f"https://app1.ownbackup.com/api/v1/events?from={event_id}"
            
            payload={}
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers, data=payload)
            
            res = response.json()
            row_count = len(res)

        #Gets the last event in the Response
            
            last_event = res[-1]['event_id']
            print(f'This is the last event: {last_event}')
            
            last_event_list.append(last_event)
            
        #Adds the last event to the list to be used if iterating
            for i in last_event_list:
                page+=1
                
                if row_count == 1000:
                    url = f"https://app1.ownbackup.com/api/v1/events?from={last_event}"
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
                    print(f'This is the count of logs on the last page: {row_count}\n This is the last event Id: {last_event}')
                    break
                
            df = pd.DataFrame(final_res)
            res_csv = df.to_csv('audit_log.csv', index=False)
        
            print('audit_log.csv is now ready')
            return res_csv

    #print(get_audit_logs(get_all_logs = True))
        
    #The service Id is mandatory, To grab the latest backup set 'latest_backup' = True
    def get_service_backups(self, service_id, backup_id = None, latest_backup = False):
        
        if service_id == None:
            raise Exception('No Service ID detected, please enter a service ID as an argument to retrieve its backups')
        
        #If no Backup_id is provided, a list of backups is provided
        if backup_id == None and latest_backup == False:
            url = f'https://app1.ownbackup.com/api/v1/services/{service_id}/backups'
            
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers)
            res = response.json()
            clean_response = json.dumps(res, indent=2)
            return clean_response
        
        elif backup_id == None and latest_backup == True:
            url = f'https://app1.ownbackup.com/api/v1/services/{service_id}/backups'
            
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers)
            res = response.json()
            latest_backup_id = res[-1]['id']
            return latest_backup_id
        
        #If a backup id is provided information for the specific backup is returned
        else:
            url = f'https://app1.ownbackup.com/api/v1/services/{service_id}/backups/{backup_id}'
        
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers)
            res = response.json()
            clean_response = json.dumps(res, indent=2)
            
            return clean_response
        
    #get_service = get_service_backups(25598, 27155502)
    #print(get_service)

    # Service_id and Backup_id are required
    # name must be the object name in plural
    def list_backup_objects(self, service_id, backup_id, name = None, download_all = False,
                            download_link = False, download_added_link = False, 
                            download_changed_link = False, download_removed_link = False):
        ''' Required arguments: service_id, backup_id, name 
        name: The object api name in plural form e.g. Accounts/Custom_Object__cs
        download_all: will download all csvs for the given object
        download_link: downloads the csv of all records
        download_added_link: downloads only added records
        download_changed_link: downloads only changed records
        download_removed_link: downloads only deleted records
        list_backup_objects: gets object information from the backups and,
        provides the links to download the object from the backup.
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
                    
                get_link = requests.get(i['download_link'], headers=headers, stream=True)
                with open(f'{name}_download_added_link.csv', 'wb') as dload:
                    dload.write(get_link.content)
                print(f'{name}_download_added_link csv generated')
                
            elif name != None and download_changed_link == True:
                for i in res:
                    if name and name.lower() == i['name']:
                        obj_name = i['name']
                        object_info = i
                        break
                    
                get_link = requests.get(i['download_link'], headers=headers, stream=True)
                with open(f'{name}_download_changed_link.csv', 'wb') as dload:
                    dload.write(get_link.content)
                print(f'{name}_download_changed_link csv generated')
                
            elif name != None and download_removed_link == True:
                for i in res:
                    if name and name.lower() == i['name']:
                        obj_name = i['name']
                        object_info = i
                        break
                    
                get_link = requests.get(i['download_link'], headers=headers, stream=True)
                with open(f'{name}_download_removed_link.csv', 'wb') as dload:
                    dload.write(get_link.content)
                print(f'{name}_download_removed_link csv generated') 
        except:
            print(f'Something went wrong... verify the object name and make sure it is plural for example: "object_name__cs')
                    
        return clean_response

    def get_service_ids_to_seed(self, source_service_name = None , destination_service_name = None):
        '''source_service_name and destination_service_name are optional arguments to get the service ids'''
        url = DOMAIN.get('app1') + 'services'
        if source_service_name == None and destination_service_name == None:
            
            headers = {'Authorization': f'Bearer {self.get_access_token}'}
            response = requests.request("GET", url, headers=headers)
            response_json = response.json()
            clean_response = json.dumps(response_json, indent = 2)
            
            return clean_response
        
        else: 
            service_ids = {'source_service_id' : source_service_id, 
                    'destination_service_id' : destination_service_id}
            
            for service in response.json():
                if source_service_name in service["displayed_name"]:
                    source_service_id = service["id"]
                        
                if destination_service_name in service["displayed_name"]:
                    destination_service_id = service["id"]
                    
            service_ids.update({'source_service_id' : source_service_id,
                                'destination_service_id' : destination_service_id})  
        
            return service_ids


    # Template_id, destination_id, seeding_method, and disable_automations are required arguments
    def start_seed_job(self, template_id, destination_id,
                    seeding_method = 'incremental', disable_automations = False,
                    reindex = None, disable_validation_rules = None, backup_id = None, service_id = None):
        '''template_id, destination_id, and disable_automations are required arguments'''
        
        if template_id == None or destination_id == None or seeding_method == None or disable_automations == None:
            raise Exception(f'Required arguments missing: template_id, destination_id, seeding_method, disable_automations')
        
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
        
        files=[]    
        headers = {'Authorization': f'Bearer {self.get_access_token}'}

        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        response_json = response.json()
        clean_response = json.dumps(response_json, indent = 2)
        #print(clean_response)
        return response.text
        
    #seed = start_seed_job(20552, get_service_ids_to_seed.get('destination_service_id'))

    #print(seed)