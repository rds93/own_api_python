import requests
import pandas as pd
import json

#This should be an ENV Variable
OWN_ACCESS_TOKEN = 'token'
AUTH_URL = 'https://auth.owndata.com/oauth2/aus4c3z3l8FqrbqDU4h7/v1/token'
CLIENT_ID = '0oa4c413eq8wwcEzP4h7'

DOMAIN = {'app1' : 'https://app1.ownbackup.com/api/v1/', 'useast2' : 'https://useast2.ownbackup.com/api/v1/'}
payload = 'grant_type=' + 'refresh_token' + '&scope=' + 'api:access' + '&refresh_token=' + OWN_ACCESS_TOKEN + '&client_id=' + CLIENT_ID 

def own_login():
    headers = {'Content-Type': 'application/x-www-form-urlencoded',
               'Accept': 'application/json'}

    response = requests.request("POST", AUTH_URL, headers=headers, data=payload)
    res = response.json()
    
    access_token = res.get('access_token')
    print(f'Status Code: {response.status_code}')
    #print(response.text)
    return access_token

get_access_token = own_login()

def get_audit_logs(event_id = None, get_all_logs = False):
    last_event_list = []
    
    #Gets the first 1000 events
    if event_id == None and get_all_logs == False:
        url = f"https://app1.ownbackup.com/api/v1/events"
        
        payload={}
        headers = {'Authorization': f'Bearer {get_access_token}'}
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
        headers = {'Authorization': f'Bearer {get_access_token}'}
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
        headers = {'Authorization': f'Bearer {get_access_token}'}
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
                headers = {'Authorization': f'Bearer {get_access_token}'}
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
        headers = {'Authorization': f'Bearer {get_access_token}'}
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
                headers = {'Authorization': f'Bearer {get_access_token}'}
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

print(get_audit_logs(get_all_logs = True))
    
#Optionally enter the exact source and destination service name to search for the Id
def get_backup_services(source_service_name = None , destination_service_name = None):
    url = DOMAIN.get('app1') + 'services'
    
    if source_service_name == None and destination_service_name == None:
        
        headers = {'Authorization': f'Bearer {get_access_token}'}
        response = requests.request("GET", url, headers=headers)
        response_json = response.json()
        clean_response = json.dumps(response_json, indent = 2)
        
        return clean_response
    
    else: 
        service_ids = {'source_service_id' : source_service_id, 
                   'destination_service_id' : destination_service_id}
        
        for service in response.json():
            if service["status"] != "ARCHIVED":
                
                if source_service_name in service["displayed_name"]:
                    source_service_id = service["id"]
                    
                if destination_service_name in service["displayed_name"]:
                    destination_service_id = service["id"]
                
        service_ids.update({'source_service_id' : source_service_id,
                            'destination_service_id' : destination_service_id})  
       
        return service_ids

#The service Id is mandatory, To grab the latest backup set 'latest_backup' = True
def get_service_backups(service_id, backup_id = None, latest_backup = False):
    
    if service_id == None:
        raise Exception('No Service ID detected, please enter a service ID as an argument to retrieve its backups')
    
    #If no Backup_id is provided, a list of backups is provided
    if backup_id == None and latest_backup == False:
        url = f'https://app1.ownbackup.com/api/v1/services/{service_id}/backups'
        
        headers = {'Authorization': f'Bearer {get_access_token}'}
        response = requests.request("GET", url, headers=headers, data=payload)
        res = response.json()
        clean_response = json.dumps(res, indent=2)
        return clean_response
    
    elif backup_id == None and latest_backup == True:
        url = f'https://app1.ownbackup.com/api/v1/services/{service_id}/backups'
        
        headers = {'Authorization': f'Bearer {get_access_token}'}
        response = requests.request("GET", url, headers=headers, data=payload)
        res = response.json()
        latest_backup_id = res[-1]['id']
        return latest_backup_id
    
    #If a backup id is provided information for the specific backup is returned
    else:
        url = f'https://app1.ownbackup.com/api/v1/services/{service_id}/backups/{backup_id}'
    
        headers = {'Authorization': f'Bearer {get_access_token}'}
        response = requests.request("GET", url, headers=headers, data=payload)
        res = response.json()
        clean_response = json.dumps(res, indent=2)
        
        return clean_response
    

# Template_id, destination_id, seeding_method, and disable_automations are required arguments
def start_seed_job(template_id, destination_id,
                   seeding_method = 'incremental', disable_automations = False,
                   reindex = None, disable_validation_rules = None, backup_id = None, service_id = None):
    
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
    headers = {'Authorization': f'Bearer {get_access_token}'}

    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    response_json = response.json()
    clean_response = json.dumps(response_json, indent = 2)
    #print(clean_response)
    return response.text
    
#seed = start_seed_job(20552, get_services.get('destination_service_id'))

#print(seed)
