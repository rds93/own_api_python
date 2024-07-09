import requests
import pandas as pd
import json

#This should be an ENV Variable
OWN_ACCESS_TOKEN = 'tdD2UmwetfzcsLAZvokA2qzW_BzXJNVHmHVFE1Xk0y0'
AUTH_URL = 'https://auth.owndata.com/oauth2/aus4c3z3l8FqrbqDU4h7/v1/token'
CLIENT_ID = '0oa4c413eq8wwcEzP4h7'
payload = 'grant_type=' + 'refresh_token' + '&scope=' + 'api:access' + '&refresh_token=' + OWN_ACCESS_TOKEN + '&client_id=' + CLIENT_ID 

DOMAIN = {'app1' : 'https://app1.ownbackup.com/api/v1/', 'useast2' : 'https://useast2.ownbackup.com/api/v1/'}
def own_login():
    #url = 'https://auth.ownbackup.com/oauth2/aus4c3z3l8FqrbqDU4h7/v1/token'
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded',
               'Accept': 'application/json'}

    response = requests.request("POST", AUTH_URL, headers=headers, data=payload)
    res = response.json()
    
    access_token = res.get('access_token')
    print(f'Status Code: {response.status_code}')
    #print(response.text)
    return access_token

get_access_token = own_login()

def get_audit_logs(event_id = None):
    last_event_list = []
    if event_id == None:
        url = f"https://app1.ownbackup.com/api/v1/events"
        #url = f'{STG6_REQUESTS_URL}/api/v1/events'
        #url = f'{STG11_REQUESTS_URL}/api/v1/events'
        payload={}
    
        headers = {'Authorization': f'Bearer {get_access_token}'}
        response = requests.request("GET", url, headers=headers, data=payload)
  
        res = response.json()
        row_count = len(res)
   
    #Gets the last event in the Response
        last_event = res[-1]['event_id']
    
    #Adds the last event to the list to be used if iterating
        last_event_list.append(last_event)
        print(f'list of last events: {last_event_list}')
        
        if row_count < 1000:
            print(f'This is the row count: {row_count}')
            return response.text
        else: 
            print(f'first 1000 events: {last_event_list}')
            return res, last_event_list
    else: 
        url = f"https://app1.ownbackup.com/api/v1/events?from={event_id}"
        #url = f'{STG6_REQUESTS_URL}/api/v1/events?from={event_id}'
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
        if row_count == 1000:
            print(f'events {last_event_list}') 
            return res, last_event_list
            
        else: 
            print(f'Number of rows: {row_count}')
            return res

##-------------- TO DO FIX Duplicates are created in final res ---------- 

def paginate_audit_logs():
    #gets the response from the first audit call
    (final_res, event_list) = get_audit_logs()  
   
    #event_list = audit_event
    #print(final_res)
    page = 0
    for i in event_list:
        page+=1
            
        print(f'last event ids: {event_list}')
        print(f'page: {page}')
        
        print(f'Series continued from event id: {i}')
            
        if len(get_audit_logs(i)) == 2:    
            (continue_audit_res, last_event_id) = get_audit_logs(i) 
              
        #   should be the last event id in the log
            #print(f'audit continued: {continue_audit_res}')
            #print(last_event_id)
            
            final_res.extend(continue_audit_res)
            event_list.append(last_event_id[-1])
            continue
        
        elif len(get_audit_logs(i)) != 2:
            break
            
            #if len(get_audit_logs(i)) != 2:
                #break
            #else:
                #final_res.extend(continue_audit_res)
              #  continue  
              
    #print(final_res)
    df = pd.DataFrame(final_res)
    res_csv = df.to_csv('audit_log.csv', index=False)
    return res_csv
    
go = paginate_audit_logs() 
#test = get_audit_logs()

print(go)

#To grab the latest backup set 'latest_backup' = True
def get_service_backups(service_id, backup_id = None, latest_backup = False):
    #If no Backup_id is provided, a list of backups is provided
    if backup_id == None and latest_backup == False:
        url = f'https://app1.ownbackup.com/api/v1/services/{service_id}/backups'
        
        headers = {'Authorization': f'Bearer {get_access_token}'}
        response = requests.request("GET", url, headers=headers, data=payload)
        res = response.json()
        clean_response = json.dumps(res, indent=2)
        return clean_response
    
    #If a backup id is provided information for the specific backup is returned.
    elif backup_id == None and latest_backup == True:
        url = f'https://app1.ownbackup.com/api/v1/services/{service_id}/backups'
        
        headers = {'Authorization': f'Bearer {get_access_token}'}
        response = requests.request("GET", url, headers=headers, data=payload)
        res = response.json()
        clean_response = json.dumps(res, indent=2)
        latest_backup_id = res[-1]['id']
        return latest_backup_id
    else:
        url = f'https://app1.ownbackup.com/api/v1/services/{service_id}/backups/{backup_id}'
    
        headers = {'Authorization': f'Bearer {get_access_token}'}
        response = requests.request("GET", url, headers=headers, data=payload)
        res = response.json()
        clean_response = json.dumps(res, indent=2)
        
        return clean_response
#backup = own_backups('25598')

#print(backup)

#Enter the exact source and destination service name to search for the Id

def get_all_services(source_service_name , destination_service_name):
    url = DOMAIN.get('app1') + 'services'
    
    destination_service_id = None
    source_service_id = None
    service_ids = {'source_service_id' : source_service_id, 
                   'destination_service_id' : destination_service_id}
    
    headers = {'Authorization': f'Bearer {get_access_token}'}
    response = requests.request("GET", url, headers=headers)
    response_json = response.json()
    clean_response = json.dumps(response_json, indent = 2)
    
    for service in response.json():
        if service["status"] != "ARCHIVED":
            if source_service_name in service["displayed_name"]:
                source_service_id = service["id"]
            if destination_service_name in service["displayed_name"]:
                destination_service_id = service["id"]
                
    service_ids.update({'source_service_id' : source_service_id,
                        'destination_service_id' : destination_service_id})
                
    #print(clean_response)
    return service_ids

get_services = get_all_services('Prod Data', 'Test Expired Auth Token' )

print(f'Source and Destination Service Ids: {get_services}')

gsb = get_service_backups(get_services.get('source_service_id'), latest_backup = True)

print(gsb)

#8958 20552
# Template_id, destination_id, seeding_method, and disable_automations are required arguments

def start_seed_job(template_id, destination_id,
                   seeding_method = 'incremental', disable_automations = False,
                   reindex = None, disable_validation_rules = None, backup_id = None, service_id = None):
    
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
    return clean_response
    
seed = start_seed_job(20552, get_services.get('destination_service_id'))

print(seed)
