import requests
import json
import pandas as pd
from dateutil import parser
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
import os


# Cookie to the sandbox
sandbox_key = os.getenv("FLUKE_KEY")

headers = {
    "Content-Type": "application/json", 
    "Cookie": sandbox_key
}

# Environment variables from GitHub
key = os.getenv("MOTIVE_KEY")

motive_headers = {
    "accept": "application/json", 
    "X-Api-Key": key
}



def filter_issues(inspection_data: list) -> list:
  """
  Give raw inspection data from Motive, returns a list of inspections that
  only contain issues with the truck. 

  Args:
    inspection_data (list): Raw inspection data from Motive API

  Returns:
    list: List of inspections that only contain issues with the truck
  """
  important_issues = []

  for report in inspection_data["inspection_reports"]:
    inspection = report.get('inspection_report', {})
    id = inspection.get('id')
    time = inspection.get('time')
    location = inspection.get('location')

    truck_issues = {
      'id': id,
      'date': time,
      'location': location,
      'vehicle': inspection.get('vehicle'),
      'asset': inspection.get('asset'),
      'driver': inspection.get('driver'),
      'inspection_type': "Post Trip" if inspection.get('inspection_type') == "post_trip" else "Pre Trip",
      'odometer': inspection.get('odometer'),
      'issues': []
    }

    # Check for issues in inspected parts; one truck can have more than one issue (Reason for truck_issues variable)
    for part in inspection.get('inspected_parts', []):
      if part.get('type') == 'major' or part.get('type') == 'minor': # or part.get('type') == 'minor':
        
        # Add all documentation necessary to address the issue
        issue = {
          'inspected_item': part.get('id'),
          'category': part.get('category'),
          'notes': part.get('notes'),
          'priority': part.get('type'),
        }
        truck_issues['issues'].append(issue)  

    # If there are any issues on this inspection report add it to the list
    if truck_issues['issues']:
      important_issues.append(truck_issues)

  return important_issues

def new_data(inspection_data: list) -> list:

    # Look into work order requests last push as well ###################################

    """
    Filters out the data that has already been seen and returns the new data
    
    Args:
        inspection_data (list): List of inspection reports that have been filtered for issues

    Returns:
        list: List of inspection reports that are new since last run
    """

    # Find the latest issue about the truck uploaded to fluke
    url = 'https://torcroboticssb.us.accelix.com/api/entities/def/WorkOrders/search-paged'

    # Cookie to the sandbox
    data = {'select': [{'name': 'site'}, {'name': 'createdBy'}, {'name': 'updatedBy'}, {'name': 'updatedSyncDate'}, {'name': 'dataSource'}, {'name': 'status'}, {'name': 'closedOn'}, {'name': 'openedOn'}, {'name': 'startDate'}, {'name': 'assetId'}, {'name': 'requestId'}, {'name': 'scheduledEventId'}, {'name': 'description'}, {'name': 'details'}, {'name': 'taskId'}, {'name': 'priorityCode'}, {'name': 'rimeRanking'}, {'name': 'signature'}, {'name': 'image'}, {'name': 'geolocation'}, {'name': 'reason'}, {'name': 'parentId'}, {'name': 'c_workordertype'}, {'name': 'c_jobstatus'}, {'name': 'c_priority'}, {'name': 'c_completedon'}, {'name': 'c_projectid'}, {'name': 'c_problemtype'}, {'name': 'c_estimatedhours'}, {'name': 'c_downtime'}, {'name': 'c_comments'}, {'name': 'c_completeddate'}, {'name': 'c_failurecode'}, {'name': 'c_issuecode'}, {'name': 'c_department'}, {'name': 'c_linenumber'}, {'name': 'c_availablestatus'}, {'name': 'c_completedby'}, {'name': 'c_closedby'}, {'name': 'c_requestedon'}, {'name': 'c_requesteremail'}, {'name': 'c_site'}, {'name': 'c_building'}, {'name': 'c_imagefield'}, {'name': 'c_floorlevel'}, {'name': 'c_requesterphone'}, {'name': 'c_compid'}, {'name': 'c_onholdreason'}, {'name': 'c_assetmountingposition'}, {'name': 'c_assettypesymptom'}, {'name': 'c_symptom'}, {'name': 'c_foreignkeylookupsymptom'}, {'name': 'c_terminalzone'}, {'name': 'c_downtimestart'}, {'name': 'c_downtimeend'}, {'name': 'c_repairtimehrs'}, {'name': 'c_repairtimestart'}, {'name': 'c_repairtimeend'}, {'name': 'c_location'}, {'name': 'c_documentlink'}, {'name': 'c_symptomassettypediagnosis'}, {'name': 'c_locationdiagnosis'}, {'name': 'c_diagnosis'}, {'name': 'c_documentlinkdiagnosis'}, {'name': 'c_parentasset'}, {'name': 'c_maintenancelog'}, {'name': 'c_parentassetdescription'}, {'name': 'c_firmware'}, {'name': 'c_deploymentsoftware'}, {'name': 'c_assettypeasset'}, {'name': 'c_tasknumber'}, {'name': 'id'}, {'name': 'number'}, {'name': 'createdOn'}, {'name': 'updatedOn'}], 'filter': {'and': [{'name': 'isDeleted', 'op': 'isfalse'}]}, 'order': [{'name': 'number', 'desc': True}], 'pageSize': 20, 'page': 0, 'fkExpansion': True}

    # Check the data from work orders for latest
    index = 1
    latestBaseTruckWO = None
    while(latestBaseTruckWO == None):

        response = requests.post(url, headers=headers, data=json.dumps(data))
        assert response.status_code == 200
        response = response.json()
        dx = response['data']

        data['page'] = index
        response = requests.post(url, headers=headers, data=json.dumps(data))
        assert response.status_code == 200
        dx.extend(response.json()['data'])

        # dataframe
        df = pd.DataFrame(data={cx: [x[cx] for x in dx] for cx in sorted(dx[0].keys())})

        # get most recent base truck error
        for i in range(df.shape[0]):
            if(df.get("c_priority")[i] != None and df.get("c_priority")[i].get("title")[0:10] == "Base Truck"):
                latestBaseTruckWO = df.get("openedOn")[i]
                break # If latestDate comes from a base truck work order than use that one

        index += 1


    url = 'https://torcroboticssb.us.accelix.com/api/entities/def/WorkOrdersRequests/search-paged'
    # Cookie to the sandbox
    data = {'select': [{'name': 'site'}, {'name': 'createdBy'}, {'name': 'updatedBy'}, {'name': 'updatedSyncDate'}, {'name': 'dataSource'}, {'name': 'status'}, {'name': 'createdOn'}, {'name': 'assetId'}], 'filter': {'and': [{'name': 'isDeleted', 'op': 'isfalse'}]}, 'order': [{'name': 'number', 'desc': True}], 'pageSize': 20, 'page': 0, 'fkExpansion': True}

    index = 1
    latestWORequest = None
    while(latestWORequest == None):
        response = requests.post(url, headers=headers, data=json.dumps(data))
        assert response.status_code == 200
        response = response.json()
        dx = response['data']

        data['page'] = index
        response = requests.post(url, headers=headers, data=json.dumps(data))
        assert response.status_code == 200
        dx.extend(response.json()['data'])

        # dataframe
        df = pd.DataFrame(data={cx: [x[cx] for x in dx] for cx in sorted(dx[0].keys())})

        # get most recent base truck error
        for i in range(df.shape[0]):
            if(df.get("assetId")[i] != None and (df.get("assetId")[i]["subsubtitle"] == "Freightliner" or df.get("assetId")[i]["subsubtitle"] == "Trailer")):
                latestWORequest = df.get("createdOn")[i]
                break # If latestDate comes from a base truck work order than use that one

        index += 1
    
    latestFlukeUpload = parser.isoparse(latestBaseTruckWO)
    latestFlukeWORUpload = parser.isoparse(latestWORequest)

    if(latestFlukeUpload < latestFlukeWORUpload):
        latestFlukeUpload = latestFlukeWORUpload


    # Checks if the new data has already been processed
    filter_data = []
    for report in inspection_data:
        motiveTime = parser.isoparse(report["date"])
        
        print(f'{motiveTime} - Motive Time : {latestFlukeUpload} - Fluke Time')

        if(motiveTime > latestFlukeUpload): # if motive inspection report time comes after the latest date from fluke
            filter_data.append(report)

    return filter_data

def get_motive_data() -> list:
    """
    Gets the data of inspection reports within the last day from motive API and returns the filtered data. Filtered data is ones with a issue to request a work order for and that have not already been posted to fluke. 

    Returns:
        list: List of inspection reports that have been filtered for new issues that must be posted to fluke
    """

    # Gets all of the issues within the past 24 hours
    index = 1
    issues = []
    while len(issues) <= 5: 
        # end point for motive's truck status data, gets most recent inspection report
        motive = f"https://api.keeptruckin.com/v2/inspection_reports?per_page=50&page_no={index}"

        # get truck status data
        response = requests.get(motive, headers=motive_headers)

        new_issues = filter_issues(response.json())

        issues = issues + new_issues

        time = str(response.json()['inspection_reports'][0]['inspection_report']['time'])
        time = datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

        # Get current time in UTC
        now = datetime.now(timezone.utc)
        past_24_hours = now - timedelta(days=1)

        # Check if the given time is within the past 24 hours
        if past_24_hours <= time <= now:
            pass
        else:
            break
        
            
        try: 
            print(str(len(issues)) + " " + str(index) + " " + str(response.json()['inspection_reports'][0]['inspection_report']['time']))
        except:
            print(response.json())
            break
        index += 1

    data = issues

    data = new_data(data)

    return data

def convert_to_post(data: list) -> list: 
    """
    Converts filtered data from motive to a format that can be posted to fluke api
    
    Args:
        data (list): List of inspection reports that have been filtered for new issues that must be posted to fluke

    Returns:
        list: List of inspection reports that have been converted to a format that can be posted to fluke api
    """

    # Get all of the asset ids from fluke
    def getfreightlinersAndTrailers():
        # config
        url = 'https://torcroboticssb.us.accelix.com/api/entities/def/Assets/search-paged'

        data = {
            "select": [
                {"name": "c_description"},
                {"name": "c_assettype"},
                {"name": "id"}
            ],
            "filter": {
                "and": [
                    {"name": "isDeleted", "op": "isfalse"}
                ]
            },
            "order": [
                {"name": "c_serialnumber", "desc": True}
            ],
            "pageSize": 20,
            "page": 0,
            "fkExpansion": True
        }

        # API
        response = requests.post(url, headers=headers, data=json.dumps(data))
        assert response.status_code == 200
        response = response.json()
        dx = response['data']
        pages = response['totalPages']
        for page in tqdm(range(1, pages), desc='Assets'):
            data['page'] = page
            response = requests.post(url, headers=headers, data=json.dumps(data))
            assert response.status_code == 200
            dx.extend(response.json()['data'])

        # dataframe
        df = pd.DataFrame(data={cx: [x[cx] for x in dx] for cx in sorted(dx[0].keys())})

        filtered = []
        for i, row in enumerate(df.iloc[:, 0]):

            if "Freightliner" in row['title'] or "Trailer" in row['title']:
                filtered.append([df.iloc[i][1], df.iloc[i][2]])

        return filtered
    
    # Builds work orders for Work Order
    def buildWorkOrder(post):
        description = []
        under_description = []

        for issue in post['issues']:
            adding = f"{issue['notes']}"

            if issue['priority'] == 'major': # puts the major issue first in the description
                description.insert(0, "Major Issue - " + issue['category'])
                under_description.insert(0, adding)
            else:
                description.append("Minor Issue - " + issue['category'])
                under_description.append(adding)

        # building overall priority
        overall_priority = {
            'entity': 'PriorityLevels',
            'id': '954c61fe-6f07-4c5c-8de4-b72594321c42',
            'isDeleted': False,
            'number': 6,
            'title': 'Base Truck Blocking'
        } 

        # building work order type
        work_order_type = {'entity': 'WorkOrderTypes', 'id': 'f04406fe-847e-4d49-899e-0053758d7fc3', 'isDeleted': False, 'number': 24, 'title': 'Motive Base Truck Corrective'}

        # Holder for the asset sent to work order
        assetId = {}
        c_compid = ""

        try: 
            if post['vehicle'] != None:

                for row in df:
                    if post['vehicle']['number'] in row[0]:
                        truckId = row[1]

                if truckId == None:
                    print(f'{post} is not a valid truck in fluke. Ending this post.')
                    return False

                assetId = {
                    'entity': 'Assets', 
                    'id': truckId,
                    'image': None,
                    'isDeleted': False,
                    'subsubtitle': post['vehicle']['make'].title(),
                    'subtitle': post['vehicle']['number'],
                    'title': post['vehicle']['number']
                }

                c_compid = post['vehicle']['number']
            else:
                
                for row in df:
                    if post['asset']['name'] in row[0]:
                        trailerId = row[1]

                if trailerId == None:
                    print(f'{post} is not a valid trailer in fluke. Ending this post.')
                    return False

                assetId = {
                    'entity': 'Assets', 
                    'id': trailerId, # Need to be able to get ids for trailer assets - use post['asset']['name']
                    'image': None,
                    'isDeleted': False,
                    'subsubtitle': post['asset']['make'],
                    'subtitle': post['asset']['name'],
                    'title': post['asset']['name']
                }

                c_compid = post['asset']['name']
        
        except Exception as err:
            print("Could not process the asset of: " + str(post))
            return False

        # payload for post request
        post_data = {
            "occurredOn": post['date'],
            "properties": {
                'assetId': assetId,
                'description': ", ".join(f"{i+1}. {desc}" for i, desc in enumerate(description)) if len(description) != 1 else description[0],
                'details': post['inspection_type'] + ' Inspection\n' + ", ".join(f"{i+1}. {desc}" for i, desc in enumerate(under_description)) if len(description) != 1 else post['inspection_type'] + ' Inspection\n' + under_description[0],
                'createdBy': {
                    'entity': 'UserData',
                    'id': '00000000-0000-0000-0000-000000000002', # Need to get user UUID by username
                    'number': 0, # Need to get the user number
                    'title': f"{post['driver']['last_name'].replace(',', '').title()} {post['driver']['first_name'].replace(',', '').title()} "
                },
                'c_priority': overall_priority,
                'c_jobstatus': {
                    'entity': 'JobStatus', 
                    'id': '11111111-8588-40d2-b33d-111111111113', # UUID For New
                    'isDeleted': False, 
                    'number': 3, 
                    'title': 'New'
                },
                'c_workordertype': work_order_type,
                'c_requesteremail': post['driver']['email'],
                'c_compid': c_compid
            }
        }

        return post_data

    def buildWorkOrderRequest(post):
        description = []
        under_description = []

        for issue in post['issues']:
            adding = f"{issue['notes']}"

            description.append(issue['category'])
            under_description.append(adding)

        # Holder for the asset sent to work order
        assetId = {}
        c_compid = ""

        try: 
            if post['vehicle'] != None:

                for row in df:
                    if post['vehicle']['number'] in row[0]:
                        truckId = row[1]

                if truckId == None:
                    print(f'{post} is not a valid truck in fluke. Ending this post.')
                    return False

                assetId = {
                    'entity': 'Assets', 
                    'id': truckId,
                    'image': None,
                    'isDeleted': False,
                    'subsubtitle': post['vehicle']['make'].title(),
                    'subtitle': post['vehicle']['number'],
                    'title': post['vehicle']['number'],
                }

                c_compid = post['vehicle']['number']
            else:
                
                for row in df:
                    if post['asset']['name'] in row[0]:
                        trailerId = row[1]

                if trailerId == None:
                    print(f'{post} is not a valid trailer in fluke. Ending this post.')
                    return False

                assetId = {
                    'entity': 'Assets', 
                    'id': trailerId, # Need to be able to get ids for trailer assets - use post['asset']['name']
                    'image': None,
                    'isDeleted': False,
                    'subsubtitle': post['asset']['make'],
                    'subtitle': post['asset']['name'],
                    'title': post['asset']['name']
                }

                c_compid = post['asset']['name']
        
        except Exception as err:
            print("Could not process the asset of: " + str(post))
            return False

        # payload for post request
        post_data = {
            "properties": {
                'assetId': assetId,
                'description': ", ".join(f"{i+1}. {desc}" for i, desc in enumerate(description)) if len(description) != 1 else description[0],
                'details': f'Motive Base Truck - {post["inspection_type"]}: ' + ' - '.join(f"Minor Issue - {desc}" for desc in under_description),
                'createdBy': {
                    'entity': 'UserData',
                    'id': '00000000-0000-0000-0000-000000000002', # Need to get user UUID by username
                    'number': 0, # Need to get the user number
                    'title': f"{post['driver']['last_name'].replace(',', '').title()} {post['driver']['first_name'].replace(',', '').title()} "
                },
                'formId': 7,
                'c_requesteremail': post['driver']['email'],
                'c_compid': c_compid,
                'c_requestedOn': post['time']
            }
        }

        return post_data

    df = getfreightlinersAndTrailers()

    converted_data = []
    
    # For every truck that needs a post
    for post in data: 

        useWorkOrderRequest = True
        # If there is a major issue than switch to basic Work Orders
        for issue in post['issues']:
            if issue['priority'] == 'major': # puts the major issue first in the description
                useWorkOrderRequest = False

        if(useWorkOrderRequest):
            post_data = buildWorkOrderRequest(post)
        else:
            post_data = buildWorkOrder(post)

        if(post_data != False):
            converted_data.append(post_data)

    return converted_data

def post_WO(data: list) -> list:
    """
    Posts the work orders to fluke api and returns the responses

    Args:
        data (list): List of inspection reports that have been converted to a format that can be posted to fluke api

    Returns:
        list: List of responses from the post requests
    """

    # Config
    tenant = "torcroboticssb.us.accelix.com"
    site = "def"
    woEndpoint = f"https://{tenant}/api/entities/{site}/WorkOrders"
    worEndpoint = f"https://{tenant}/api/entities/{site}/WorkOrdersRequests"

    responses = []
    # Send a post request with the data
    for work_order in data:
        
        endpoint = ""
        if 'major' in work_order['properties']['description'].lower():
            endpoint = woEndpoint
        else:
            endpoint = worEndpoint

        response = requests.post(endpoint, headers=headers, data=json.dumps(work_order))
        responses.append(response)

    return responses

def test_save(data: list):
    """
    When testing, declares what data that would be posted to fluke

    Args:
        data (list): List of inspection reports converted to a format that can be posted to fluke api
    """

    # Save the data for testing purposes
    with open ('test_data.txt', 'w') as f:
        for post in data:
            f.write(str(post) + "\n")

def main():
    """
    Main loop that checks for new inspection reports from motive and posts them to fluke (or saves them to a csv file during testing)
    """
    # returns a list of recent inspection reports that had major or minor issues
    data = get_motive_data()
    
    if len(data) == 0:
        print("No new data.")
        return

    # converts the previous data list to a list that can be posted to fluke api
    WO_posts = convert_to_post(data)

    # posts work orders to fluke and returns the responses
    responses = post_WO(WO_posts)

    # Saves work order to csv file during testing
    # test_save(WO_posts)

    for response in responses:
        print(response.json())

    print("::notice::Detected Work Order Post")

if __name__ == "__main__":
    main()