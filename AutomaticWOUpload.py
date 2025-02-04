import requests
import os
import json
import pandas as pd
from dateutil import parser

""" 
Defining all api fields
"""
# REAL: motive = "https://api.keeptruckin.com/v2/inspection_reports?per_page=10"
# USED FOR TESTING: motive = "https://api.keeptruckin.com/v2/inspection_reports?start_date=2025-01-02&end_date=2025-01-02&per_page=50"

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

  for report in inspection_data:
    inspection = report.get('inspection_report', {})
    id = inspection.get('id')
    time = inspection.get('time')
    location = inspection.get('location')

    truck_issues = {
      'id': id,
      'date': time,
      'location': location,
      'vehicle': inspection.get('vehicle'),
      'driver': inspection.get('driver'),
      'inspection_type': "Post Trip" if inspection.get('inspection_type') == "post_trip" else "Pre Trip",
      'odometer': inspection.get('odometer'),
      'issues': []
    }

    # Check for major issues in inspected parts
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

    if truck_issues['issues']:
      important_issues.append(truck_issues)

  return important_issues

def new_data(inspection_data: list) -> list:
    """
    Filters out the data that has already been seen and returns the new data
    
    Args:
        inspection_data (list): List of inspection reports that have been filtered for issues

    Returns:
        list: List of inspection reports that are new since last run
    """

    # The previous 10 inspection reports that have been sent to Fluke
    key = os.getenv("FLUKE_KEY")
    url = 'https://torcroboticssb.us.accelix.com/api/entities/def/WorkOrders/search-paged'

    headers = {
        "Content-Type": "application/json", 
        "Cookie": key
    }
    data = {'select': [{'name': 'site'}, {'name': 'createdBy'}, {'name': 'updatedBy'}, {'name': 'updatedSyncDate'}, {'name': 'dataSource'}, {'name': 'status'}, {'name': 'closedOn'}, {'name': 'openedOn'}, {'name': 'startDate'}, {'name': 'assetId'}, {'name': 'requestId'}, {'name': 'scheduledEventId'}, {'name': 'description'}, {'name': 'details'}, {'name': 'taskId'}, {'name': 'priorityCode'}, {'name': 'rimeRanking'}, {'name': 'signature'}, {'name': 'image'}, {'name': 'geolocation'}, {'name': 'reason'}, {'name': 'parentId'}, {'name': 'c_workordertype'}, {'name': 'c_jobstatus'}, {'name': 'c_priority'}, {'name': 'c_completedon'}, {'name': 'c_projectid'}, {'name': 'c_problemtype'}, {'name': 'c_estimatedhours'}, {'name': 'c_downtime'}, {'name': 'c_comments'}, {'name': 'c_completeddate'}, {'name': 'c_failurecode'}, {'name': 'c_issuecode'}, {'name': 'c_department'}, {'name': 'c_linenumber'}, {'name': 'c_availablestatus'}, {'name': 'c_completedby'}, {'name': 'c_closedby'}, {'name': 'c_requestedon'}, {'name': 'c_requesteremail'}, {'name': 'c_site'}, {'name': 'c_building'}, {'name': 'c_imagefield'}, {'name': 'c_floorlevel'}, {'name': 'c_requesterphone'}, {'name': 'c_compid'}, {'name': 'c_onholdreason'}, {'name': 'c_assetmountingposition'}, {'name': 'c_assettypesymptom'}, {'name': 'c_symptom'}, {'name': 'c_foreignkeylookupsymptom'}, {'name': 'c_terminalzone'}, {'name': 'c_downtimestart'}, {'name': 'c_downtimeend'}, {'name': 'c_repairtimehrs'}, {'name': 'c_repairtimestart'}, {'name': 'c_repairtimeend'}, {'name': 'c_location'}, {'name': 'c_documentlink'}, {'name': 'c_symptomassettypediagnosis'}, {'name': 'c_locationdiagnosis'}, {'name': 'c_diagnosis'}, {'name': 'c_documentlinkdiagnosis'}, {'name': 'c_parentasset'}, {'name': 'c_maintenancelog'}, {'name': 'c_parentassetdescription'}, {'name': 'c_firmware'}, {'name': 'c_deploymentsoftware'}, {'name': 'c_assettypeasset'}, {'name': 'c_tasknumber'}, {'name': 'id'}, {'name': 'number'}, {'name': 'createdOn'}, {'name': 'updatedOn'}], 'filter': {'and': [{'name': 'isDeleted', 'op': 'isfalse'}]}, 'order': [{'name': 'number', 'desc': True}], 'pageSize': 20, 'page': 0, 'fkExpansion': True}

    # API
    response = requests.post(url, headers=headers, data=json.dumps(data))
    assert response.status_code == 200
    response = response.json()
    dx = response['data']

    data['page'] = 1
    response = requests.post(url, headers=headers, data=json.dumps(data))
    assert response.status_code == 200
    dx.extend(response.json()['data'])

    # dataframe
    df = pd.DataFrame(data={cx: [x[cx] for x in dx] for cx in sorted(dx[0].keys())})

    # get most recent base truck error
    latestDate = None
    for i in range(df.shape[0]):
        latestDate = df.get("openedOn")[i]

        if(df.get("c_priority")[0].get("title")[0:10] == "Base Truck"):
            break # If latestDate comes from a base truck work order than use that one
    latestDate = parser.isoparse(latestDate)

    # Checks if the new data has already been processed
    filter_data = []
    for report in inspection_data:
        motiveTime = parser.isoparse(report["date"])
        if(motiveTime > latestDate): # motive inspection report time comes after the latest date from fluke
            print(f"{motiveTime} happened more recently than {latestDate}")
            filter_data.append(report)

    return filter_data

def get_motive_data() -> list:
    """
    Gets the data of 10 most recent inspection reports from motive API and returns the filtered data

    Returns:
        list: List of inspection reports that have been filtered for new issues that must be posted to fluke
    """

    # Environment variables from GitHub
    key = os.getenv("MOTIVE_KEY")
    endpoint = os.getenv("MOTIVE_ENDPOINT")

    motive_headers = {
        "accept": "application/json", 
        "X-Api-Key": key
    }

    response = requests.get(endpoint, headers=motive_headers)
    data = response.json()

    print('Using: ' +  key + " and " + endpoint)
    print(data)
    f1_data = filter_issues(data['inspection_reports'])
    f2_data = new_data(f1_data)

    return f2_data

def convert_to_post(data: list) -> list: 
    """
    Converts filtered data from motive to a format that can be posted to fluke api
    
    Args:
        data (list): List of inspection reports that have been filtered for new issues that must be posted to fluke

    Returns:
        list: List of inspection reports that have been converted to a format that can be posted to fluke api
    """

    TRUCK_IDS = os.getenv("TRUCK_IDS")
    converted_data = []
    
    for post in data: 
        # building description
        description = []
        for issue in post['issues']:
            adding = f"{issue['category']} + {issue['notes']}"

            if issue['priority'] == 'major': # puts the major issue first in the description
                description.insert(0, "Major Issue: " + adding)
            else:
                description.append("Minor Issue: " + adding)

        # building overall priority
        overall_priority = { # updated to major if there is a major issue in the inspection report
            'entity': 'PriorityLevels',
            'id': '3ed2ba71-fe10-47a3-abba-a92373957b0e',
            'isDeleted': False,
            'number': 7,
            'title': 'Base Truck Non-Blocking'
        } 
        if description[0][0:4] == 'Major':
            overall_priority = {
                'entity': 'PriorityLevels',
                'id': '954c61fe-6f07-4c5c-8de4-b72594321c42',
                'isDeleted': False,
                'number': 6,
                'title': 'Base Truck'
            } 

        # building work order type
        work_order_type = { # updated to major if there is a major issue in the inspection report
                'entity': 'WorkOrderTypes',
                'id': '94b4593d-8e8b-49ab-a71c-7c0430667d50',
                'isDeleted': False,
                'number': 21,
                'title': 'Base Truck Preventive'
        }
        if description[0][0:4] == 'Major':
            work_order_type = { 
                'entity': 'WorkOrderTypes',
                'id': 'b2f98322-14af-44a9-b853-e7d0ec8ff9f7',
                'isDeleted': False,
                'number': 20,
                'title': 'Base Truck Corrective'
            }
        
        # payload for post request
        post_data = {
            "occurredOn": post['date'],
            "properties": {
                'assetId': {
                    'entity': 'Assets', 
                    'id': TRUCK_IDS[post['vehicle']['number']],
                    'image': None,
                    'isDeleted': False,
                    'subsubtitle': post['vehicle']['make'],
                    'subtitle': post['vehicle']['number'],
                    'title': post['vehicle']['number']
                },
                'description': ", ".join(f"{i+1}. {desc}" for i, desc in enumerate(description)),
                'details': f"Inspection Type: {post['inspection_type']}, Odometer: {post['odometer']}",
                'createdBy': {
                    'entity': 'UserData',
                    'id': '00000000-0000-0000-0000-000000000002', # Need to get user UUID by username
                    'number': 0, # Need to get the user number
                    'title': f"{post['driver']['last_name'].replace(',', '')} {post['driver']['first_name']}"
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


            }
        }

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


    key = os.getenv("FLUKE_KEY")
    endpoint = os.getenv("FLUKE_ENDPOINT")

    fluke_headers = {
        "Content-Type": "application/json", 
        "Cookie": key
    }


    responses = []
    # Send a post request with the data
    for work_order in data:
        break
        # response = requests.post(endpoint, headers=fluke_headers, data=json.dumps(work_order))
        # responses.append(response)

    return responses

def test_save(data: list):
    """
    When testing, saves the data that would be posted to fluke to a csv file called uploaded_WOs.csv

    Args:
        data (list): List of inspection reports converted to a format that can be posted to fluke api
    """

    # Save the data for testing purposes
    with open("uploaded_WOs.csv", "a") as f:
        for wo in data:
            f.write(str(wo))
            f.write('\n')

def main():
    """
    Main loop that checks for new inspection reports from motive and posts them to fluke (or saves them to a csv file during testing)
    """

    try:
        # returns a list of recent inspection reports that had major or minor issues
        data = get_motive_data()
        
        # converts the previous data list to a list that can be posted to fluke api
        WO_posts = convert_to_post(data)

        # posts work orders to fluke and returns the responses
        # responses = post_WO(WO_posts)

        # Saves work order to csv file during testing
        test_save(WO_posts)
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()