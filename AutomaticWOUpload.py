import requests
import os
import json
import pandas as pd
from dateutil import parser
# Gets inspection reports of past day
from datetime import datetime, timedelta, timezone


""" 
Defining all api fields
"""

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
    # key = os.getenv("FLUKE_KEY")

    # headers = {
    #     "Content-Type": "application/json", 
    #     "Cookie": key
    # }

    # Cookie to the sandbox
    headers = {
        "Content-Type": "application/json", 
        "Cookie": f"JWT-Bearer=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiI5NWZkYzZhYS0wOWNiLTQ0NzMtYTIxZC1kNzBiZTE2NWExODMiLCJ0aWQiOiJUb3JjUm9ib3RpY3MiLCJleHAiOjQxMDI0NDQ4MDAsInNpZCI6bnVsbCwiaWlkIjpudWxsfQ.Gh3b3ibvSeYy7YpqDUI9daup86dYjsM_lisS-8ESWDs"
    }

    data = {'select': [{'name': 'site'}, {'name': 'createdBy'}, {'name': 'updatedBy'}, {'name': 'updatedSyncDate'}, {'name': 'dataSource'}, {'name': 'status'}, {'name': 'closedOn'}, {'name': 'openedOn'}, {'name': 'startDate'}, {'name': 'assetId'}, {'name': 'requestId'}, {'name': 'scheduledEventId'}, {'name': 'description'}, {'name': 'details'}, {'name': 'taskId'}, {'name': 'priorityCode'}, {'name': 'rimeRanking'}, {'name': 'signature'}, {'name': 'image'}, {'name': 'geolocation'}, {'name': 'reason'}, {'name': 'parentId'}, {'name': 'c_workordertype'}, {'name': 'c_jobstatus'}, {'name': 'c_priority'}, {'name': 'c_completedon'}, {'name': 'c_projectid'}, {'name': 'c_problemtype'}, {'name': 'c_estimatedhours'}, {'name': 'c_downtime'}, {'name': 'c_comments'}, {'name': 'c_completeddate'}, {'name': 'c_failurecode'}, {'name': 'c_issuecode'}, {'name': 'c_department'}, {'name': 'c_linenumber'}, {'name': 'c_availablestatus'}, {'name': 'c_completedby'}, {'name': 'c_closedby'}, {'name': 'c_requestedon'}, {'name': 'c_requesteremail'}, {'name': 'c_site'}, {'name': 'c_building'}, {'name': 'c_imagefield'}, {'name': 'c_floorlevel'}, {'name': 'c_requesterphone'}, {'name': 'c_compid'}, {'name': 'c_onholdreason'}, {'name': 'c_assetmountingposition'}, {'name': 'c_assettypesymptom'}, {'name': 'c_symptom'}, {'name': 'c_foreignkeylookupsymptom'}, {'name': 'c_terminalzone'}, {'name': 'c_downtimestart'}, {'name': 'c_downtimeend'}, {'name': 'c_repairtimehrs'}, {'name': 'c_repairtimestart'}, {'name': 'c_repairtimeend'}, {'name': 'c_location'}, {'name': 'c_documentlink'}, {'name': 'c_symptomassettypediagnosis'}, {'name': 'c_locationdiagnosis'}, {'name': 'c_diagnosis'}, {'name': 'c_documentlinkdiagnosis'}, {'name': 'c_parentasset'}, {'name': 'c_maintenancelog'}, {'name': 'c_parentassetdescription'}, {'name': 'c_firmware'}, {'name': 'c_deploymentsoftware'}, {'name': 'c_assettypeasset'}, {'name': 'c_tasknumber'}, {'name': 'id'}, {'name': 'number'}, {'name': 'createdOn'}, {'name': 'updatedOn'}], 'filter': {'and': [{'name': 'isDeleted', 'op': 'isfalse'}]}, 'order': [{'name': 'number', 'desc': True}], 'pageSize': 20, 'page': 0, 'fkExpansion': True}

    # API
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
    
    latestFlukeUpload = parser.isoparse(latestBaseTruckWO)

    # Checks if the new data has already been processed
    filter_data = []
    for report in inspection_data:
        motiveTime = parser.isoparse(report["date"])
        
        if(motiveTime > latestFlukeUpload): # if motive inspection report time comes after the latest date from fluke
            print(f"{motiveTime} happened more recently than {latestFlukeUpload}")
            filter_data.append(report)

    return filter_data

def get_motive_data() -> list:
    """
    Gets the data of inspection reports within the last day from motive API and returns the filtered data. Filtered data is ones with a issue to request a work order for and that have not already been posted to fluke. 

    Returns:
        list: List of inspection reports that have been filtered for new issues that must be posted to fluke
    """

    # Environment variables from GitHub
    # key = os.getenv("MOTIVE_KEY")

    # motive_headers = {
    #     "accept": "application/json", 
    #     "X-Api-Key": key
    # }

    motive_headers = {
        "accept": "application/json", 
        "X-Api-Key": "9e90504a-82f0-4ed4-b54c-ce37f388f211"
    }

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
            print("The given time is within the past 24 hours.")
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

    TRUCK_IDS = {
        "C7": "d663f82e-7219-482a-a5d3-a8e57ba4ea59",
        "C8": "706ea360-cb06-4f37-8d70-2c0bb82fb0ca",
        "C9": "9c5b2846-af76-476b-b252-2c9707188276",
        "C10": "c4fed5c5-dd59-42a3-8d78-809952ad4cdf",
        "C11": "bff05cfc-7311-41d0-b1ad-25ee1d3cc632",
        "C12": "45344ac8-f29d-4164-9a12-fa10fcb30500",
        "C14": "935c56dc-6989-4fc8-ba25-c76f93bf1261",
        "C15": "a646399f-b228-45d7-935e-d6b2ec61288c",
        "C16": "12fbaa2b-05f2-4637-82e7-996612f1df4b",
        "C17": "0cbc0a7b-39ea-477e-9e91-fb329ecd7e5b",
        "C18": "261cb7bd-d9c2-4b0d-a143-8fd65d101adc",
        "C20": "c0e13ff9-3b76-4141-9af0-b162ffd70cb0",
        "C21": "f2a17bb6-1888-41b2-8c84-b95035b7f473",
        "C22": "54118751-e006-4c3b-b068-8a899d5f57f7",
        "C24": "df6d179f-878c-4a5a-92b1-13afe09b2b73",
        "C25": "45c485b7-e3f2-41bd-86ed-a686b9af3433",
        "C26": "f6bde98a-249b-4e7c-805d-9edf8bcfe2f0",
        "ZZ6217": "5421a0ca-2262-4255-a73d-99e5016ed370",
        "ZZ6218": "73dc89c4-f6b4-4f1c-9830-27db4508049c",
        "ZZ6175": "b4e31fed-0cc0-480c-a1cf-064e38558f21",
        "ZZ6180": "9e8a13b2-6165-4f9e-b704-2c69b5fc04cc",
        "ZZ6181": "75cc8154-5031-4b40-94bf-bdcce1ff2fb4",
        "ZZ6182": "a1f9358a-411f-4c18-a451-34f478d0d1d4",
        "ZZ6183": "ef8f9a55-00d6-4a0a-b36d-8d7aab9bfd32",
        "ZZ6184": "81761df3-2124-4d20-99ee-b8a9038d46e6",
        "ZZ6214": "f7e27cdd-c1f8-4dea-a82f-bd557126e3b1",
        "ZZ6215": "179fe759-6b9d-4335-a1b5-ff53483444f0",
        "ZZ6216": "ef684bc6-c23b-4a78-b9ab-37323f7f495b"
    }
    converted_data = []
    
    for post in data: 

        print("Post: " + str(post))

        # building description
        description = []
        for issue in post['issues']:

            if(issue['category'] == "Other"):
                issue['category'] = "Category - Other"

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
        

        try: 
            assetId = {
                'entity': 'Assets', 
                'id': TRUCK_IDS[post['vehicle']['number']],
                'image': None,
                'isDeleted': False,
                'subsubtitle': post['vehicle']['make'],
                'subtitle': post['vehicle']['number'],
                'title': post['vehicle']['number']
            }
        except:
            assetId = {
                'entity': 'Assets', 
                'id': None,
                'image': None,
                'isDeleted': False,
                'subsubtitle': None,
                'subtitle': None,
                'title': None
            }

        # payload for post request
        post_data = {
            "occurredOn": post['date'],
            "properties": {
                'assetId': assetId,
                'description': ", ".join(f"{i+1}. {desc}" for i, desc in enumerate(description)) if len(description) > 1 else description[0],
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


    # key = os.getenv("FLUKE_KEY")

    # fluke_headers = {
    #     "Content-Type": "application/json", 
    #     "Cookie": key
    # }

    # Cookie to the sandbox
    fluke_headers = {
        "Content-Type": "application/json", 
        "Cookie": f"JWT-Bearer=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiI5NWZkYzZhYS0wOWNiLTQ0NzMtYTIxZC1kNzBiZTE2NWExODMiLCJ0aWQiOiJUb3JjUm9ib3RpY3MiLCJleHAiOjQxMDI0NDQ4MDAsInNpZCI6bnVsbCwiaWlkIjpudWxsfQ.Gh3b3ibvSeYy7YpqDUI9daup86dYjsM_lisS-8ESWDs"
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
    print("Saving Data")

    with open("uploaded_WOs.csv", "a") as f:
        for wo in data:
            f.write(str(wo))
            f.write('\n')

def main():
    """
    Main loop that checks for new inspection reports from motive and posts them to fluke (or saves them to a csv file during testing)
    """
    # returns a list of recent inspection reports that had major or minor issues
    data = get_motive_data()
    
    # converts the previous data list to a list that can be posted to fluke api
    WO_posts = convert_to_post(data)

    print(WO_posts)

    # posts work orders to fluke and returns the responses
    # responses = post_WO(WO_posts)

    # Saves work order to csv file during testing
    test_save(WO_posts)
    

if __name__ == "__main__":
    main()