import requests
import json
import pandas as pd
from dateutil import parser
from datetime import datetime, timedelta, timezone
import os


# Tells if the script should be run in test mode or production
production = True
motiveProduction = True
checkData = False

# Cookie to the fluke
production_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiI5NWZkYzZhYS0wOWNiLTQ0NzMtYTIxZC1kNzBiZTE2NWExODMiLCJ0aWQiOiJUb3JjUm9ib3RpY3MiLCJleHAiOjQxMDI0NDQ4MDAsInNpZCI6bnVsbCwiaWlkIjpudWxsfQ.Gh3b3ibvSeYy7YpqDUI9daup86dYjsM_lisS-8ESWDs"
sandbox_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiI5NWZkYzZhYS0wOWNiLTQ0NzMtYTIxZC1kNzBiZTE2NWExODMiLCJ0aWQiOiJUb3JjUm9ib3RpY3NTQiIsImV4cCI6NDEwMjQ0NDgwMCwic2lkIjpudWxsLCJpaWQiOm51bGx9.94frut80sKx43Cm4YKfVbel8upAQ8glWdfYIN3tMF7A"

# Motive API Key
key = "9e90504a-82f0-4ed4-b54c-ce37f388f211" if motiveProduction else "ab7e71b6-e38e-469b-93ac-3b50b81aa8bd" # - This is the key for the sandbox account

headers = {
    "Content-Type": "application/json", 
    "Cookie": "JWT-Bearer=" + production_key if production else "JWT-Bearer=" + sandbox_key,
}

motive_headers = {
    "accept": "application/json", 
    "X-Api-Key": key
}

# Values for fluke endpoints
tenant = "torcrobotics.us.accelix.com" if production else "torcroboticssb.us.accelix.com"
site = "def"

def getFreightlinersAndTrailers() -> pd.DataFrame:
    """
    Gets all of the freightliners and trailer assets from fluke.

    Returns:
        pandas.DataFrame: A DataFrame containing the following columns for each asset:
            - 'c_description': Number of the truck (ex: C19 - Mill Mountain).
            - 'c_assettype': The type of the asset (either 'Freightliner' or 'Trailer').
            - 'id': The unique identifier of the asset.

    """

    # Get the freightliner assets
    url = f'https://{tenant}/api/entities/{site}/Assets/search-paged'

    data = {
        "select": [
            {"name": "c_description"},
            {"name": "c_assettype"},
            {"name": "id"}
        ],
        "filter": {
            "and": [
                {"name": "isDeleted", "op": "isfalse"},
                {"name": "c_assettype", "op": "eq", "value": "Freightliner"},
            ],
        },
        "order": [],
        "pageSize": 50,
        "page": 0,
        "fkExpansion": True
    }

    # API
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    if response.status_code != 200:
        print("Error getting Freightliners", flush=True)
        return False
    
    response = response.json()
    dx = response['data']
    pages = response['totalPages']
    for page in range(1, pages):
        data['page'] = page
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code != 200:
            print("Error getting Freightliners", flush=True)
            return False
        dx.extend(response.json()['data'])

    # Get the trailer assets
    url = f'https://{tenant}/api/entities/{site}/Assets/search-paged'

    data = {
        "select": [
            {"name": "c_description"},
            {"name": "c_assettype"},
            {"name": "id"}
        ],
        "filter": {
            "and": [
                {"name": "isDeleted", "op": "isfalse"},
                {"name": "c_assettype", "op": "eq", "value": "Trailer"},
            ],
        },
        "order": [],
        "pageSize": 50,
        "page": 0,
        "fkExpansion": True
    }

    # API
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print("Error getting Trailers", flush=True)
        return False
    response = response.json()
    dx.extend(response['data'])
    pages = response['totalPages']
    for page in range(1, pages):
        data['page'] = page
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code != 200:
            print("Error getting Trailers", flush=True)
            return False
        dx.extend(response.json()['data'])

    # dataframe
    df = pd.DataFrame(data={cx: [x[cx] for x in dx] for cx in sorted(dx[0].keys())})

    return df


def filterIssues(inspection_data: list) -> list:
  """
  Given raw inspection data from Motive, returns a list of inspections that
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


def checkNewData(inspection_data: list) -> list:
    """
    Filters out the data that has already been seen and returns the new data
    
    Args:
        inspection_data (list): List of inspection reports that have been filtered for issues

    Returns:
        list: List of inspection reports that are new since last run
    """

    # Find the latest issue about the truck uploaded to fluke
    url = f'https://{tenant}/api/entities/{site}/WorkOrders/search-paged'

    # Cookie to the sandbox
    data = {
        'select': 
            [{'name': 'openedOn'}, {'name': 'assetId'}], 
        'filter': {
            'and': [
                {"name": "c_workordertype", "op": "eq", "value": "Motive Base Truck Corrective"},
            ],
        }, 
        'order': [{'name': 'number', 'desc': True}], 'pageSize': 1, 'page': 0, 'fkExpansion': True
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))

    try:
        if response.status_code != 200:
            print("Error getting Work Orders Major Issues", flush=True)
            return False

        response.json()

        # The data is filtered to get only the 1 latest base truck blocking 
        lastMajorBaseTruck = response.json()['data'][0]['openedOn']
        
        # The data is filtered to get only the 1 latest base truck nonblocking 
        lastMajorBaseTruck = parser.isoparse(lastMajorBaseTruck)
    except:
        lastMajorBaseTruck = datetime(2021, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
    

    # Getting the work orders requests latest upload from motive
    url = f'https://{tenant}/api/entities/{site}/WorkOrdersRequests/search-paged'
    # Cookie to the sandbox
    data = {'select': [{'name': 'site'}, {'name': 'createdBy'}, {'name': 'updatedBy'}, {'name': 'updatedSyncDate'}, {'name': 'dataSource'}, {'name': 'status'}, {'name': 'createdOn'}, {'name': 'assetId'}], 'filter': {'and': [{'name': 'isDeleted', 'op': 'isfalse'}]}, 'order': [{'name': 'number', 'desc': True}], 'pageSize': 50, 'page': 0, 'fkExpansion': True}

    index = 0
    lastMinorBaseTruck = None
    while(lastMinorBaseTruck == None):
        data['page'] = index
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code != 200:
            print("Error getting Work Order Requests", flush=True)
            return False
        
        dx = response.json()['data']

        # dataframe
        df = pd.DataFrame(data={cx: [x[cx] for x in dx] for cx in sorted(dx[0].keys())})

        # get most recent base truck error
        for i in range(df.shape[0]):
            if(df.get("assetId")[i] != None and (df.get("assetId")[i]["subsubtitle"] == "Freightliner" or df.get("assetId")[i]["subsubtitle"] == "Trailer")):
                lastMinorBaseTruck = df.get("createdOn")[i]
                break # If latestDate comes from a base truck work order than use that one

        index += 1
    
    lastMinorBaseTruck = parser.isoparse(lastMinorBaseTruck)

    # Gets the latest upload made by this system
    if(lastMinorBaseTruck < lastMajorBaseTruck):
        latestFlukeUpload = lastMajorBaseTruck
    else:
        latestFlukeUpload = lastMinorBaseTruck


    # Checks if the new data has already been processed
    filter_data = []
    for report in inspection_data:
        motiveTime = parser.isoparse(report["date"])
        
        if(motiveTime > latestFlukeUpload): # if motive inspection report time comes after the latest date from fluke
            filter_data.append(report)

    return filter_data


def getMotiveData() -> list:
    """
    Gets the data of inspection reports within the last day from motive API and returns the filtered data. Filtered data is ones with a issue to request a work order for and that have not already been posted to fluke. 

    Returns:
        list: List of inspection reports that have been filtered for new issues that must be posted to fluke
    """

    # Gets all of the issues within the past 24 hours
    index = 1
    issues = []
    # Can only go to the 5th page from motive
    while index <= 5: 
        # end point for motive's truck status data, gets most recent inspection report
        motive = f"https://api.keeptruckin.com/v2/inspection_reports?per_page=50&page_no={index}"

        # get truck status data
        response = requests.get(motive, headers=motive_headers)

        if response.status_code != 200:
            print("Error getting Motive Data", flush=True)
            return False

        new_issues = filterIssues(response.json())

        issues = issues + new_issues

        try: 
            # Gets the time from the earliest inspection report in that batch 
            time = str(response.json()['inspection_reports'][-1]['inspection_report']['time'])
            time = datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except:
            break

        # Get current time in UTC
        now = datetime.now(timezone.utc)
        past_24_hours = now - timedelta(days=1)

        # Check if the given time is within the past 24 hours
        # Greater than represents more recent (further in the future is bigger)
        if past_24_hours > time:
            break
            
        try: 
            print(str(len(issues)) + " " + str(index) + " " + str(response.json()['inspection_reports'][0]['inspection_report']['time']))
        except:
            break
        index += 1

    # Makes sure the data is new compared to last uploaded fluke data
    if checkData or production:
        data = checkNewData(issues)
    else: 
        data = issues

    # data = issues
    if data == False:
        return False

    return data


def convertToPost(data: list, df) -> list: 
    """
    Converts filtered data from motive to a format that can be posted to fluke api
    
    Args:
        data (list): List of inspection reports that have been filtered for new issues that must be posted to fluke

    Returns:
        list: List of inspection reports that have been converted to a format that can be posted to fluke api
    """

    # Gets id of the truck or trailer
    def getAssetId(post):
        # Holder for the asset sent to work order
        assetId = {}
        c_compid = ""

        try:  
            if post['vehicle']['number'].split(" ")[0] == "White":
                post['vehicle']['number'] = post['vehicle']['number'].split(" ")[2]

            truckId = None
            index = df[df['c_description'].str.contains(post['vehicle']['number'])].index.tolist()
            truckId = df.loc[index[0]]['id']

            if truckId == None:
                print(f'Error: This is not a valid truck in fluke. Ending this post. {post}', flush=True)
                return (False, False)

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
            

        except Exception as err:

            try:
                trailerId = None

                index = df[df['c_description'].str.contains(post['asset']['name'])].index.tolist()
                trailerId = df.loc[index[0]]['id']

                if trailerId == None:
                    print(f'Error: This is not a valid trailer in fluke. Ending this post. {post}', flush=True)
                    return (False, False)

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
                print("Error: Could not process the asset of: " + str(post), flush=True)
                return (False, False)
        
        return (assetId, c_compid)
    

    # Converts the description and notes 
    def getDescriptionAndNotes(post):
        description = []
        notes = []

        for issue in post['issues']:

            if(issue['notes'] == ''):
                adding = 'No comments noted by driver.'
            else:
                adding = f"{issue['notes']}"

            if issue['priority'] == 'major': # puts the major issue first in the description
                description.insert(0, issue['category'])
                notes.insert(0, 'Major Issue: ' + adding)
            else:
                description.append(issue['category'])
                notes.append('Minor Issue: ' + adding)

        description =  ", ".join(f"{i+1}. {desc}" for i, desc in enumerate(description)) if len(description) != 1 else description[0]

 
        if 'major' in notes[0].lower():
            details = f'<b>{post["inspection_type"]} Inspection:</b><br>' + (";<br>".join(f"{i+1}. {desc}" for i, desc in enumerate(notes)))
        else:
            details = f'<b>Motive Base Truck - {post["inspection_type"]} Inspection:</b><br>' + ("<br>".join(f"{i+1}. {desc}" for i, desc in enumerate(notes)))

        return (description, details)

    # Creates the new work order payload
    def createWorkOrder(post):
        assetId, compid = getAssetId(post)

        # If there is no asset associated with the work order then do not post it
        if not assetId:
            return (False, False)
        
        description, details = getDescriptionAndNotes(post)

        if 'major' in details.lower():
            isRequest = False
        else:
            isRequest = True

        if production: 
            work_order_type = {
                "entity": "WorkOrderTypes",
                "id": "ad127a5d-38d8-40ad-9eb0-882abcdde551",
                "isDeleted": False,
                "number": 24,
                "title": "Motive Base truck Corrective"
            }
        else:
            work_order_type = {
                "entity": "WorkOrderTypes",
                "id": "f04406fe-847e-4d49-899e-0053758d7fc3",
                "isDeleted": False,
                "number": 24,
                "title": "Motive Base Truck Corrective",
            }
        job_status = {
            "entity": "JobStatus",
            "id": "11111111-8588-40d2-b33d-111111111113",
            "isDeleted": False,
            "number": 3,
            "title": "New",
        }
        priority = {
            "entity": "PriorityLevels",
            "id": "954c61fe-6f07-4c5c-8de4-b72594321c42",
            "isDeleted": False,
            "number": 6,
            "title": "Base Truck Blocking",
        }

        base_payload = {
            "properties": {
                "assetId": assetId,
                "description": description,
                "details": details,
                "createdBy": {
                    "entity": "UserData",
                    "id": "00000000-0000-0000-0000-000000000002",
                    "number": 0,
                    "title": f"{post['driver']['last_name'].title()} {post['driver']['first_name'].title()}",
                },
                "c_requesteremail": post["driver"]["email"],
                "c_compid": compid,
            }
        }

        # Should go to work orders requests
        if isRequest:
            base_payload["properties"]["formId"] = 7
            base_payload["properties"]["c_requestedOn"] = post["date"]
        else:
            base_payload["occurredOn"] = post['date']
            base_payload['properties'].update({'c_priority': priority, 'c_jobstatus': job_status, 'c_workordertype': work_order_type})
        
        motiveId = post['id']

        return (base_payload, motiveId)

    # The motive issues converted to fluke payloads
    converted_data = []
    
    # For every truck that needs a post
    for post in data: 
        post_data, motiveId = createWorkOrder(post)

        if(post_data != False):
            converted_data.append([post_data, motiveId])

    return converted_data


def postWorkOrders(data: list) -> list:
    """
    Posts the work orders to fluke api and returns the responses

    Args:
        data (list): List of inspection reports that have been converted to a format that can be posted to fluke api

    Returns:
        list: List of responses from the post requests
    """


    def giveExternalId(inspectionReportId, inspectionReportDay, externalId):
        # Need ID and Date of the inspection report
        url = f"https://api.gomotive.com/v2/inspection_reports/{inspectionReportId}?time={inspectionReportDay}"

        payload = {
            "external_ids_attributes": [
            {
                "external_id": externalId,
                "integration_name": "Fluke"
            }
            ]
        }

        response = requests.put(url, json=payload, headers=motive_headers)

        # print(response.text, flush=True)


    # Config
    woEndpoint = f"https://{tenant}/api/entities/{site}/WorkOrders"
    worEndpoint = f"https://{tenant}/api/entities/{site}/WorkOrdersRequests"

    responses = []
    # Send a post request with the data
    for work_order in data:

        endpoint = ''

        # Check if it should go to work order requests or work order
        
        try:
            if work_order[0]['properties']['details'][0:20] != "<b>Motive Base Truck":
                endpoint = woEndpoint
                
                response = requests.post(endpoint, headers=headers, data=json.dumps(work_order[0]))
                responses.append(response)

                giveExternalId(work_order[1], work_order[0]['occurredOn'], response.json()['id'])
            else:
                endpoint = worEndpoint

                response = requests.post(endpoint, headers=headers, data=json.dumps(work_order[0]))
                responses.append(response)

                giveExternalId(work_order[1], work_order[0]['properties']['c_requestedOn'], response.json()['id'])
        except:
            print("Error posting work order", flush=True)
            print("Payload Data: " + str(work_order[0]), flush=True)
            print("Motive ID: " + str(work_order[1]), flush=True)
            continue


    return responses


def main():
    """
    Main loop that checks for new inspection reports from motive and posts them to fluke (or saves them to a csv file during testing)
    """

    # Get all of the assets
    df = getFreightlinersAndTrailers()

    # Makes sure a dataframe is returned, and an error did not happen
    try: 
        if df == False:
            return
    except:
        pass

    # Does not take much time at all; not bad to call it twice => if we only call it after then getting asset ids everytime for no reason
    data = getMotiveData()

    # Only continue if Motive Data was gathered succesfully 
    if data == False:
        return

    # only continues if there is an inspection report to upload
    if len(data) == 0:
        print("No new data.", flush=True)
        return

    # converts the previous data list to a list that can be posted to fluke api
    WO_posts = convertToPost(data, df)

    # posts work orders to fluke and returns the responses
    responses = postWorkOrders(WO_posts)

    # All of the responses of uploaded work orders
    print(":notice: Inspection Report Found", flush=True)


if __name__ == "__main__":
    main()