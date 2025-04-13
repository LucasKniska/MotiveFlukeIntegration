import requests
import json

# Cookie to the sandbox
sandbox_key = "JWT-Bearer=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiI5NWZkYzZhYS0wOWNiLTQ0NzMtYTIxZC1kNzBiZTE2NWExODMiLCJ0aWQiOiJUb3JjUm9ib3RpY3NTQiIsImV4cCI6NDEwMjQ0NDgwMCwic2lkIjpudWxsLCJpaWQiOm51bGx9.94frut80sKx43Cm4YKfVbel8upAQ8glWdfYIN3tMF7A"

# Environment variables from GitHub
motive_key = "9e90504a-82f0-4ed4-b54c-ce37f388f211"

headers = {
    "Content-Type": "application/json", 
    "Cookie": sandbox_key
}

motive_headers = {
    "accept": "application/json", 
    "X-Api-Key": motive_key
}

# FIND All newly completed/closed WO(R)
def filterMinorsFromMotive(inspectionReports):

    filtered = []

    for report in inspectionReports['data']:
        
        try:
            if report['details'][3:20] == "Motive Base Truck":
                filtered.append(report)
        except:
            pass

    return filtered

def findCompletedWorkOrdersAndRequests():
    # Completed Work Orders Section
    url = 'https://torcroboticssb.us.accelix.com/api/entities/def/WorkOrders/search-paged'

    # Cookie to the sandbox
    data = {
        'select': 
            [{'name': 'id'}, {'name': 'closedOn'}, {'name': 'updatedBy'}, {'name': 'c_priority'}, {'name': 'assetId'}, {'name': 'c_maintenancelog'}, {'name': 'status'}], 
        'filter': {
            'and': [
                {"name": "c_workordertype", "op": "eq", "value": "Motive Base Truck Corrective"},
                {"name": "status", "op": "eq", "value": "H"},
            ],
        }, 
        'order': [{'name': 'number', 'desc': True}], 'pageSize': 50, 'page': 0, 'fkExpansion': True
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("Major Work Orders: ", response.status_code)

    # Checks status from major issues 
    try:
        MajorReportStatus = response.json()['data']
    except:
        MajorReportStatus = []

    # Getting work order requests statuses
    # Completed Work Orders Section
    url = 'https://torcroboticssb.us.accelix.com/api/entities/def/WorkOrders/search-paged'

    # Cookie to the sandbox
    data = {
        'select': 
            [{'name': 'id'}, {'name': 'closedOn'}, {'name': 'updatedBy'}, {'name': 'openedOn'}, {'name': 'c_priority'}, {'name': 'assetId'}, {'name': 'status'}, {'name': 'details'}, {'name': 'requestId'}], 
        'filter': {
            'and': [
                {"name": "status", "op": "eq", "value": "H"},
            ],
        }, 
        'order': [{'name': 'number', 'desc': True}], 'pageSize': 50, 'page': 0, 'fkExpansion': True
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("Minor Work Orders ", response.status_code)

    response = filterMinorsFromMotive(response.json())

    # Checks status from major issues 
    MinorReportStatus = response


    # Getting work order requests statuses
    # Completed Work Orders Section
    url = 'https://torcroboticssb.us.accelix.com/api/entities/def/WorkOrdersRequests/search-paged'

    # Cookie to the sandbox
    data = {
        'select': 
            [{'name': 'id'}, {'name': 'requestedOn'}, {'name': 'assetId'}, {'name': 'status'}, {'name': 'details'}], 
        'filter': {
            'and': [
                {"name": "status", "op": "eq", "value": "X"},
            ],
        },
        'order': [{'name': 'number', 'desc': True}], 'pageSize': 50, 'page': 0, 'fkExpansion': True
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    print("Minor Work Orders ", response.status_code)

    response = filterMinorsFromMotive(response.json())
    MinorWOR = response

    return {
        "MajorWO": MajorReportStatus,
        "MinorWO": MinorReportStatus,
        "WOR": MinorWOR,
    }
    

def getByExternalId(externalId):
    url = "https://api.gomotive.com/v2/inspection_reports/lookup_by_external_id"
    params = {
        "external_id": externalId,
        "integration_name": "Fluke"
    }

    response = requests.get(url, headers=motive_headers, params=params)

    if response.status_code != 200:
        return False

    return response.json()


def lookForClosedWO(currentWO):
    motiveData = []

    for wo in currentWO['WOR']:
        if(wo['status'] == "X"):
            print("Minor Work Order Request Rejected: ", wo)

            data = getByExternalId(wo['id'])

            if data == False:
                print("NO data found for: ", wo['id'])
                continue

            data = {
                "log_id": data['inspection_report']['log_id'],
                'date': data['inspection_report']['date'],
                'inspected_parts': [part['id'] for part in data['inspection_report']['inspected_parts']],
                'closedOn': data['inspection_report']['date'],
                'mechanic_note': 'Rejected',
                'name': 'Automatic'
            }

            motiveData.append(data)

    for wo in currentWO['MinorWO']:
        if(wo['status'] == "H"):
            print("Minor Work Order Closed: ", wo)
            print("   Request Id: ", wo['requestId'])

            data = getByExternalId(wo['requestId']['id'])

            if data == False:
                print("NO data found for: ", wo['id'])
                continue

            data = {
                "log_id": data['inspection_report']['log_id'],
                'date': data['inspection_report']['date'],
                'inspected_parts': [part['id'] for part in data['inspection_report']['inspected_parts']],
                'closedOn': wo['closedOn'],
                'mechanic_note': wo['c_maintenancelog'],
                'name': wo['updatedBy']['title']
            }

            motiveData.append(data)

    
    for wo in currentWO['MajorWO']:
        if(wo['status'] == "H"):
            print("Major Work Order Closed: ", wo)

            data = getByExternalId(wo['id'])

            if data == False:
                print("NO data found for: ", wo['id'])
                continue

            data = {
                "log_id": data['inspection_report']['log_id'],
                'date': data['inspection_report']['date'],
                'inspected_parts': [part['id'] for part in data['inspection_report']['inspected_parts']],
                'closedOn': wo['closedOn'],
                'mechanic_note': wo['c_maintenancelog'],
                'name': wo['updatedBy']['title']
            }

            motiveData.append(data)
    
    return motiveData

def resolveInspectionReport(data):
  # Need ID and Date of the inspection report
  url = f"https://api.gomotive.com/v2/inspection_reports/{data['log_id']}?time={data['date']}"

  payload = {
    "defect_statuses": {
      "resolved_defects": data['inspected_parts'],
      "mechanic_signed_at": data['closedOn'],
      "resolver_id": 4288195, # Carlas resolver id
      "mechanic_name": data['name'],
      "mechanic_note": data['mechanic_note'],
      "status": "repaired"
    }
  }

  response = requests.put(url, json=payload, headers=motive_headers)
  print(response.text)

if __name__ == "__main__":
    # Current Work orders and requests
    currentWO = findCompletedWorkOrdersAndRequests()

    # Gets the id of the motive Inspection Report
    for key in currentWO:
        if isinstance(currentWO[key], dict):
            currentWO[key] = [currentWO[key]]

    motiveData = lookForClosedWO(currentWO)

    for inspectionReport in motiveData:
        resolveInspectionReport(inspectionReport)

