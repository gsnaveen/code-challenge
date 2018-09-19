import json,heapq
from datetime import datetime

def Ingest(e, D):
    
    # Ingest function for adding data to the data structure
    
    if e["type"] == "CUSTOMER":
        theKey = e["key"]
    else:
        theKey = e["customer_id"]
        if theKey == "":
            print("Not a valid customer id : "  + e)
            return 
    
    #Getting the event time into a variable and computing min and max date for the dataset
    evantDtt = datetime.strptime(e["event_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
    D["MaxDate"] = max(evantDtt,D["MaxDate"])
    D["MinDate"] = min(evantDtt,D["MinDate"])
    
    #Since records can be recieved in any order we are adding customerID to the datastructure for not missing the event
    if theKey not in D["data"].keys():
        
        D["data"][theKey] = { "CUSTOMER" :{"last_name":"" , "adr_city": "","adr_state":"","event_time":datetime.min,"joinDate": datetime.max},
                            "SITE_VISIT":0 ,
                            "IMAGE":0,
                            "ORDER":{"total_amount":0,"order_count":0},
                            "ORDER_DETAIL" : {}
                             }
    
    if e["type"] == "CUSTOMER":         #verb  NEW   UPDATE
            # event time drive if the data needs to be updated.
            # record will not be updated If new timestamp is older than the exisiting record
            if evantDtt > D["data"][theKey]["CUSTOMER"]["event_time"]:
                D["data"][theKey]["CUSTOMER"]["last_name"] = e["last_name"]
                D["data"][theKey]["CUSTOMER"]["adr_city"] = e["adr_city"]
                D["data"][theKey]["CUSTOMER"]["adr_state"] = e["adr_state"]
                D["data"][theKey]["CUSTOMER"]["event_time"] = evantDtt
                D["data"][theKey]["CUSTOMER"]["joinDate"] = min(D["data"][theKey]["CUSTOMER"]["joinDate"],evantDtt)
            else:
                #If the evantDtt is < than the current event date then the join Date is update for LTV computation
                D["data"][theKey]["CUSTOMER"]["joinDate"] = min(D["data"][theKey]["CUSTOMER"]["joinDate"],evantDtt)
            
    elif e["type"] == "SITE_VISIT":     #verb NEW
        #update the number of visit counter 
        D["data"][theKey]["SITE_VISIT"] += 1
        
    elif e["type"] == "IMAGE":          #verb UPLOAD
        #update the number of image upload counter
        D["data"][theKey]["IMAGE"] += 1
        
    elif e["type"] == "ORDER":          #verb NEW   UPDATE
        
        #take only the numeric value in the total_amount e.g "12.34 USD", 12.34 will be saved to the datastructure
        try:
            total_amount = float(e["total_amount"].split(" ")[0])
        except:
            print("Order total amount not valid :" +e)
            return 
        
        if e["key"] not in D["data"][theKey]["ORDER_DETAIL"].keys():
            #If this order is recieved for the first time. the order Key , total_amount and event date time is saved to the data structure
            D["data"][theKey]["ORDER_DETAIL"][ e["key"]] = (total_amount,evantDtt)
            D["data"][theKey]["ORDER"]["order_count"] += 1
            D["data"][theKey]["ORDER"]["total_amount"] += total_amount
        elif evantDtt > D["data"][theKey]["ORDER_DETAIL"][ e["key"]][1] :
                #if the event date is greater than saved record ORDER total_amount is updated 
                D["data"][theKey]["ORDER"]["total_amount"] =  D["data"][theKey]["ORDER"]["total_amount"] \
                                                                    + (total_amount - D["data"][theKey]["ORDER_DETAIL"][ e["key"]][0])
                    
                #Order detail record is pdated based on the event key of the ORDER event
                D["data"][theKey]["ORDER_DETAIL"][ e["key"]] = (total_amount,evantDtt)
 
    else:
        print("Data not mapped! : " + e)
        return


def TopXSimpleLTVCustomers(x, D):
    
    TopLTVbyValue = []
    ComputedValue = {} # Key = LTV with HASH Value is List of customers with the same value
    xRunner = 0
    t = 10
    
    LastdataDate = D["MaxDate"]
    MindataDate = D["MinDate"]
    CustList = D["data"].keys()
    
    for customer_id in CustList:
        
        # If orders > 0
        # If order amount > 0
        # customer information 
        # Visits > 0 
        # as these elemets are required for computing Customer LTV
        
        if  D["data"][customer_id]["ORDER"]["order_count"] > 0 and \
            D["data"][customer_id]["ORDER"]["total_amount"] > 0 and \
                D["data"][customer_id]["CUSTOMER"]["joinDate"] >= MindataDate and \
                D["data"][customer_id]["CUSTOMER"]["joinDate"] <= LastdataDate and \
                D["data"][customer_id]["SITE_VISIT"] > 0:
            
            #Total Order Amount
            orderedAmount = D["data"][customer_id]["ORDER"]["total_amount"]
            #Total Number of Visits
            NumberOfVisits = D["data"][customer_id]["SITE_VISIT"]
            #Visits Per week
            weeks = ((LastdataDate - D["data"][customer_id]["CUSTOMER"]["joinDate"]).days /7)
            
            #For the scenario where lasteDate of dataset and the customer join date is the same
            if weeks == 0 : weeks = 1
            VisitsPerWeek = NumberOfVisits / weeks
            
            #Computing LTV
            value = (52 * ((orderedAmount / NumberOfVisits)* VisitsPerWeek )) * t
            
            #If the value does not exist then add to the HASH and add customerid as a list value
            if value not in ComputedValue.keys():
                ComputedValue[value] = [customer_id]
            else:
                ComputedValue[value].append(customer_id)
                
            #for the first x values add to the heap    
            if xRunner < x:
                heapq.heappush(TopLTVbyValue,value)
                xRunner += 1
            else:
                # after x values are added pop the push the value into the heap and pop the lowest value.
                # This will ensure we have x top values in th heap
                 heapq.heappushpop(TopLTVbyValue,value)
    
    xRunner = 0                
    OutFile = open("../output/output.txt", "w")
    
    #Sort the top values in decending order 
    #there is a possibility of multiple customers having same LTV
    gotTopX = False
    for topCust in sorted(set(TopLTVbyValue),reverse = True):
        for CustId in ComputedValue[topCust]:
            if xRunner < x:
                OutFile.write(CustId + "\n")
                xRunner += 1
            else:
                # If top customer_ids written then break the for loop
                gotTopX = True
                break
                
        if gotTopX: 
            # If top customer_ids written then break the for loop
            break

    OutFile.close()  
    
    if xRunner == 0:
        print("no data returned")
        
if __name__ == "__main__":

    D = {"data": {},"MaxDate":datetime.min,"MinDate":datetime.max}
    
    with open('../input/input.txt') as x: inData = x.read()
    toProcessData = json.loads(inData)
    
    #Adding data to the data structure
    for x in toProcessData:
        Ingest(x,D)
        
    #Computing TopX
    x = 1
    TopXSimpleLTVCustomers(x, D)