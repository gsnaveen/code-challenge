import unittest
from datetime import datetime
from cltv import Ingest

class testCases(unittest.TestCase):
    
    def test_order(self):
        
        D = {"data": {},"MaxDate":datetime.min,"MinDate":datetime.max}
        customer_id = '96f55c7d8f42'
        
        x = {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "12.34 USD"}
        Ingest(x,D)
        result = D["data"][customer_id]["ORDER"]["total_amount"]
        self.assertEqual(result,12.34)
        
        # For in order record with higher total_amount is updated
        x = {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-07T12:55:56.555Z", "customer_id": "96f55c7d8f42", "total_amount": "13.34 USD"}
        Ingest(x,D)
        result = D["data"][customer_id]["ORDER"]["total_amount"]
        self.assertEqual(result,13.34)
        
        # For in order record with less total_amount is updated
        x= {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-08T12:55:56.555Z", "customer_id": "96f55c7d8f42", "total_amount": "10.34 USD"}
        Ingest(x,D)
        result = D["data"][customer_id]["ORDER"]["total_amount"]
        self.assertEqual(result,10.34)
        
        # For out of order OLD record the total_amount is not updated
        x = {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-05T12:55:56.555Z", "customer_id": "96f55c7d8f42", "total_amount": "22.34 USD"}
        Ingest(x,D)
        result = D["data"][customer_id]["ORDER"]["total_amount"]
        self.assertNotEqual(result,22.34)
        
        
        
    def test_custinfo(self):
        
        D = {"data": {},"MaxDate":datetime.min,"MinDate":datetime.max}
        customer_id = '96f55c7d8f42'
        
        x = {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "12.34 USD"}
        Ingest(x,D)
        result = D["data"][customer_id]["CUSTOMER"]["last_name"]
        self.assertEqual(result,"")
        
        # For in order record with customer information is updated
        x = {"type": "CUSTOMER", "verb": "NEW", "key": "96f55c7d8f42", "event_time": "2017-01-06T12:46:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AK"}
        Ingest(x,D)
        result = D["data"][customer_id]["CUSTOMER"]["last_name"]
        self.assertEqual(result,"Smith")
        
        # For out order record is ot updated 
        x = {"type": "CUSTOMER", "verb": "NEW", "key": "96f55c7d8f42", "event_time": "2017-01-01T12:46:46.384Z", "last_name": "Smithx", "adr_city": "Middletown", "adr_state": "AK"}
        Ingest(x,D)
        result = D["data"][customer_id]["CUSTOMER"]["last_name"]
        self.assertNotEqual(result,"Smithx")
        
        
if __name__ == "__main__":
    unittest.main()