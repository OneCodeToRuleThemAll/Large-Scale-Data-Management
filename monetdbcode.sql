create LOADER myloader(filename string) LANGUAGE PYTHON {
import pandas as pd
f = pd.read_csv(filename,sep=',')
_emit.emit({'title':f.title.to_list(),'city':f.city.to_list(),'state':f.state.to_list(),'postal_code':f.postal_code.to_list(),'price':f.price.to_list(),'facts and features':f['facts and features'].to_list(),'real estate provider':f['real estate provider'].to_list()})};

CREATE TABLE mytable FROM LOADER myloader(r'C:\Users\Nikolaos\Desktop\zillow.csv');

CREATE OR REPLACE FUNCTION get_baths(i STRING)
RETURNS INTEGER
LANGUAGE PYTHON
{result=[]
 for j in range(0,len(i)):
          if i[j].split(',')[1].split(' ')[1] != 'None':
              result.append(float(i[j].split(',')[1].split(' ')[1]))
          else:
           result.append(0)
 return result
};

CREATE OR REPLACE FUNCTION get_beds(i STRING) # for beds
RETURNS integer
LANGUAGE PYTHON
{result=[]
 for j in range(0,len(i)):
          if i[j].split(',')[0].split(' ')[0] != 'None':
              result.append(int(float(i[j].split(',')[0].split(' ')[0])))
          else:
           result.append(0)
 return result
};

CREATE OR REPLACE FUNCTION get_sqft(i STRING) # for sqft
RETURNS integer
LANGUAGE PYTHON
{result=[]
 for j in range(0,len(i)):
          if i[j].split(',')[2].split(' ')[0] != 'None':
              result.append(int(float(i[j].split(',')[2].split(' ')[0])))
          else:
           result.append(0)
 return result
};

CREATE OR REPLACE FUNCTION get_type(i STRING) # for type 
RETURNS string 
LANGUAGE PYTHON
{result=[]
 for j in range(0,len(i)):
  if 'condo' in i[j].lower():
   result.append('Condo')
  elif 'house' in i[j].lower():
   result.append('House')
  elif 'lot' in  i[j].lower():
   result.append('Lot/Land')
  elif 'construction' in i[j].lower():
   result.append('New Construction')
  elif 'townhouse' in i[j].lower():
   result.append('House')
  elif 'home' in i[j].lower():
   result.append('Home')
  else:
   result.append('Other Type')
 return result
};


CREATE OR REPLACE FUNCTION get_offer(i STRING) # for offer
RETURNS string 
LANGUAGE PYTHON
{result=[]
 for j in range(0,len(i)):
  if 'sale' in i[j].lower():
   result.append('Sale')
  elif 'rent' in i[j].lower():
   result.append('Rent')
  elif 'sold' in  i[j].lower():
   result.append('Sold')
  elif 'foreclosure' in i[j].lower():
   result.append('Foreclose')
  else:
   result.append('Other')
 return result
};

create table final as SELECT title,city,state,postal_code,price,"facts and features",get_baths("facts and features") as baths,get_beds("facts and features") as beds,get_sqft("facts and features") as Sqft,get_type("title") as type,get_offer("title") as offer from mytable;

create table filtered_types as select * from final where (offer = 'Sale');

CREATE OR REPLACE FUNCTION get_price(i STRING)
RETURNS integer 
LANGUAGE PYTHON
{result=[]
 for j in range(0,len(i)):
                result.append(int(float((i[j][1:].replace(',','').replace('+','').replace(' ','')))))
 return result
                };

CREATE TABLE test as SELECT title,city,state,postal_code,price,"facts and features",get_baths("facts and features") as baths,get_beds("facts and features") as beds,get_sqft("facts and features") as Sqft,get_type("title") as type,get_offer("title") as offer,get_price("price") as PriceValue from filtered_types;

create table final_filters as select *  from test where beds <=10 and (PriceValue > 100000) and (PriceValue < 20000000) and (type='House');


select avg(pricevalue/Sqft) as avg_price_per_sqft,beds from final_filters group by beds;