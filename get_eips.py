import json
import boto3
import os

def get_config_response(query,next_token):
    client = boto3.client('config')
    try:
        if next_token!=None: # for handling data over 50 records
    	    response = client.select_aggregate_resource_config(
    	    Expression=query,
    	    ConfigurationAggregatorName='<Aggregator-Name>',
        	Limit=50,
        	NextToken=next_token
    	    )
    	    return response
        else:
            response = client.select_aggregate_resource_config(
    	    Expression=query,
    	    ConfigurationAggregatorName='<Aggregator-Name>',
        	Limit=50
    	    )
            return response
    except Exception as e:
	    print(e)

def process_records(results,header):
    print(results)
    master_data = []
    for result in results:
        data_lines = []
        tag=''
        for i in header:
            if '.' in i:
                x = i.split(".")
                try:
                    key = result[x[0]]
                    if isinstance(key,dict):	
                        data_lines.append(key[x[1]])
                    elif isinstance(key,list):
                        try:
                            data_lines.append(key[0][x[1]])  #NetworkInterface ID
                            data_lines.append(key[1][x[1]]) #Instance ID
                            tag = process_tags(key[1][x[1]])
                        except IndexError:
                            tag = process_tags(key[0][x[1]]) #resource list for tags
                            data_lines.append("Not Attached")
                except KeyError as e:
                    if 'relationships' in i:
                        data_lines.append("")
                        data_lines.append("Not Attached")
                    else:
                        data_lines.append("")
            else:
                if 'accountId' in i:
                    result[i] = '\''+result[i]
                try:
                    data_lines.append(result[i])
                except KeyError:
                    data_lines.append("")
        if tag!='':
            data_lines.append(tag)
        master_data.append(data_lines)
        
    return master_data

def process_tags(tag):
    	select_query = 'select tags where resourceId = \'' + tag + '\''
    	print(select_query)
    	response_tags = get_config_response(select_query,None)
    	return(response_tags["Results"][0].replace(",",";").replace("\\u003d","="))

def lambda_handler(event, context):
    eip_select_query = 'select resourceId,resourceName,configuration.privateIpAddress,accountId,relationships.resourceId,configuration.associationId where resourceType = \'AWS::EC2::EIP\' order by accountId'
    response = get_config_response(eip_select_query,None)
    results = [json.loads(response["Results"][i]) for i in range(len(response["Results"]))]
    while ("NextToken" in response):
	    next_token = response["NextToken"]
	    print(next_token)
	    response = get_config_response(eip_select_query,next_token)
	    results += [json.loads(response["Results"][i]) for i in range(len(response["Results"]))]
    header = []
    fields = response["QueryInfo"]["SelectFields"]
    for field in fields:
        header.append(field["Name"])        
    master_data = process_records(results,header)
    headers = ["EIP ID","External IP","Internal IP","AccountID", "NetworkInterfaceID","Instance ID","AssociationID", "Tags"]
    with open('/tmp/output.csv','w') as f_out:
        f_out.write("|".join(headers) + "\n")
        for data in master_data:
            f_out.write("|".join(data) + "\n")
            
    #Post to s3 function and script
    s3drive = os.environ.get('S3Storage')
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('/tmp/output.csv',s3drive,"eiplist/eip_list.csv") 
    #s3.meta.client.upload_file('/tmp/output.csv','utd-oit-cloudteam-a-304056624135-temp','eip_list.csv') TBD
    return {
        'statusCode': 200,
        'body': json.dumps('Success from Lambda! Eip file generated')
    }

