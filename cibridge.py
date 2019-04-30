from graphql_client import GraphQLClient
from graphql_http_client import query, mutation
import json

ws_url = "ws://localhost:8080/subscriptions"

"""
Uploads a file to cibridge and returns the data id.

param:file - Takes the path to the input file 
returns: data id of the file
"""
def upload_file(file):
    add_data_mutation = """
      mutation{
      uploadData(file:"%s", properties: null){
        id
        format
        name
        label
        parentDataId
        type
        isModified
      }
    }
    """%(file)
    data = json.loads(mutation(add_data_mutation).text)
    dataId = data['data']['uploadData']['id']
    return dataId

"""
Searches the converter algorithm id by taking the input and output data formats

param:in_data_format - input data format
param:out_data_format - output data format 
returns: algorithm id if available or None 
"""
def searchConverterAlgorithm(in_data_format, out_data_format):
    search_converter_algorithm = """
    query{
      findConvertersByFormat(inFormat:"%s", outFormat:"%s"){
        id
        inData
        outData
        label
        description
        parentOutputData
        type
        remoteable
        menuPath
        conversion
        authors
        implementers
        integrators
        documentationUrl
        reference
        referenceUrl
        writtenIn
      }
    }
    """%(in_data_format, out_data_format)
    data = json.loads(query(search_converter_algorithm).text)
    converter_id = data['data']['findConvertersByFormat'][0]['id']
    print("converted id algo: " + converter_id)
    return converter_id

"""
Fetches the array results given the output data format expected and the parent data id 

param: out_data_format - output data format expected
param: parentDataId - Parent Data Id 
returns: array of results with matching parentId 
"""
def getResultsDataId(out_data_format, parentDataId):
    q = """
    query{
      getData(filter:{ formats:["%s"]}){
        results{
          id
          format
          parentDataId
        }
      }
    }
    """%(out_data_format)
    queryResults = json.loads(query(q).text)
    dataIds = []
    for i in queryResults["data"]["getData"]["results"]:
        if i["parentDataId"] == parentDataId:
            dataIds.append(i["id"])
    return dataIds

"""
Fetches the result given the output data format expected and the parent data id

param: out_data_format - output data format
param: parentDataId - Parent Data Id 
returns: reault Data Id
"""
def getResultDataId(out_data_format, parentDataId):
    q = """
    query{
      getData(filter:{ formats:["%s"]}){
        results{
          id
          format
          parentDataId
        }
      }
    }
    """%(out_data_format)
    queryResults = json.loads(query(q).text)
    dataId = None
    for i in queryResults["data"]["getData"]["results"]:
        if i["parentDataId"] == parentDataId:
            dataId = i["id"]
    return dataId

"""
Creates an instance of the algorithm given the algorithm_id and the data_id

param: converter_id - algorithm id 
param: parentDataId - data id for the algorithm 
returns: instance id of the algorithm created 
"""
def createAlgorithmInstance(converter_id, dataId):
    create_converter_algorithm_instance = """
    mutation{
      createAlgorithm(algorithmDefinitionId:"%s",
      dataIds:["%s"],
      ){
        id
      }
    }
    """%(converter_id, dataId)
    data = json.loads(mutation(create_converter_algorithm_instance).text)
    converter_algo_instance = data["data"]["createAlgorithm"]["id"]
    print("Converter Algorithm Instance: " + converter_algo_instance)
    return converter_algo_instance

"""
Creates an instance of the algorithm given the algorithm_id, data_id and the parameters

param: converter_id - algorithm id 
param: parentDataId - data id for the algorithm 
param: parameters - parameters for the algorithm 
returns: instance id of the algorithm created 
"""
def create_algorithm_withParams(algorithmId, dataId, parameters):
    create_coauthor_algorithm_instance="""
    mutation{
      createAlgorithm(algorithmDefinitionId:"%s",
      dataIds:["%s"],
      parameters:[%s]
      ){
        id
      }
    }
    """%(algorithmId, dataId, parameters)

    print("Query: " + create_coauthor_algorithm_instance)
    responseId = json.loads(mutation(create_coauthor_algorithm_instance).text)
    print(responseId)
    coauthor_algo_instance = responseId["data"]["createAlgorithm"]["id"]
    print("Algorithm Instance: " + coauthor_algo_instance)
    return coauthor_algo_instance

"""
Runs an algorithm instance given the algorithm_instance_id.

param: converter_algo_instance - algorithm instance that has to be scheduled 
returns: runs the algorithm and returns the run result
"""
def runAlgorithm(converter_algo_instance):
    run_converter_algorithm = """
    mutation{
      runAlgorithmNow(algorithmInstanceId:"%s")
    }
    """%(converter_algo_instance)
    data = json.loads(mutation(run_converter_algorithm).text)
    return data

"""
Creates a subscription to the algorithm given the algorithm_instance and reference to the callback method.

param: converter_algo_instance - instance of the algorithm for which the results are expected 
param: callbackMethod - reference to the callback method 
returns: subscription id created
"""
def subscribe_to_algorithm(converter_algo_instance, callbackMethod):
    converter_algo_status_subscription = """
    subscription{
      algorithmInstanceUpdated(filter:{
        algorithmInstanceIds: "%s"
      }){
        id
        progress
        state
      }
    }
    """%(converter_algo_instance)
    global ws
    ws = GraphQLClient(ws_url)
    convereter_algo_subscription = ws.subscribe(converter_algo_status_subscription, callback=callbackMethod)
    return convereter_algo_subscription

"""
Gets the local path to download result file given the dataId
 
param: dataId - data id 
returns: local path to download
"""
def download_data(dataId):
    download_query = """
    query{
      downloadData(dataId:"%s")
    }
    """%(dataId)
    data = json.loads(query(download_query).text)
    return data["data"]["downloadData"]