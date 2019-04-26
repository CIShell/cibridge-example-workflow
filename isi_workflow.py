from graphql_client import GraphQLClient
import requests
import json

http_url = "http://localhost:8080/graphql"
ws_url = "ws://localhost:8080/subscriptions"


def query(query, variables=None, headers=None):
    payload = {'headers': headers, 'query': query, 'variables': variables}
    resp = requests.post(http_url, json=payload)
    return resp


def mutation(query, variables=None, headers=None):
    payload = {'headers': headers, 'query': query, 'variables': variables}
    resp = requests.post(http_url, json=payload)
    return resp


def uploadIsiFile(file):
    add_data_mutation = """
      mutation{
      uploadData(file:"%s", properties: null){
        id
      }
    }
    """%(file)
    data = json.loads(mutation(add_data_mutation).text)
    dataId = data['data']['uploadData']['id']
    return dataId

def searchConverterAlgorithm(in_data_format, out_data_format):
    search_converter_algorithm = """
    query{
      findConvertersByFormat(inFormat:"%s", outFormat:"%s"){
        id
      }
    }
    """%(in_data_format, out_data_format)
    data = json.loads(query(search_converter_algorithm).text)
    converter_id = data['data']['findConvertersByFormat'][0]['id']
    print("converted id algo: " + converter_id)
    return converter_id

def getResultDataId(out_data_format):
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
    dataId = queryResults["data"]["getData"]["results"][0]["id"]
    return dataId

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
    data = json.loads(mutation(create_coauthor_algorithm_instance).text)
    coauthor_algo_instance = data["data"]["createAlgorithm"]["id"]
    print("Algorithm Instance: " + coauthor_algo_instance)
    return coauthor_algo_instance

def runAlgorithm(converter_algo_instance):
    run_converter_algorithm = """
    mutation{
      runAlgorithmNow(algorithmInstanceId:"%s")
    }
    """%(converter_algo_instance)
    data = json.loads(mutation(run_converter_algorithm).text)
    return data

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

def download_data(dataId):
    download_query = """
    query{
      downloadData(dataId:"%s")
    }
    """%(dataId)
    data = json.loads(query(download_query).text)
    return data["data"]["downloadData"]

def clusterAlgoCallback(_id, data):
    print("Cluster Algorithm CallBack " + json.dumps(data))

def converter2CallBack(_id, data):
    print("Converter2 CallBack " + json.dumps(data))
    if (data["id"] == converter2SubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'FINISHED'):  # DownloadingData
        dataId = getResultDataId("edu.uci.ics.jung.graph.Graph")
        parameters = """
        {
            key: "n"
            value: "1"
        }
        """
        clusterAlgoInstance = create_algorithm_withParams("edu.iu.nwb.analysis.weakcomponentclustering.ClusteringAlgorithm", dataId, parameters)
        global clusterAlgoSubscriptionId
        clusterAlgoSubscriptionId = subscribe_to_algorithm(clusterAlgoInstance, clusterAlgoCallback)
        runAlgorithm(clusterAlgoInstance)
    elif (data["id"] == converter2SubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'ERRORED'):
        print("Algorithm Errored")
        # ws.stop_subscribe(_id)

def coauthorCallBack(_id, data):
    print("Co Author Call Back " + json.dumps(data))
    if (data["id"] == coAuthor1SubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'FINISHED'):
        converted_id = searchConverterAlgorithm("prefuse.data.Graph","edu.uci.ics.jung.graph.Graph")
        dataId = getResultDataId("prefuse.data.Graph")
        converter2_instance = createAlgorithmInstance(converted_id, dataId)
        print("converter2_instance: "+ converter2_instance)
        global converter2SubscriptionId
        converter2SubscriptionId = subscribe_to_algorithm(converter2_instance, converter2CallBack)
        runAlgorithm(converter2_instance)
        # ws.stop_subscribe(_id)
    elif (data["id"] == coAuthor1SubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'ERRORED'):
        print("Algorithm Errored")
        # ws.stop_subscribe(_id)

def converter1callback(_id, data):
  print("Converter 1 CallBack: " + json.dumps(data))
  if (data["id"] == converter1SubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'FINISHED'):#DownloadingData
      resultDataId = getResultDataId("prefuse.data.Table")
      print("Output Data Id: " + resultDataId)
      parameters="""
      {
        key: "fileFormat"
        value: "isi"
      }
      """
      coauthor_algo_instance = create_algorithm_withParams("edu.iu.nwb.analysis.extractcoauthorship",resultDataId, parameters)
      print("coauthor_algo_instance: " + coauthor_algo_instance)
      global coAuthor1SubscriptionId
      coAuthor1SubscriptionId = subscribe_to_algorithm(coauthor_algo_instance, coauthorCallBack)
      print("check this: "+coAuthor1SubscriptionId)
      runAlgorithm(coauthor_algo_instance)
      # print("check this"+_id)
      # ws.stop_subscribe(converter1SubscriptionId)
  elif (data["id"] == converter1SubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'ERRORED'):
      print("Algorithm Errored")
      # ws.stop_subscribe(converter1SubscriptionId)

if __name__ == '__main__':

    # Uploading Data
    filepath = 'C:/Users/abharath/Downloads/sci2/sampledata/scientometrics/isi/FourNetSciResearchers.isi'
    dataId = uploadIsiFile(filepath)

    # Finding COnverter Algorithm
    in_data_format = 'file:text/isi'
    out_data_format = 'prefuse.data.Table'
    converter_id = searchConverterAlgorithm(in_data_format, out_data_format)

    # Creating an Instance of Algorithm with data id
    converter_algo_instance = createAlgorithmInstance(converter_id, dataId)

    # Sunscribing to alerts of algorithm
    converter1SubscriptionId = subscribe_to_algorithm(converter_algo_instance, converter1callback)

    # Running an algorithm
    runAlgorithm(converter_algo_instance)

    global coAuthor1SubscriptionId
    global converter2SubscriptionId
    global clusterAlgoSubscriptionId
    global ws




