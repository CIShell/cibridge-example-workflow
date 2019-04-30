from cibridge import *

import json
import time
import shutil
import sys
import os

def converter5CallBack(_id, data):
    print("Converter5 Algorithm CallBack " + json.dumps(data))
    if (data["id"] == converter5SubscriptionId and data['payload']['data']['algorithmInstanceUpdated'][
        'state'] == 'FINISHED'):  # DownloadingData
        converter5DataId = getResultDataId("file:text/csv", graphTableDataId[0])
        path = download_data(converter5DataId)
        time.sleep(2)
        print("Converter 5 File: " + path)
        try:
            shutil.move(path, res2)
        except:
            pass
        print("done")
        os._exit(1)

    elif (data["id"] == converter5SubscriptionId and data['payload']['data']['algorithmInstanceUpdated'][
        'state'] == 'ERRORED'):
        print("Algorithm Errored")

def converter4CallBack(_id, data):
    print("Converter4 Algorithm CallBack " + json.dumps(data))
    if (data["id"] == converter4SubscriptionId and data['payload']['data']['algorithmInstanceUpdated'][
        'state'] == 'FINISHED'):  # DownloadingData
        converter4DataId = getResultDataId("file:text/csv", graphTableDataId[1])
        path = download_data(converter4DataId)
        print("Converter 4 File: " + path)
        try:
            shutil.move(path, res1)
        except:
            pass
    elif (data["id"] == converter4SubscriptionId and data['payload']['data']['algorithmInstanceUpdated'][
        'state'] == 'ERRORED'):
        print("Algorithm Errored")

def graphTableAlgoCallback(_id, data):
    print("Graph Table Algorithm CallBack " + json.dumps(data))
    if (data["id"] == graphTableAlgoSubscriptionId and data['payload']['data']['algorithmInstanceUpdated'][
        'state'] == 'FINISHED'):  # DownloadingData
        global graphTableDataId
        graphTableDataId = getResultsDataId("prefuse.data.Table", converter3DataId)
        converter4Id = searchConverterAlgorithm("prefuse.data.Table", "file:text/csv")
        if flag:
            converter4Instance = createAlgorithmInstance(converter4Id, graphTableDataId[1])
            global converter4SubscriptionId
            converter4SubscriptionId = subscribe_to_algorithm(converter4Instance, converter4CallBack)
            runAlgorithm(converter4Instance)
        converter5Instance = createAlgorithmInstance(converter4Id, graphTableDataId[0])
        global converter5SubscriptionId
        converter5SubscriptionId = subscribe_to_algorithm(converter5Instance, converter5CallBack)
        runAlgorithm(converter5Instance)

    elif (data["id"] == graphTableAlgoSubscriptionId and data['payload']['data']['algorithmInstanceUpdated'][
        'state'] == 'ERRORED'):
        print("Algorithm Errored")

def converter3CallBack(_id, data):
    print("Converter3 CallBack " + json.dumps(data))
    if (data["id"] == converter3SubscriptionId and data['payload']['data']['algorithmInstanceUpdated'][
        'state'] == 'FINISHED'):  # DownloadingData
        global converter3DataId
        converter3DataId = getResultDataId("prefuse.data.Graph", clusterDataId)
        graphTableAlgoInstance = createAlgorithmInstance("edu.iu.nwb.converter.tablegraph.GraphTable", converter3DataId)
        global graphTableAlgoSubscriptionId
        graphTableAlgoSubscriptionId = subscribe_to_algorithm(graphTableAlgoInstance, graphTableAlgoCallback)
        runAlgorithm(graphTableAlgoInstance)

    elif (data["id"] == converter3SubscriptionId and data['payload']['data']['algorithmInstanceUpdated'][
        'state'] == 'ERRORED'):
        print("Algorithm Errored")

def clusterAlgoCallback(_id, data):
    print("Cluster Algorithm CallBack " + json.dumps(data))
    if (data["id"] == clusterAlgoSubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'FINISHED'):  # DownloadingData
        global clusterDataId
        clusterDataId = getResultDataId("edu.uci.ics.jung.graph.Graph", converter2DataId)
        converterAlgoId = searchConverterAlgorithm("edu.uci.ics.jung.graph.Graph", "prefuse.data.Graph")
        algoInstance = createAlgorithmInstance(converterAlgoId, clusterDataId)
        global converter3SubscriptionId
        converter3SubscriptionId = subscribe_to_algorithm(algoInstance, converter3CallBack)
        runAlgorithm(algoInstance)
    elif (data["id"] == clusterAlgoSubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'ERRORED'):
        print("Algorithm Errored")

def converter2CallBack(_id, data):
    print("Converter2 CallBack " + json.dumps(data))
    if (data["id"] == converter2SubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'FINISHED'):  # DownloadingData
        global converter2DataId
        converter2DataId = getResultDataId("edu.uci.ics.jung.graph.Graph", coAuthorResultDataId)
        parameters = """
        {
            key: "n"
            value: "1"
            attributeType: INTEGER
        }
        """
        clusterAlgoInstance = create_algorithm_withParams("edu.iu.nwb.analysis.weakcomponentclustering.ClusteringAlgorithm", converter2DataId, parameters)
        global clusterAlgoSubscriptionId
        clusterAlgoSubscriptionId = subscribe_to_algorithm(clusterAlgoInstance, clusterAlgoCallback)
        runAlgorithm(clusterAlgoInstance)
    elif (data["id"] == converter2SubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'ERRORED'):
        print("Algorithm Errored")

def coauthorCallBack(_id, data):
    print("Co Author Call Back " + json.dumps(data))
    if (data["id"] == coAuthor1SubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'FINISHED'):
        converted_id = searchConverterAlgorithm("prefuse.data.Graph","edu.uci.ics.jung.graph.Graph")
        global coAuthorResultDataId
        coAuthorResultDataId = getResultDataId("prefuse.data.Graph", converter1DataId)
        converter2_instance = createAlgorithmInstance(converted_id, coAuthorResultDataId)
        global converter2SubscriptionId
        converter2SubscriptionId = subscribe_to_algorithm(converter2_instance, converter2CallBack)
        runAlgorithm(converter2_instance)
    elif (data["id"] == coAuthor1SubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'ERRORED'):
        print("Algorithm Errored")

def converter1callback(_id, data):
  print("Converter 1 CallBack: " + json.dumps(data))
  if (data["id"] == converter1SubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'FINISHED'):#DownloadingData
      global converter1DataId
      converter1DataId = getResultDataId("prefuse.data.Table", isiDataId)
      parameters="""
      {
        key: "fileFormat"
        value: "isi"
        attributeType: STRING
      }
      """
      coauthor_algo_instance = create_algorithm_withParams("edu.iu.nwb.analysis.extractcoauthorship",converter1DataId, parameters)
      global coAuthor1SubscriptionId
      coAuthor1SubscriptionId = subscribe_to_algorithm(coauthor_algo_instance, coauthorCallBack)
      runAlgorithm(coauthor_algo_instance)
  elif (data["id"] == converter1SubscriptionId and data['payload']['data']['algorithmInstanceUpdated']['state'] == 'ERRORED'):
      print("Algorithm Errored")

if __name__ == '__main__':

    global ws, isiDataId, coAuthor1SubscriptionId, converter2SubscriptionId, clusterAlgoSubscriptionId, converter1DataId
    global coAuthorResultDataId, converter2DataId, clusterDataId, converter3SubscriptionId, converter3DataId
    global graphTableAlgoSubscriptionId, graphTableDataId, converter4SubscriptionId, converter5SubscriptionId
    global res1, res2, flag
    flag = True
    if len(sys.argv)==4:
        res1 = sys.argv[3]
        res2 = sys.argv[2]
    elif len(sys.argv)==3:
        flag = False
        res2 = sys.argv[2]
    else:
        print("Invalid input. Please find the format.")
        print("<input file path> <authors_output_file_path> <network_output_file_path>")

    # Uploading Data
    filepath = sys.argv[1]
    isiDataId = upload_file(filepath)

    # Finding COnverter Algorithm
    in_data_format = 'file:text/isi'
    out_data_format = 'prefuse.data.Table'
    converter_id = searchConverterAlgorithm(in_data_format, out_data_format)

    # Creating an Instance of Algorithm with data id
    converter_algo_instance = createAlgorithmInstance(converter_id, isiDataId)

    # Subscribing to alerts of algorithm
    converter1SubscriptionId = subscribe_to_algorithm(converter_algo_instance, converter1callback)

    # Running an algorithm
    runAlgorithm(converter_algo_instance)




