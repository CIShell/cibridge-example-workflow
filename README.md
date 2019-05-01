# cibridge-example-workflow
An example repository showing how to use CIBridge and Sci2 algorithms using Python

# Building Instructions
1. Visit this repository `https://github.com/CIShell/cishell-cibridge` and follow the build instructions and running container instructions to run the CIBridge GraphQL server.
2. Use the `Sci2Container` to access the algorithms from Sci2 algorithms.

# Steps to run the script

1. Install the following dependencies using pip. <br/>
`pip install requests` <br/>
`pip install py-graphql-client`

2. Run the isi_worklow.py file by passing the input `isi` file, the output path with filename for the `authors.csv` and the third optional argument output path with filename for the `nodes.csv`.

Examples:
1.  `isi_workflow.py xxxx/FourNetSciResearchers.isi authors.csv nodes.csv`
2. `isi_workflow.py xxxx/FourNetSciResearchers.isi authors.csv`

For the above example you will get the output in the same folder with names `authors.csv` and `nodes.csv`
