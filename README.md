# Elas4RDF - index service

Performs **keyword** search over **RDF** data, with classic IR techniques, upon triple-based documents using **Elasticsearch** (ES). 

Models two different indexing perspectives in both of which each ES doc represents a triple.

1. *baseline* : only  makes use  of  the  information  that  exists  in  the  triple’s  three  components  (subject, predicate, object). In case the value of one of the components is a URI, the URI is tokenized into keywords.

2. *extended* : extends baseline document by including additional information for each triple component (if it is a resource - URI). Additional information corresponds to values of properties that can be given as input (e.g. *rdfs:comment*).


This project automates the indexing process and can be applied over any environment that runs Elasticsearch & Python 3.
### Setup
Download an Elastic search package  (e.g. v=elasticsearch-6.8.0). After extracting all contents, start an instance:
```
  ./elasticsearch-6.8.0/bin/elasticsearch&
```  

Next, install all requirements needed for our scripts as described in requirements.txt:
```
  pip3 install -r requirements.txt
```

## Indexer service 
Start indexing process by running ```indexer_service.py``` with a single parameter, the configuration file (-cfile). This (.properties) file 
contains all necessary configuration for the process. Options include:

* **baseline** & **extended** index

```
index.id=<data_id>                      # a unique id for the dataset

index.base=<yes,no>                     # create baseline index (?)
index.base.name=<baseline_name>         # ES index name
index.base.include_uri=<yes,no>         # index keywords derived from the URI part
index.base.include_namespace=<yes,no>   # index keywords derived from the namespace part

index.ext=<yes,no>                      # create extended index (?)
index.ext.name=<extended_name>          # ES index name      
index.ext.fields=<name_1>;<URI_1> <name_2>;<URI_2>  # specify the extended properties
index.ext.include_sub=<yes,no>           # extend subject properties (?)
index.ext.include_pre=<yes,no>           # extend predicate properties (if a resource) (?)
index.ext.include_obj=<yes,no>           # extend object properties (if a resource) (?)
```

* **elastic** & other options:
```
index.data=<RDF_dir>            # input directory
elastic.address=<host_name>     # defaults to 'localhost'
elastic.port=<port_number>      # defaults to '9200'
```
Examples of .properties files are included in ```res/configuration```. 

Note that RDF files (inside input directory) are expected to be of N-triples syntax (.nt).
