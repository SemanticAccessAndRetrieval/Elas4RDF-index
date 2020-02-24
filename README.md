# Elas4RDF - index service

Performs **keyword** search over **RDF** data, with classic IR techniques, upon triple-based documents using **Elasticsearch** (ES). 

Models two different indexing perspectives in both of which each ES doc represents a triple.
Models two different indexing perspectives in both of which each ES doc represents a triple.

1. *baseline* : only  makes use  of  the  information  that  exists  in  the  triple’s  three  components  (subject, predicate, object). In case the value of one of the components is a URI, the URI is tokenized into keywords.

2. *extended* : extends baseline document by including additional information for each triple component (if it is a resource - URI). Additional information corresponds to values of properties that can be given as input (e.g. *rdfs:comment*).


This project automates the indexing process and can be applied over any environment that runs Elasticsearch & Python3.
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
Start indexing process by running ```indexer_service.py``` which accepts a single parameter, the configuration file (-cfile). This (.tsv) file 
contains all necessary configuration for the process. Options include:

* **baseline** & **extended** index



```
indexing.base.name	<baseline_name>
indexing.base.include_uri	<yes,no>    # index keywords derived from the URI part
indexing.base.include_namespace	<yes,no>    # index keywords derived from the namespace part
indexing.baseline	<yes,no>
indexing.properties.index	<yes,no>     
indexing.extend	<yes,no>
indexing.ext.name   <extended_name>
indexing.ext.fields <name_1>;<URI_1> <name_2>;<URI_2> 
indexing.ext.include_sub    <yes,no>
indexing.ext.include_obj    <yes,no>
```

* **elastic** & other options:
```
indexing.data	<RDF_dir>
elastic.address	<host_name>
elastic.port	<port_number>
```
Examples of configuration files are included in ```res/configuration```. 


Note that RDF files are expected to be in the form of triples (.ttl).
