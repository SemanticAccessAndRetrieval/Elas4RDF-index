# Elas4RDF

Performs keyword search over **RDF** data, with classic IR techniques, upon triple-based  documents using **Elasticsearch** (ES).

Contains both an indexer and a searcher service that can be applied over any environment that runs Elasticsearch.

Indexer models two different indexing perspectives in both of which each ES doc represents a triple.

1. *baseline* : only  makes use  of  the  information  that  exists  in  the  triple’s  three  components  (subject, predicate, object). In case the value of one of the components is a URI, the URI is tokenized into keywords.

2. *extended* : extends baseline document by including additional information for each triple component (if it is a resource - URI). Additional information corresponds to values of properties that can be given as input (e.g. *rdfs:comment*). 