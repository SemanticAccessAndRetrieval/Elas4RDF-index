import sys
from elasticsearch import Elasticsearch
import json


def get_baseline(config):
    # prepare curl-commands
    curl_put = 'curl -XPUT ' + '"' + config.elastic_address + ":" + config.elastic_port + "/" + config.base_index + '" ' + '-H \'Content-Type: application/json\' -d\''

    # load baseline mapping from res
    with open('res/mapping/baseline.json') as file:
        base_map = json.load(file)

    if config.inc_uris:
        pass
    else:
        base_map['mappings']['_doc']['properties']['subjectKeywords']['enabled'] = False
        base_map['mappings']['_doc']['properties']['predicateKeywords']['enabled'] = False
        base_map['mappings']['_doc']['properties']['objectKeywords']['enabled'] = False

    if config.inc_nspace:
        base_map['mappings']['_doc']['properties']['subjectNspaceKeys']['type'] = "text"
        base_map['mappings']['_doc']['properties']['predicateNspaceKeys']['type'] = "text"
        base_map['mappings']['_doc']['properties']['objectNspaceKeys']['type'] = "text"

        base_map['mappings']['_doc']['properties']['subjectNspaceKeys']['analyzer'] = "url_analyzer"
        base_map['mappings']['_doc']['properties']['predicateNspaceKeys']['analyzer'] = "url_analyzer"
        base_map['mappings']['_doc']['properties']['objectNspaceKeys']['analyzer'] = "url_analyzer"
    else:
        base_map['mappings']['_doc']['properties']['subjectNspaceKeys']['enabled'] = False
        base_map['mappings']['_doc']['properties']['predicateNspaceKeys']['enabled'] = False
        base_map['mappings']['_doc']['properties']['objectNspaceKeys']['enabled'] = False

    ## curl-command, used for debuggin
    # curl_put += "\n" + json.dumps(base_map, indent=4, sort_keys=False) + '\''

    return base_map


def get_extended(config):
    # prepare curl-commands
    curl_put = 'curl -XPUT ' + '"' + config.elastic_address + ":" + config.elastic_port + "/" + config.ext_index + '" ' + '-H \'Content-Type: application/json\' -d\''

    # load extended mapping from res
    with open('res/mapping/extended.json') as file:
        ext_map = json.load(file)

    if config.inc_uris:
        pass
    else:
        ext_map['mappings']['_doc']['properties']['subjectKeywords']['enabled'] = False
        ext_map['mappings']['_doc']['properties']['predicateKeywords']['enabled'] = False
        ext_map['mappings']['_doc']['properties']['objectKeywords']['enabled'] = False

    if config.inc_nspace:
        ext_map['mappings']['_doc']['properties']['subjectNspaceKeys']['type'] = "text"
        ext_map['mappings']['_doc']['properties']['predicateNspaceKeys']['type'] = "text"
        ext_map['mappings']['_doc']['properties']['objectNspaceKeys']['type'] = "text"

        ext_map['mappings']['_doc']['properties']['subjectNspaceKeys']['analyzer'] = "url_analyzer"
        ext_map['mappings']['_doc']['properties']['predicateNspaceKeys']['analyzer'] = "url_analyzer"
        ext_map['mappings']['_doc']['properties']['objectNspaceKeys']['analyzer'] = "url_analyzer"
    else:
        ext_map['mappings']['_doc']['properties']['subjectNspaceKeys']['enabled'] = False
        ext_map['mappings']['_doc']['properties']['predicateNspaceKeys']['enabled'] = False
        ext_map['mappings']['_doc']['properties']['objectNspaceKeys']['enabled'] = False

    for field in config.ext_fields.keys():
        if config.ext_inc_sub:
            f = field + "_sub"
            ext_map['mappings']['_doc']['properties'][f] = {}
            ext_map['mappings']['_doc']['properties'][f]['type'] = "text"
            ext_map['mappings']['_doc']['properties'][f]['analyzer'] = "m_analyzer"

        if config.ext_inc_sub:
            f = field + "_obj"
            ext_map['mappings']['_doc']['properties'][f] = {}
            ext_map['mappings']['_doc']['properties'][f]['type'] = "text"
            ext_map['mappings']['_doc']['properties'][f]['analyzer'] = "m_analyzer"

    ## curl-command, used for debuggin
    # curl_put += json.dumps(ext_map, indent=4, sort_keys=False) + '\''

    return ext_map


def get_properties(field):
    # load extended mapping from res
    with open('res/mapping/properties.json') as file:
        ext_map = json.load(file)

    ext_map['mappings']['_doc']['properties'][field] = {}
    ext_map['mappings']['_doc']['properties'][field]['type'] = "text"
    ext_map['mappings']['_doc']['properties'][field]['analyzer'] = "m_analyzer"

    return ext_map
