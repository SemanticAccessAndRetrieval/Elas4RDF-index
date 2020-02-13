import argparse
import os
import sys, csv

import el_controller
import elasticsearch
from index import mappings, baseline, extended


# configuration file object
class Configuration(object):
    def __init__(self):
        self.base_index = "bindex"
        self.inc_uris = True
        self.inc_nspace = False

        self.ext = False
        self.ext_index = ""
        self.ext_fields = {}
        self.ext_inc_sub = True
        self.ext_inc_obj = True

        self.rdf_dir = ""
        self.elastic_address = "http://localhost"
        self.elastic_port = "9200"


# initialize from configuration file
def init_config_file(cfile):
    if not os.path.isfile(cfile):
        print('Error, ' + '\'' + cfile + '\'' + ' does not exist.')
        sys.exit(-1)

    config = Configuration()
    with open(cfile) as tsvfile:
        tsvreader = csv.reader(tsvfile, delimiter="\t")
        for line in tsvreader:
            if line[0] == "indexing.base.name":
                config.base_index = line[1]

            elif line[0] == "indexing.base.include_uri":
                if line[1] == "yes":
                    config.inc_uris = True
                elif line[0] == "no":
                    config.inc_uris = False
                else:
                    print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[0] + " " + line[
                        1] + ' not recognized.')
                    sys.exit(-1)

            elif line[0] == "indexing.base.include_namespace":
                if line[1] == "yes":
                    config.inc_nspace = True
                elif line[1] == "no":
                    config.inc_nspace = False
                else:
                    print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[0] + " " + line[
                        1] + ' not recognized.')
                    sys.exit(-1)

            elif line[0] == "indexing.extend":
                if line[1] == "yes":
                    config.ext = True
                elif line[1] == "no":
                    config.ext = False
                else:
                    print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[0] + " " + line[
                        1] + ' not recognized.')
                    sys.exit(-1)


            elif line[0] == "indexing.ext.name":
                config.ext_index = line[1]

            elif line[0] == "indexing.ext.fields":
                if len(line[1].rsplit(" ", 1)) == 0:
                    print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[0] + " " + line[
                        1] + ' not recognized.')
                    sys.exit(-1)
                for field_entry in line[1].rsplit(" ", 1):
                    if len(field_entry.rsplit(";", 1)) == 0:
                        print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[
                            1] + ' not recognized.')
                        sys.exit(-1)
                    else:
                        contents = field_entry.rsplit(";", 1)
                        field_name = contents[0]
                        field = contents[1]
                        config.ext_fields[field_name] = field

            elif line[0] == "indexing.ext.include_sub":
                if line[1] == "yes":
                    config.ext_inc_sub = True
                elif line[1] == "no":
                    config.ext_inc_sub = False
                else:
                    print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[0] + " " + line[
                        1] + ' not recognized.')
                    sys.exit(-1)

            elif line[0] == "indexing.ext.include_obj":
                if line[1] == "yes":
                    config.ext_inc_obj = True
                elif line[1] == "no":
                    config.ext_inc_obj = False
                else:
                    print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[0] + " " + line[
                        1] + ' not recognized.')
                    sys.exit(-1)

            elif line[0] == "indexing.data":
                if not os.path.isdir(line[1]):
                    print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[
                        1] + ' not a proper folder.')
                    sys.exit(-1)
                else:
                    config.rdf_dir = line[1]

            elif line[0] == "elastic.address":
                config.elastic_address = line[1]

            elif line[0] == "elastic.port":
                config.elastic_port = line[1]

            else:
                print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[
                    0] + ' not recognized.')
                sys.exit(-1)

    return config


# create the ElasticSearch indexes - mappings
def create_indexes(config):
    try:
        # get mapping & create index
        base_map = mappings.get_baseline(config)
        el_controller.create_index(config.base_index, base_map)

        # create extended & properties indexes
        if config.ext:
            ext_map = mappings.get_extended(config)
            el_controller.create_index(config.ext_index, ext_map)

            for field in config.ext_fields.keys():
                prop_map = mappings.get_properties(field)
                el_controller.create_index(field, prop_map)

    except elasticsearch.ElasticsearchException as e:
        print('Elas4RDF error: could not create indexes: ' + str(e))
        exit(-1)


# starts indexing for baseline
def index_baseline(config):
    baseline.controller(config)

# starts indexing for baseline
def index_extended(config):
    extended.controller(config)

def main():
    # setting up arguments parser
    parser = argparse.ArgumentParser(description='\'Indexer for generating the baseline and/or extended index\'')
    parser.add_argument('-config', help='"specify the .config file', required=True)
    args = vars(parser.parse_args())

    # read configuration file
    config = init_config_file(args['config'])

    # print verification message
    if (config.ext):
        print("Elas4RDF: configuration file loaded successfully.\n"
              "Create the following indexes: "
              "\n\t 1. baseline - \'" + config.base_index +
              "\' \n\t 2. extended - \'" + config.ext_index +
              "\' \n\t 3. properties - " + str(config.ext_fields.keys()) +
              "\nOther options: "
              "\n\t base.include_uri: " + str(config.inc_uris) +
              "\n\t base.include_namespace: " + str(config.inc_nspace) +
              "\n\t ext.include_subject: " + str(config.ext_inc_sub) +
              "\n\t ext.include_object: " + str(config.ext_inc_obj) +
              "\n\t index.data: " + config.rdf_dir +
              "\n\t elastic_address: " + config.elastic_address +
              "\n\t elastic_port: " + config.elastic_port
              )
    else:
        print("Elas4RDF: configuration file loaded successfully.\n"
              "Create the following index: "
              "\n\t 1. baseline - \'" + config.base_index +
              "'\nOther options: "
              "\n\t base.include_uri: " + str(config.inc_uris) +
              "\n\t base.include_namespace: " + str(config.inc_nspace) +
              "\n\t index.data: " + config.rdf_dir +
              "\n\t elastic_address: " + config.elastic_address +
              "\n\t elastic_port: " + config.elastic_port
              )
    raw_input("Press Enter to continue...")

    # initialize & basic configuration
    try:
        el_controller.init(config.elastic_address, config.elastic_port)
    except elasticsearch.ElasticsearchException as e:
        print('Elas4RDF error: could not initialize Elasticsearch: ' + str(e))

    # create index structures & start indexing
    create_indexes(config)
    index_baseline(config)

    if config.ext:
        index_extended()


if __name__ == "__main__":
    main()
