import argparse
import os
import sys, csv

import el_controller
import elasticsearch
from index import mappings, baseline, extended, print_message


# configuration file object
class Configuration(object):
    def __init__(self):
        self.base = True
        self.base_index = "bindex"
        self.inc_uris = True
        self.inc_nspace = False
        self.prop = False

        self.ext = False
        self.ext_index = ""
        self.ext_fields = {}
        self.ext_inc_sub = True
        self.ext_inc_pre = True
        self.ext_inc_obj = True

        self.rdf_dir = ""
        self.instances = 5
        self.elastic_address = "http://localhost"
        self.elastic_port = "9200"

        self.verbose = False


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

            elif line[0] == "indexing.baseline":
                if line[1] == "yes":
                    config.base = True
                elif line[1] == "no":
                    config.base = False
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
                        config.prop = True

            elif line[0] == "indexing.ext.include_sub":
                if line[1] == "yes":
                    config.ext_inc_sub = True
                elif line[1] == "no":
                    config.ext_inc_sub = False
                else:
                    print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[0] + " " + line[
                        1] + ' not recognized.')
                    sys.exit(-1)

            elif line[0] == "indexing.ext.include_pre":
                if line[1] == "yes":
                    config.ext_inc_pre = True
                elif line[1] == "no":
                    config.ext_inc_pre = False
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
                if os.path.isdir(line[1]):
                    config.rdf_dir = line[1]
                else:
                    print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[
                        1] + ' not a proper folder.')
                    sys.exit(-1)

            elif line[0] == "indexing.instances":
                try:
                    config.instances = int(line[1])
                except ValueError:
                    print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[
                        1] + ' not an integer')
                    sys.exit(-1)

            elif line[0] == "elastic.address":
                config.elastic_address = line[1]

            elif line[0] == "elastic.port":
                try:
                    config.elastic_port = int(line[1])
                except ValueError:
                    print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[
                        1] + ' not an integer')
                    sys.exit(-1)

            elif line[0] == "verbose":
                if line[1] == "yes":
                    config.verbose = True
                elif line[1] == "no":
                    config.verbose = False
                else:
                    print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[0] + " " + line[
                        1] + ' not recognized.')
                    sys.exit(-1)

            else:
                print('Error,' + '\'' + cfile + '\'' + ' is not a proper config file: ' + line[
                    0] + ' not recognized.')
                sys.exit(-1)

    return config


# create the ElasticSearch indexes - mappings
def create_indexes(config):
    try:
        if config.base:
            # get mapping & create index
            base_map = mappings.get_baseline(config)
            el_controller.create_index(config.base_index, base_map)

            if config.prop:
                for field in config.ext_fields.keys():
                    prop_map = mappings.get_properties(field)
                    el_controller.create_index(field, prop_map)

        # create extended & properties indexes
        if config.ext:
            ext_map = mappings.get_extended(config)
            el_controller.create_index(config.ext_index, ext_map)

    except elasticsearch.ElasticsearchException as e:
        print('Elas4RDF error: could not create indexes: ' + str(e))
        exit(-1)


# starts indexing for baseline
def index_baseline(config):
    baseline.controller(config)


# starts indexing for extended
def index_extended(config):
    extended.controller(config)


# verifies properties-indexes exist before starting extended
def properties_exist(config):
    exist = True
    index_missing = []
    for field in config.ext_fields.keys():
        if not el_controller.index_exists(field):
            index_missing.append(field)
            exist = False

    if not exist:
        print('Elas4RDF error, could not create \'' + str(config.ext_index) + '\'.'
                                                                              ' Missing properties-index(es): ',
              index_missing, ". Start baseline indexing process again.")

    return exist


def main():
    # setting up arguments parser
    parser = argparse.ArgumentParser(description='\'Indexer for generating the baseline and/or extended index\'')
    parser.add_argument('-config', help='"specify the config file(.tsv)', required=True)
    args = vars(parser.parse_args())

    # read configuration file
    config = init_config_file(args['config'])

    # print verification message
    print_message.verification_message(config)

    # initialize & basic configuration
    try:
        el_controller.init(config.elastic_address, config.elastic_port)
    except elasticsearch.ElasticsearchException as e:
        print('Elas4RDF error: could not initialize Elasticsearch: ' + str(e))

    # create index mappings & structures
    create_indexes(config)

    # start indexing
    if config.base:
        index_baseline(config)
    if config.ext:
        if properties_exist(config):
            index_extended(config)
        else:
            exit(-1)


if __name__ == "__main__":
    main()
