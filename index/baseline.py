import glob
import os
import re
import time
from pathlib import Path
from timeit import default_timer as timer
from multiprocessing import Pool, Manager

import elasticsearch
import el_controller
from index import print_message


# extract name-space from an input URI
def get_name_space(triple_part, pred_flag):
    if pred_flag:
        n_space = triple_part.rsplit('#', 1)[0]
    else:
        n_space = triple_part.rsplit('/', 1)[0]

    return n_space


# is a prefix included in uri
def contains_prefix(uri):
    return uri.rsplit(':', 1)[0].__contains__(":")


# main method for indexing - accepts an input file
def baseline_index(input_file):
    # bulk config - empirically set to 3500
    bulk_size = 3500
    prop_bulk_size = 3500
    bulk_actions = []
    prop_bulk_actions = []
    global config

    if (config.verbose):
        print("\t " + input_file + ": started")

    with open(input_file) as fp:

        line = fp.readline()
        while line:

            # not a valid .nt line
            if "<" not in line:
                line = fp.readline()
                continue

            line = line.replace("<", "").replace(">", "").replace("\n", "")
            contents = line.split(" ", 2)

            if len(contents) < 3:
                line = fp.readline()
                continue

            # handle subject
            if contains_prefix(contents[0]):
                split_prefix = contents[0].rsplit(':', 1)
                sub_keywords = split_prefix[0].split("/")[-1] + ":" + split_prefix[1]
                sub_nspace = ''.join(re.split("(/)", split_prefix[0])[:-1])
            else:
                sub_keywords = contents[0].rsplit('/', 1)[-1].replace(":", "")
                sub_nspace = get_name_space(contents[0], False)

            # handle predicate
            if "#" not in contents[1]:
                pred_keywords = contents[1].rsplit('/', 1)[-1].replace(":", "")
                pred_nspace = get_name_space(contents[1], False)
            else:
                pred_keywords = contents[1].rsplit('#', 1)[-1].replace(":", "")
                pred_nspace = get_name_space(contents[1], True)

            # handle object
            if "\"" in contents[2]:
                obj_keywords = contents[2].replace("\"", " ")[:-2]
                obj_nspace = ""
            elif "/" in contents[2]:
                if contains_prefix(contents[2]):
                    split_prefix = contents[2].rsplit(':', 1)
                    obj_keywords = split_prefix[0].split("/")[-1] + ":" + split_prefix[1][:-2]
                    obj_nspace = ''.join(re.split("(/)", split_prefix[0])[:-1])
                else:
                    obj_keywords = contents[2].rsplit('/', 1)[-1].replace(":", "")[:-2]
                    obj_nspace = get_name_space(contents[2], False)
            elif "#" in contents[2]:
                obj_keywords = contents[2].rsplit('#', 1)[-1].replace(":", "")[:-2]

            # if predicate-property is included in ext_fields - build properties indexes
            if config.prop and contents[1] in config.ext_fields.values():

                # get field-prop name
                field_prop = {v: k for k, v in config.ext_fields.items()}[contents[1]]

                # create a property - document
                prop_doc = {"resource_terms": sub_keywords, field_prop: obj_keywords}

                # add insert action
                prop_action = {
                    "_index": field_prop,
                    '_op_type': 'index',
                    "_type": "_doc",
                    "_source": prop_doc
                }

                prop_bulk_actions.append(prop_action)
                if len(prop_bulk_actions) > prop_bulk_size:
                    el_controller.bulk_action(prop_bulk_actions)
                    del prop_bulk_actions[0:len(prop_bulk_actions)]

            # create a triple - document
            doc = {"subjectKeywords": sub_keywords, "predicateKeywords": pred_keywords,
                   "objectKeywords": obj_keywords, "subjectNspaceKeys": sub_nspace,
                   "predicateNspaceKeys": pred_nspace, "objectNspaceKeys": obj_nspace}

            try:
                # add insert action
                action = {
                    "_index": config.base_index,
                    '_op_type': 'index',
                    "_type": "_doc",
                    "_source": doc
                }

                bulk_actions.append(action)
                if len(bulk_actions) > bulk_size:
                    el_controller.bulk_action(bulk_actions)
                    del bulk_actions[0:len(bulk_actions)]

            except elasticsearch.ElasticsearchException as es:

                print("Elas4RDF: Exception occured (skipping line), in file: " + input_file)
                if (config.verbose):
                    print(str(es))
            finally:
                line = fp.readline()
            ####

    # flush any action that is left inside the bulk actions
    el_controller.bulk_action(bulk_actions)
    el_controller.bulk_action(prop_bulk_actions)

    global finished_files
    global total_files
    finished_files.append(input_file)

    # print progress information
    if len(finished_files) == len(total_files):
        p_str = ""
        end = "\n"
        time.sleep(5)
    else:
        p_str = "\r"
        end = ""
    print("\t Files : " + str(len(finished_files)) + " / " +
          str(len(total_files)) + " , triples indexed: " + str(
        el_controller.count_docs(config.base_index)) + p_str,
          end=end)

    if (config.verbose):
        print("\t " + input_file + ": finished")


def controller(config_f):
    global config
    config = config_f

    rdf_dir = config.rdf_dir

    # count.nt files
    global total_files
    total_files = []
    for path in Path(rdf_dir).rglob('*.nt'):
        total_files.append(str(path.absolute()))

    print_message.baseline_starting(config, str(len(total_files)))

    # list all .nt files of input RDF_DIRs
    all_files = glob.glob(rdf_dir + '/**/*.nt', recursive=True)

    start = timer()

    # deploy index instances (as indicated in indexing.instances in -config)
    manager = Manager()
    global finished_files
    finished_files = manager.list()
    p = Pool(config.instances)
    p.map(baseline_index, all_files)

    end = timer()

    # get final number of docs & print message
    docs_b = el_controller.count_docs(config.base_index)
    print_message.baseline_finised(config, str((end - start)), docs_b)
