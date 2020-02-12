import glob
import multiprocessing
import os
import re
import sys
from timeit import default_timer as timer

import el_controller


# extract name-space from an input URI
def get_name_space(triple_part, pre_flag):
    if pre_flag:
        n_space = triple_part.rsplit('#', 1)[0]
    else:
        n_space = triple_part.rsplit('/', 1)[0]

    return n_space


def is_resource(full_uri):
    return full_uri in name_spaces


def index_rdf_folder(input_folder, config):
    # bulk config - empirically set to 3500
    bulk_size = 3500
    prop_bulk_size = 3500
    bulk_actions = []
    prop_bulk_actions = []

    iter = 0
    print("--" + input_folder + ": started")

    # parse each .ttl file inside input folder
    for ttl_file in glob.glob(input_folder + '/*.ttl'):
        with open(ttl_file) as fp:

            line = fp.readline()
            while line:

                if "<" not in line:
                    line = fp.readline()
                    continue

                line = line.replace("<", "").replace(">", "").replace("\n", "")
                contents = line.split(" ", 2)

                if len(contents) < 3:
                    line = fp.readline()
                    continue

                # handle subject
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
                    obj_keywords = contents[2].rsplit('/', 1)[-1].replace(":", "")[:-2]
                    obj_nspace = get_name_space(contents[2], False)
                elif "#" in contents[2]:
                    obj_keywords = contents[2].rsplit('#', 1)[-1].replace(":", "")[:-2]

                if config.ext:

                    # if predicate-property is included in properties-files config
                    if contents[1] in config.ext_fields.values():

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

                #### monitor output ####
                iter += 1
                if iter % 1000000 == 0:
                    print("Iter: ", iter, " -- " + input_folder)
                line = fp.readline()
                ####

    # flush any action that is left inside the bulk actions
    el_controller.bulk_action(bulk_actions)
    el_controller.bulk_action(prop_bulk_actions)

    print("--" + input_folder + ": finished")


# known namespaces - resources (manually maintained)
name_spaces = set()
name_spaces.add("http://dbpedia.org/resource")


def controller(config):
    rdf_dir = config.rdf_dir

    # deploy index instances (currently set manually to 12)
    ttl_folders = []
    for ttl_folder in os.listdir(rdf_dir):
        ttl_folder = rdf_dir + "/" + ttl_folder
        if os.path.isdir(ttl_folder):
            ttl_folders += [os.path.join(ttl_folder, f) for f in os.listdir(ttl_folder)]

    start = timer()
    p = multiprocessing.Pool(12)
    p.map(index_rdf_folder, ttl_folders, config)

    end = timer()
    print("elapsed time: ", (end - start))
