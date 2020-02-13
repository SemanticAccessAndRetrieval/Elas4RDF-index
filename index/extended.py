import argparse
import glob
import multiprocessing
import os
import re
import sys
from timeit import default_timer as timer

import el_controller


def get_name_space(triple_part, pre_flag):
    if pre_flag:
        n_space = triple_part.rsplit('#', 1)[0]
    else:
        n_space = triple_part.rsplit('/', 1)[0]

    return n_space


def is_resource(full_uri):
    return full_uri in name_spaces


def get_property(entity):
    return \
        {
            "query": {
                "constant_score": {
                    "filter": {
                        "term": {
                            "resource_terms": "" + entity
                        }
                    }
                }
            }
        }


def extended_index(rdf_folder):
    bulk_actions = []
    bulk_size = 3500

    iter = 0
    print("--" + rdf_folder + ": started")

    for ttl_file in glob.glob(rdf_folder + '/*.ttl'):
        with open(ttl_file) as fp:

            prop_maps = {}

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

                # create elastic triple-doc
                doc = {"subjectKeywords": sub_keywords, "predicateKeywords": pred_keywords,
                       "objectKeywords": obj_keywords, "subjectNspaceKeys": sub_nspace,
                       "predicateNspaceKeys": pred_nspace, "objectNspaceKeys": obj_nspace}

                # retrieve all subject's properties (described in ext_fields)
                if config.ext_inc_sub:

                    for prop_name in config.ext_fields.keys():

                        if prop_maps.__contains__(sub_keywords):
                            doc[prop_name + "_sub"] = prop_maps[sub_keywords]

                        else:
                            prop_res = el_controller.search(prop_name, '', 150, get_property(sub_keywords))
                            doc[prop_name + "_sub"] = []

                            for prop_hit in prop_res['hits']['hits']:
                                doc[prop_name + "_sub"].append(" " + prop_hit["_source"][prop_name])

                            prop_maps[sub_keywords] = doc[prop_name + "_sub"]

                # retrieve all object's properties (described in ext_fields)
                if config.ext_inc_obj == 1 and is_resource(obj_nspace):

                    for prop_name in config.ext_fields.keys():

                        if prop_maps.__contains__(obj_keywords):
                            doc[prop_name + "_obj"] = prop_maps[obj_keywords]

                        else:

                            prop_res = el_controller.search(prop_name, '', 150, get_property(obj_keywords))
                            doc[prop_name + "_obj"] = []
                            for prop_hit in prop_res['hits']['hits']:
                                doc[prop_name + "_obj"].append(" " + prop_hit["_source"][prop_name])

                            prop_maps[obj_keywords] = doc[prop_name + "_obj"]

                # add insert action
                action = {
                    "_index": config.ext_index,
                    '_op_type': 'index',
                    "_type": "_doc",
                    "_source": doc
                }

                bulk_actions.append(action)
                if len(bulk_actions) > bulk_size:
                    el_controller.bulk_action(bulk_actions)
                    del bulk_actions[0:len(bulk_actions)]

                iter += 1
                if iter % 1000000 == 0:
                    print("Iter: ", iter, " -- " + rdf_folder)
                line = fp.readline()

    # flush any action that is left inside the bulk actions
    el_controller.bulk_action(bulk_actions)

    print("--" + rdf_folder + ": finished")


####################################################


# known namespaces - resources (manually maintained) TODO
name_spaces = set()
name_spaces.add("http://dbpedia.org/resource")


def controller(config_f):
    global config
    config = config_f

    rdf_dir = config.rdf_dir

    # deploy index instances (currently set manually to 5) TODO
    ttl_folders = []
    for ttl_folder in os.listdir(rdf_dir):
        ttl_folder = rdf_dir + "/" + ttl_folder
        if os.path.isdir(ttl_folder):
            ttl_folders += [os.path.join(ttl_folder, f) for f in os.listdir(ttl_folder)]

    print(ttl_folders)

    start = timer()
    p = multiprocessing.Pool(5)
    p.map(extended_index, ttl_folders)

    end = timer()
    print("elapsed time: ", (end - start))
