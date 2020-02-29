import sys

def verification_message(config):
    print("Elas4RDF: configuration file loaded successfully")
    print("Create the following indexes: ", end='')
    options_str = "\nOther options: "

    if config.base:
        print("\n\t baseline - \'" + config.base_index + "\'", end='')
        options_str += "\n\t base.include_uri: " + str(config.inc_uris) + \
                       "\n\t base.include_namespace: " + str(
            config.inc_nspace)

    if config.prop and config.base:
        print("\n\t properties - " + str(config.ext_fields.keys()), end='')

    if config.ext:
        print("\n\t extended - \'" + config.ext_index + "\'", end='')
        options_str += "\n\t ext.include_subject: " + str(config.ext_inc_sub) + \
                       "\n\t ext.include_predicate: " + str(config.ext_inc_pre) + \
                       "\n\t ext.include_object: " + str(config.ext_inc_obj)

    if not config.base and not config.ext:
        print("No indexes are enabled, see configuration (baseline & extended -> no). Exiting.")
        return

    options_str += "\n\t index.data: " + config.rdf_dir + \
                   "\n\t index.instances: " + str(config.instances) + \
                   "\n\t elastic_address: " + config.elastic_address + \
                   "\n\t elastic_port: " + str(config.elastic_port)

    print(options_str)
    input("Press Enter to continue...")


def baseline_starting(config, stats):
    if config.prop:
        print("Elas4RDF: Baseline & properties indexing started .. ", end='')
    else:
        print("Elas4RDF: Baseline indexing started .. ", end='')
    print("Files : " + stats)


def baseline_process(stats):
    pass


def baseline_finised(config, stats, docs_num):
    if config.prop:
        print("Elas4RDF: Successfully created indexes: "
              "\n\t 1. baseline - \'" + config.base_index + "\'" + " (" + str(docs_num) + " triples)" +
              "\n\t 2. properties - " + str(config.ext_fields.keys()) +
              "")
    else:
        print("Elas4RDF: Successfully created index: "
              "\n\t 1. baseline - \'" + config.base_index + "\' " + "(" + str(docs_num) + " triples)" +
              "")

    if config.verbose:
        print("\telapsed time " + stats)


def extended_starting(config, stats):
    print("Elas4RDF: Extended '" + config.ext_index + "' indexing started .. ", end='')
    print("Files : " + stats)


def extended_process(stats):
    pass


def extended_finished(config, stats, docs_num):
    print("Elas4RDF: Successfully created index: " +
          "\n\t 1. extended - \'" + config.ext_index + "\' " + "(" + str(docs_num) + " triples)" +
          "\n\t\t with extended fields - " + str(config.ext_fields.keys()) +
          "")

    if config.verbose:
        print("\telapsed time " + stats)
