import sys


def verification_message(config):
    print("Elas4RDF: configuration file loaded successfully")
    if config.ext and config.base:
        print("\n"
              "Create the following indexes: "
              "\n\t 1. baseline - \'" + config.base_index +
              "\' \n\t 2. extended - \'" + config.ext_index +
              "\' \n\t 3. properties - " + config.ext_fields.keys() +
              "\nOther options: "
              "\n\t base.include_uri: " + str(config.inc_uris) +
              "\n\t base.include_namespace: " + str(config.inc_nspace) +
              "\n\t ext.include_subject: " + str(config.ext_inc_sub) +
              "\n\t ext.include_object: " + str(config.ext_inc_obj) +
              "\n\t index.data: " + config.rdf_dir +
              "\n\t elastic_address: " + config.elastic_address +
              "\n\t elastic_port: " + config.elastic_port
              )

    elif config.base and not config.ext:
        print("Create the following index: "
              "\n\t 1. baseline - \'" + config.base_index +
              "\' \n\t 2. properties - " + config.ext_fields.keys() +
              "'\nOther options: "
              "\n\t base.include_uri: " + str(config.inc_uris) +
              "\n\t base.include_namespace: " + str(config.inc_nspace) +
              "\n\t index.data: " + config.rdf_dir +
              "\n\t elastic_address: " + config.elastic_address +
              "\n\t elastic_port: " + config.elastic_port
              )

    elif config.ext and not config.base:
        print("Create the following index: "
              "\n\t 1. extended - \'" + config.ext_index +
              "\''\nOther options: "
              "\n\t ext.include_subject: " + str(config.ext_inc_sub) +
              "\n\t ext.include_object: " + str(config.ext_inc_obj) +
              "\n\t index.data: " + config.rdf_dir +
              "\n\t elastic_address: " + config.elastic_address +
              "\n\t elastic_port: " + config.elastic_port
              )

    else:
        print("No indexes are enabled, see configuration (baseline & extended -> no). Exiting")


def baseline_finised(config):
    if config.ext:
        print("Elas4RDF: Successfully created indexes: "
              "\n\t 1. baseline - \'" + config.base_index +
              "\'\n\t 2. properties - " + config.ext_fields.keys() +
              "")
    else:
        print("Elas4RDF: Successfully created index: "
              "\n\t 1. baseline - \'" + config.base_index +
              "")


def extended_finished(config):
    print("Elas4RDF: Successfully created index: " +
          "\n\t 1. baseline - \'" + config.base_index +
          "\'\n\t\t with extended fields - " + config.ext_fields.keys() +
          "")
