import yaml
import sql
def get_confs(conf_yaml_file = "../sqlserver.yaml"):
    try:
        with open(conf_yaml_file, "rb") as yaml_file:
            confs = yaml.load(conf_yaml_file)
    except IOError:
        print("ERROR: Can't find SQL server config file
              {}".format(conf_yaml_file)
        raise
    else:
        return(confs)
