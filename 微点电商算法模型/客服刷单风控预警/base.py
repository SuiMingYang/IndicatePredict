import configparser

def setConf():
    config_url="config.conf"
    conf = configparser.ConfigParser()
    conf.read(config_url)
    return conf

conf = setConf()