import six
import copy


class FakeConfigParser(object):

    def __init__(self, config):
            self.CONFIG = copy.deepcopy(config)

    def read(self, path):
        return

    def sections(self):
        return self.CONFIG.keys()

    def has_option(self, section, key):
        return section in self.CONFIG and key in self.CONFIG[section]

    def has_section(self, section):
        return section in self.CONFIG

    def get(self, section, key):
        try:
            return self.CONFIG[section][key]
        except KeyError:
            raise six.moves.configparser.NoOptionError(section, key)
