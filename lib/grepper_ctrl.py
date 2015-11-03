# Copyright 2013-2014 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from lib.controllerlib import CommandHelp, BaseController, CommandController
from lib.controllerlib import ShellException
#import time, os, sys, platform, shutil, urllib2, socket
from lib.loghelper import LogHelper
from lib import common

@CommandHelp('Aerospike Admin')
class RootController(BaseController):
    def __init__(self):
        super(RootController, self).__init__()

        self.controller_map = {
            'show':ShowController,
            'grep':GrepController
        }

    @CommandHelp('Terminate session')
    def do_exit(self, _):
        # This function is a hack for autocomplete
        return "EXIT"

    @CommandHelp('Returns documentation related to a command',
                 'for example, to retrieve documentation for the "info"',
                 'command use "help info".')
    def do_help(self, line):
        self.executeHelp(line)


@CommandHelp('"show" is used to display Aerospike Statistics and',
             'configuration.')
class ShowController(CommandController):
    def __init__(self):
        self.controller_map = {
            'config':ShowConfigController,
            'statistics':ShowStatisticsController
        }

        self.modifiers = set()

    def _do_default(self, line):
        self.executeHelp(line)


@CommandHelp('"show config" is used to display Aerospike configuration settings')
class ShowConfigController(CommandController):
    def __init__(self):
        self.modifiers = set(['with', 'like'])

    @CommandHelp('Displays service, network, namespace, and xdr configuration')
    def _do_default(self, line):
        self.do_service(line)
        self.do_network(line)
        self.do_namespace(line)
        self.do_xdr(line)

    @CommandHelp('Displays service configuration')
    def do_service(self, _):
        service_configs = common.grepper.infoGetConfig(stanza='service')

        for config_file in sorted(service_configs.keys()):
            self.view.showConfig("Service Configuration (%s)"%(config_file),
                                 service_configs[config_file],
                                 LogHelper(config_file),
                                 **self.mods)

    @CommandHelp('Displays network configuration')
    def do_network(self, _):
        service_configs = common.grepper.infoGetConfig(stanza='network')

        for config_file in sorted(service_configs.keys()):
            self.view.showConfig("Network Configuration (%s)"%(config_file),
                                 service_configs[config_file],
                                 LogHelper(config_file), **self.mods)

    @CommandHelp('Displays namespace configuration')
    def do_namespace(self, _):
        ns_configs = common.grepper.infoGetConfig(stanza='namespace')

        for config_file in sorted(ns_configs.keys()):
            for ns, configs in ns_configs[config_file].iteritems():
                self.view.showConfig("%s Namespace Configuration (%s)"%(
                    ns, config_file),
                                     configs, LogHelper(config_file), **self.mods)

    @CommandHelp('Displays XDR configuration')
    def do_xdr(self, _):
        print "ToDo"


@CommandHelp('Displays statistics for Aerospike components.')
class ShowStatisticsController(CommandController):
    def __init__(self):
        self.modifiers = set(['with', 'like'])

    @CommandHelp('Displays bin, set, service, namespace, and xdr statistics')
    def _do_default(self, line):
        self.do_bins(line)
        self.do_sets(line)
        self.do_service(line)
        self.do_namespace(line)
        self.do_xdr(line)

    @CommandHelp('Displays service statistics')
    def do_service(self, _):
        service_stats = common.grepper.infoStatistics(stanza="service")
        for config_file in sorted(service_stats.keys()):
            self.view.showConfig("Service Statistics (%s)"%(config_file),
                                 service_stats[config_file],
                                 LogHelper(config_file),
                                 **self.mods)

    @CommandHelp('Displays namespace statistics')
    def do_namespace(self, _):
        ns_stats = common.grepper.infoStatistics(stanza="namespace")
        for config_file in sorted(ns_stats.keys()):
            for ns, configs in ns_stats[file].iteritems():
                self.view.showStats("%s Namespace Statistics (%s)"%(
                    ns, config_file),
                                    configs, LogHelper(config_file), **self.mods)

    @CommandHelp('Displays set statistics')
    def do_sets(self, _):
        set_stats = common.grepper.infoStatistics(stanza="sets")
        for config_file in sorted(set_stats.keys()):
            for ns_set, configs in set_stats[config_file].iteritems():
                self.view.showStats("%s Set Statistics (%s)"%(ns_set, config_file),
                                    configs, LogHelper(file), **self.mods)

    @CommandHelp('Displays bin statistics')
    def do_bins(self, _):
        new_bin_stats = common.grepper.infoStatistics(stanza="bins")
        for config_file in sorted(new_bin_stats.keys()):
            for ns, configs in new_bin_stats[config_file].iteritems():
                self.view.showStats("%s Bin Statistics (%s)"%(ns, file),
                                    configs, LogHelper(config_file), **self.mods)

    @CommandHelp('Displays xdr statistics')
    def do_xdr(self, _):
        print "ToDo"


@CommandHelp('Displays grep results for input string.')
class GrepController(CommandController):
    def __init__(self):
        self.controller_map = {
            'cluster':GrepClusterController,
            'servers':GrepServersController
        }
        self.modifiers = set()

    def _do_default(self, line):
        self.executeHelp(line)


class GrepFile(CommandController):
    def __init__(self, grep_cluster, modifiers):
        self.grep_cluster = grep_cluster
        self.modifiers = modifiers

    def do_show(self, line):
        if not line:
            raise ShellException("Could not understand grep request, " + \
                                 "see 'help grep'")

        mods = self.parseModifiers(line)
        line = mods['line']

        tline = line[:]
        search_str = ""
        while tline:
            word = tline.pop(0)
            if word == '-s':
                search_str = tline.pop(0)
                search_str = self.stripString(search_str)
            else:
                raise ShellException(
                    "Do not understand '%s' in '%s'"%(word,
                                                      " ".join(line)))
        grep_res = {}
        if search_str:
            grep_res = common.grepper.grep(search_str, self.grep_cluster)

        for config_file in grep_res.keys():
            #ToDo : Grep Output
            print grep_res[config_file]

    def do_count(self, line):
        if not line:
            raise ShellException("Could not understand grep request, " + \
                                 "see 'help grep'")

        mods = self.parseModifiers(line)
        line = mods['line']

        tline = line[:]
        search_str = ""
        while tline:
            word = tline.pop(0)
            if word == '-s':
                search_str = tline.pop(0)
                search_str = self.stripString(search_str)
            else:
                raise ShellException(
                    "Do not understand '%s' in '%s'"%(word,
                                                      " ".join(line)))
        grep_res = {}
        if search_str:
            grep_res = common.grepper.grepCount(search_str, self.grep_cluster)

        for config_file in grep_res.keys():
            #ToDo : Grep Count Output
            print grep_res[config_file]

    def do_latency(self, line):
        if not line:
            raise ShellException("Could not understand grep request, " + \
                                 "see 'help grep'")

        mods = self.parseModifiers(line)
        line = mods['line']

        tline = line[:]
        search_str = ""
        while tline:
            word = tline.pop(0)
            if word == '-s':
                search_str = tline.pop(0)
                search_str = self.stripString(search_str)
            else:
                raise ShellException(
                    "Do not understand '%s' in '%s'"%(word,
                                                      " ".join(line)))
        grep_res = {}

        if search_str:
            grep_res = common.grepper.grepLatency(search_str, self.grep_cluster)

        for config_file in grep_res.keys():
            #ToDo : Grep Latency Output
            print config_file
            for val in grep_res[config_file]:
                print val

    def stripString(self, search_str):
        if search_str[0] == "\"" or search_str[0] == "\'":
            return search_str[1:len(search_str)-1]
        else:
            return search_str


@CommandHelp('"grep" searches for lines with input string in logs.',
             '  Options:',
             '    -s <string>  - The String to search in log files')
class GrepClusterController(CommandController):
    def __init__(self):
        self.modifiers = set()
        self.grep_file = GrepFile(True, self.modifiers)

    @CommandHelp('Displays all possible results from logs')
    def _do_default(self, line):
        self.grep_file.do_show(line)

    @CommandHelp('Displays all possible results from logs')
    def do_show(self, line):
        self.grep_file.do_show(line)

    @CommandHelp('Displays number of occurances of input string in logs')
    def do_count(self, line):
        self.grep_file.do_count(line)

    @CommandHelp('Displays difference between consecutive results from logs.',
                 'Currently it is working for format KEY<space>VALUE and',
                 'KEY<space>(Comma separated VALUE list).')
    def do_latency(self, line):
        self.grep_file.do_latency(line)


@CommandHelp('"grep" searches for lines with input string in logs.',
             '  Options:',
             '    -s <string>  - The String to search in log files')
class GrepServersController(CommandController):
    def __init__(self):
        self.modifiers = set()
        self.grep_file = GrepFile(False, self.modifiers)

    @CommandHelp('Displays all possible results from logs')
    def _do_default(self, line):
        self.grep_file.do_show(line)

    @CommandHelp('Displays all possible results from logs')
    def do_show(self, line):
        self.grep_file.do_show(line)

    @CommandHelp('Displays number of occurances of input string in logs')
    def do_count(self, line):
        self.grep_file.do_count(line)

    @CommandHelp('Displays difference between consecutive results from logs')
    def do_latency(self, line):
        self.grep_file.do_latency(line)
