from kojiweb.util import WWWPlugin

class RunrootWWWPlugin(WWWPlugin):
    methods = ['runroot']

    def template_taskinfo(self):
        return '/usr/lib/koji-web-plugins/runroot_taskinfo.chtml'
