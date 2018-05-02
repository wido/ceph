import json
from threading import Event
from mgr_module import MgrModule


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "query get name=key,type=CephString",
            "desc": "Query the Manager Module",
            "perm": "rw"
        }
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.run = True
        self.event = Event()
        self.config = dict()

    def shutdown(self):
        self.log.info('Stopping Query module')
        self.run = False
        self.event.set()

    def serve(self):
        self.log.info('Starting Query module')
        self.run = True

        while self.run:
            self.log.debug("Waking up for a new cycle")
            self.event.wait(5)

    def handle_command(self, cmd):
        if cmd['prefix'] == 'query get':
            key = cmd['key']
            if not key:
                return -errno.EINVAL, '', 'Key should not be empty or None'

            return 0, json.dumps(self.get(key)), ''

        return (-errno.EINVAL, '',
                "Command not found '{0}'".format(cmd['prefix']))
