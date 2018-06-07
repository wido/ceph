from itertools import chain
from datetime import datetime
from threading import Event, Thread
from Queue import Queue, Empty
import json
import errno
import time

from mgr_module import MgrModule

try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError
    from requests.exceptions import ConnectionError
except ImportError:
    InfluxDBClient = None


class Module(MgrModule):
    OPTIONS = [
            {
                'name': 'hostname',
                'default': None
            },
            {
                'name': 'port',
                'default': 8086
            },
            {
                'name': 'database',
                'default': 'ceph'
            },
            {
                'name': 'username',
                'default': None
            },
            {
                'name': 'password',
                'default': None
            },
            {
                'name': 'interval',
                'default': 30
            },
            {
                'name': 'ssl',
                'default': 'false'
            },
            {
                'name': 'verify_ssl',
                'default': 'true'
            },
            {
                'name': 'threads',
                'default': 4
            }
    ]

    @property
    def config_keys(self):
        return dict((o['name'], o.get('default', None))
                for o in self.OPTIONS)

    COMMANDS = [
        {
            "cmd": "influx config-set name=key,type=CephString "
                   "name=value,type=CephString",
            "desc": "Set a configuration value",
            "perm": "rw"
        },
        {
            "cmd": "influx config-show",
            "desc": "Show current configuration",
            "perm": "r"
        },
        {
            "cmd": "influx send",
            "desc": "Force sending data to Influx",
            "perm": "rw"
        },
        {
            "cmd": "influx self-test",
            "desc": "debug the module",
            "perm": "rw"
        },
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = True
        self.config = dict()

    def get_fsid(self):
        return self.get('mon_map')['fsid']

    @staticmethod
    def get_timestamp():
        return datetime.utcnow().isoformat() + 'Z'

    @staticmethod
    def can_run():
        if InfluxDBClient is not None:
            return True, ""
        else:
            return False, "influxdb python module not found"

    def queue_worker(self, queue, client):
        while True:
            try:
                self.log.debug('Fetching item from queue')
                point = queue.get()

                self.log.debug('Writing point to Influx')
                client.write_points([point], 'ms')
            except ConnectionError as e:
                self.log.exception("Failed to connect to Influx host %s:%d",
                                   self.config['hostname'], self.config['port'])
                self.set_health_checks({
                    'MGR_INFLUX_SEND_FAILED': {
                        'severity': 'warning',
                        'summary': 'Failed to send data to InfluxDB server at %s:%d'
                                   ' due to an connection error'
                                   % (self.config['hostname'], self.config['port']),
                        'detail': [str(e)]
                    }
                })
            except InfluxDBClientError as e:
                if e.code == 404:
                    self.log.info("Database '%s' not found, trying to create "
                                  "(requires admin privs).  You can also create "
                                  "manually and grant write privs to user "
                                  "'%s'", self.config['database'],
                                  self.config['username'])
                    client.create_database(self.config['database'])
                    client.create_retention_policy(name='8_weeks', duration='8w',
                                                   replication='1', default=True,
                                                   database=self.config['database'])
                else:
                    self.set_health_checks({
                        'MGR_INFLUX_SEND_FAILED': {
                            'severity': 'warning',
                            'summary': 'Failed to send data to InfluxDB',
                            'detail': [str(e)]
                        }
                    })
                    self.log.exception('Failed to send data to InfluxDB')
            except Empty:
                continue
            except:
                self.log.exception('Unhandled Exception while sending to Influx')
            finally:
                queue.task_done()

    def get_latest(self, daemon_type, daemon_name, stat):
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]

        return 0

    def get_df_stats(self):
        df = self.get("df")
        data = []
        pool_info = {}

        now = self.get_timestamp()

        df_types = [
            'bytes_used',
            'kb_used',
            'dirty',
            'rd',
            'rd_bytes',
            'raw_bytes_used',
            'wr',
            'wr_bytes',
            'objects',
            'max_avail',
            'quota_objects',
            'quota_bytes'
        ]
        
        for df_type in df_types:
            for pool in df['pools']:
                point = {
                    "measurement": "ceph_pool_stats",
                    "tags": {
                        "pool_name": pool['name'],
                        "pool_id": pool['id'],
                        "type_instance": df_type,
                        "fsid": self.get_fsid()
                    },
                    "time": now,
                    "fields": {
                        "value": pool['stats'][df_type],
                    }
                }
                data.append(point)
                pool_info.update({str(pool['id']):pool['name']})
        return data, pool_info

    def get_pg_summary_osd(self):
        now = self.get_timestamp()
        pg_sum = self.get('pg_summary')
        osd_sum = pg_sum['by_osd']

        for osd_id, stats in osd_sum.iteritems():
            metadata = self.get_metadata('osd', '%s' % osd_id)
            for stat in stats:
                yield {
                    'measurement': 'ceph_pg_summary_osd',
                    'tags': {
                        'ceph_daemon': 'osd.{0}'.format(osd_id),
                        'type_instance': stat,
                        'host': metadata['hostname'],
                        'fsid': self.get_fsid()
                    },
                    'time': now,
                    'fields': {
                        'value': stats[stat]
                    }
                }

    def get_pg_summary_pool(self, pool_info):
        now = self.get_timestamp()
        pg_sum = self.get('pg_summary')
        pool_sum = pg_sum['by_pool']

        for pool_id, stats in pool_sum.iteritems():
            for stat in stats:
                yield {
                    'measurement': 'ceph_pg_summary_pool',
                    'tags': {
                        'pool_name': pool_info[pool_id],
                        'pool_id': pool_id,
                        'type_instance': stat,
                        'fsid': self.get_fsid()
                    },
                    'time': now,
                    'fields': {
                        'value': stats[stat],
                    }
                }

    def get_daemon_stats(self):
        now = self.get_timestamp()

        for daemon, counters in self.get_all_perf_counters().iteritems():
            svc_type, svc_id = daemon.split(".", 1)
            metadata = self.get_metadata(svc_type, svc_id)

            for path, counter_info in counters.items():
                if counter_info['type'] & self.PERFCOUNTER_HISTOGRAM:
                    continue

                value = counter_info['value']

                yield {
                    "measurement": "ceph_daemon_stats",
                    "tags": {
                        "ceph_daemon": daemon,
                        "type_instance": path,
                        "host": metadata['hostname'],
                        "fsid": self.get_fsid()
                    },
                    "time": now,
                    "fields": {
                        "value": value
                    }
                }

    def set_config_option(self, option, value):
        if option not in self.config_keys.keys():
            raise RuntimeError('{0} is a unknown configuration '
                               'option'.format(option))

        if option in ['port', 'interval', 'threads']:
            try:
                value = int(value)
            except (ValueError, TypeError):
                raise RuntimeError('invalid {0} configured. Please specify '
                                   'a valid integer'.format(option))

        if option == 'interval' and value < 5:
            raise RuntimeError('interval should be set to at least 5 seconds')

        if option == 'threads':
            if 1 > value > 32:
                raise RuntimeError('threads should be in range 1-32')

        if option in ['ssl', 'verify_ssl']:
            value = value.lower() == 'true'

        self.config[option] = value

    def init_module_config(self):
        self.config['hostname'] = \
            self.get_config("hostname", default=self.config_keys['hostname'])
        self.config['port'] = \
            int(self.get_config("port", default=self.config_keys['port']))
        self.config['database'] = \
            self.get_config("database", default=self.config_keys['database'])
        self.config['username'] = \
            self.get_config("username", default=self.config_keys['username'])
        self.config['password'] = \
            self.get_config("password", default=self.config_keys['password'])
        self.config['interval'] = \
            int(self.get_config("interval",
                                default=self.config_keys['interval']))
        self.config['threads'] = \
            int(self.get_config("threads",
                                default=self.config_keys['threads']))
        ssl = self.get_config("ssl", default=self.config_keys['ssl'])
        self.config['ssl'] = ssl.lower() == 'true'
        verify_ssl = \
            self.get_config("verify_ssl", default=self.config_keys['verify_ssl'])
        self.config['verify_ssl'] = verify_ssl.lower() == 'true'

    def gather_statistics(self, queue):
        df_stats, pools = self.get_df_stats()

        points = chain(
            df_stats,
            self.get_daemon_stats(),
            self.get_pg_summary_osd(),
            self.get_pg_summary_pool(pools)
        )

        for point in points:
            queue.put(point)

    def send_to_influx(self):
        if not self.config['hostname']:
            self.log.error("No Influx server configured, please set one using: "
                           "ceph influx config-set hostname <hostname>")

            self.set_health_checks({
                'MGR_INFLUX_NO_SERVER': {
                    'severity': 'warning',
                    'summary': 'No InfluxDB server configured',
                    'detail': ['Configuration option hostname not set']
                }
            })
            time.sleep(1)
            return

        self.set_health_checks(dict())

        self.log.debug('Sending data to Influx host: %s',
                       self.config['hostname'])

        client = InfluxDBClient(self.config['hostname'],
                                self.config['port'],
                                self.config['username'],
                                self.config['password'],
                                self.config['database'],
                                self.config['ssl'],
                                self.config['verify_ssl'])

        queue = Queue()

        self.log.debug('Starting %d queue worker threads',
                       self.config['threads'])
        workers = list()
        for i in range(self.config['threads']):
            worker = Thread(target=self.queue_worker, args=(queue, client))
            worker.setDaemon(True)
            worker.start()
            workers.append(worker)

        self.log.info('Gathering statistics and putting them in queue')
        self.gather_statistics(queue)

        self.log.info('Waiting for queue to drain (%d items remaining)',
                      queue.qsize())
        queue.join()

        self.log.debug('Shutting down queue workers')
        for worker in workers:
            worker.join(0.1)

    def shutdown(self):
        self.log.info('Stopping influx module')
        self.run = False
        self.event.set()

    def handle_command(self, cmd):
        if cmd['prefix'] == 'influx config-show':
            return 0, json.dumps(self.config), ''
        elif cmd['prefix'] == 'influx config-set':
            key = cmd['key']
            value = cmd['value']
            if not value:
                return -errno.EINVAL, '', 'Value should not be empty or None'

            self.log.debug('Setting configuration option %s to %s', key, value)
            self.set_config_option(key, value)
            self.set_config(key, value)
            return 0, 'Configuration option {0} updated'.format(key), ''
        elif cmd['prefix'] == 'influx send':
            self.send_to_influx()
            return 0, 'Sending data to Influx', ''
        if cmd['prefix'] == 'influx self-test':
            daemon_stats = self.get_daemon_stats()
            assert len(daemon_stats)
            df_stats, pools = self.get_df_stats()

            result = {
                'daemon_stats': daemon_stats,
                'df_stats': df_stats
            }

            return 0, json.dumps(result, indent=2), 'Self-test OK'

        return (-errno.EINVAL, '',
                "Command not found '{0}'".format(cmd['prefix']))

    def serve(self):
        if InfluxDBClient is None:
            self.log.error("Cannot transmit statistics: influxdb python "
                           "module not found.  Did you install it?")
            return

        self.log.info('Starting influx module')
        self.init_module_config()
        self.run = True

        while self.run:
            start = time.time()
            self.send_to_influx()
            runtime = time.time() - start
            self.log.debug('Finished sending data in Influx in %.3f seconds',
                           runtime)
            self.log.debug("Sleeping for %d seconds", self.config['interval'])
            self.event.wait(self.config['interval'])
