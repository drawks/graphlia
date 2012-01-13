#!/usr/bin/env python2.6

from amqplib import client_0_8 as amqp
from string import translate, maketrans
from xdrlib import Unpacker, Packer
import ConfigParser
import SocketServer
import socket
import string
import struct
import sys
import threading
import time


GANGLIA_LISTEN_PORT = 8649

magic_nums = { 128 : "metadata",
#129 and 130 should be int16/uint16 but since xdr is padded to 4 bytes
#they don't require sepcial handling afaict.
    129: lambda x:x.unpack_int(),
    130: lambda x:x.unpack_uint(),
    131: lambda x:x.unpack_int(),
    132: lambda x:x.unpack_uint(),
    133: lambda x:x.unpack_string(),
    134: lambda x:x.unpack_float(),
    135: lambda x:x.unpack_double(),
    136: "metadata request"
    }

mdata_hash = {}


class GangliaCollector(SocketServer.BaseRequestHandler):
    def handle(self):
        data = self.request[0]
        values = dict()
        unpacker = Unpacker(data)
        packet_type = unpacker.unpack_uint()
        if packet_type == 128:
            self.unpack_meta(unpacker)
        elif packet_type == 136:
            self.unpack_metareq(unpacker)
        elif 128 < packet_type < 136:
            self.unpack_data(unpacker,packet_type)
        else:
            pass

    def unpack_meta(self,unpacker):
        values = {}
        values['hostname'] = unpacker.unpack_string()
        if len(values['hostname'].split(':')) == 2:
            (values['hostaddr'],values['hostname']) = values['hostname'].split(':')
        values['metricname'] = unpacker.unpack_string()
        values['spoof'] = unpacker.unpack_int()
        values['metrictype'] = unpacker.unpack_string()
        values['metricname2'] = unpacker.unpack_string()
        values['metricunits'] = unpacker.unpack_string()
        values['slope'] = unpacker.unpack_int()
        values['tmax'] = unpacker.unpack_int()
        values['dmax'] = unpacker.unpack_int()
        values['num_elements'] = unpacker.unpack_int()
        i = 0
        while i < values['num_elements']:
            k = unpacker.unpack_string()
            v = unpacker.unpack_string()
            values[k] = v
            i += 1
        unpacker.done()
        print "Updating metadata for %s on %s" % (values['hostname'],values['metricname'])
        mdata_hash[(values['hostname'],values['metricname'])] = values
        return

    def unpack_data(self,unpacker,packet_type):
        values = {}
        values['hostname'] = unpacker.unpack_string()
        if len(values['hostname'].split(':')) == 2:
            (values['hostaddr'],values['hostname']) = values['hostname'].split(':')
        values['metricname'] = unpacker.unpack_string()
        values['spoof'] = unpacker.unpack_int()
        values['format'] = unpacker.unpack_string()
        values['value'] = magic_nums[packet_type](unpacker)
        unpacker.done()
        if (values['hostname'],values['metricname']) not in mdata_hash:
            print "Requesting metadata for %s on %s"  % (values['hostname'],values['metricname'])
            self.send_metareq(values)
            return
        else:
            values.update(mdata_hash[(values['hostname'],values['metricname'])])
            graphite.record_stat(values)
            return

    def unpack_metareq(self,unpacker):
        '''Haven't caught one of these yet'''
        print "BANG!"*16
        values = {}
        values['hostname']=unpacker.unpack_string()
        values['metricname']=unpacker.unpack_string()
        values['spoof']=unpacker.unpack_int()
        unpacker.done()
        return

    def send_metareq(self,values):
        sock = self.request[1]
        packer = Packer()
        packer.pack_int(136)
        packer.pack_string(values['hostname'])
        packer.pack_string(values['metricname'])
        packer.pack_bool(values['spoof'])
        sock.sendto(packer.get_buffer(),socket.MSG_EOR,self.client_address)
        return


class GraphiteAggregator(object):
    def __init__(self, host, mappings, rates, apply_formats, amqp_conn, amqp_exchange):
        self.host = host
        self.mappings = mappings
        self.use_rates = rates
        self.apply_formats = apply_formats
        self.amqp_conn = amqp_conn
        self.amqp_exchange = amqp_exchange
        self.amqp_chan = self.amqp_conn.channel()

        self.last_value = {}
        self.last_time = {}
        self.stats_lock = threading.Lock()
        self.stats = []

        update_thread = threading.Thread(target=self.send_updates_thread)
        update_thread.setDaemon(True)
        update_thread.start()

    def record_stat(self, values):
        if values['metricname'] in self.mappings['custom_mappings']:
            name = self.mappings['custom_mappings'][name].safe_substitute(values)
        else:
            name = self.mappings['default_mapping'].safe_substitute(values)
        #name = translate(orig_name,maketrans('_/','..'))
        if self.apply_formats:
            value = values['format'] % values['value']
        else:
            value = values['value']
        now = time.time()
        if name in self.use_rates:
            if name not in self.last_value:
                self.last_value[name] = value
                self.last_time[name] = now
                return
            if value < self.last_value[name]:
                # counter rollover?
                self.last_value[name] = value
                self.last_time[name] = now
                return
            rate = (value - self.last_value[name]) / \
                   (now - self.last_time[name])
            self.last_value[name] = value
            self.last_time[name] = now
            value = rate

        # lock, append to updatequeue name/value, unlock
        self.stats_lock.acquire()
        self.stats.append((name, value, now))
        self.stats_lock.release()

    def send_updates_thread(self):
        while True:
            time.sleep(10)
            try:
                self.send_updates()
            except Exception as e:
                print >>sys.stderr, "Error sending updates: %s" % (e,)

    def send_updates(self):
        if len(self.stats) == 0: return

        self.stats_lock.acquire()
        stats = self.stats[:]
        self.stats = []
        self.stats_lock.release()

        output = []
        for name, value, now in stats:
            output.append(self._stat(name, value, now))

        print "\n".join(output)
        msg = amqp.Message("\n".join(output))
        try:
            self.amqp_chan.basic_publish(msg, exchange=self.amqp_exchange)
        except socket.error as (errno, errstr):
            print >> sys.stderr, "amqp publish problem: %s" % (errstr,)
            sys.exit(2)

    def _stat(self, name, value, now):
        if self.apply_formats:
            return "%s %s %d" % (name, value, int(now))
        else:
            return "%s %r %d" % (name, value, int(now))

class McastSocket(socket.socket):
    def __init__(self, bindaddress=None, port=None):
        socket.socket.__init__(self, socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        if bindaddress is None:
            self.intf = socket.gethostbyname(socket.gethostname())
        else:
            self.intf = bindaddress
        self.group = ('', port)
        #self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(self,'SO_REUSEPORT'):
            self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_TTL, 255)
        self.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP, 1)
        try:
            self.bind(self.group)
        except:
            print "bind failed"
            pass

    def join(self,channel):
        self.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.intf) + socket.inet_aton('0.0.0.0'))
        self.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(channel) + socket.inet_aton('0.0.0.0'))

class McastServer(SocketServer.UDPServer):
    allow_reuse_address = True

    def __init__(self, server_address, RequestHandlerClass):
        SocketServer.UDPServer.__init__(self, server_address, RequestHandlerClass)
        self.socket = McastSocket(port=server_address[1])
        self.socket.join(server_address[0])


if __name__ == "__main__":

    default_config = '''
    [amqp]
    host = localhost
    user = guest
    pass = guest
    vhost = /
    exchange = stats

    [gmond]
    multicast = True
    ip = 239.2.11.71
    port = 8649

    [mapping]
    default = ganglia.${hostname}.${GROUP}.${metricname}
    '''

    config = ConfigParser.ConfigParser()
    if len(sys.argv) > 1:
        config.read(sys.argv[1])
    else:
        config.read("/etc/graphlia.ini")

    mappings = {}
    mappings['default_mapping'] = string.Template(config.get("mapping","default", raw=True))
    mappings['custom_mappings'] = dict([ (metric,string.Template(template)) for
        (metric,template) in config.items('mapping',raw=True)])
    use_rates = []
    apply_formats = True

    amqp_conn = amqp.Connection(host=config.get("amqp", "host"),
                                userid=config.get("amqp", "user"),
                                password=config.get("amqp", "pass"),
                                virtual_host=config.get("amqp", "vhost"),
                                insist=False)
    amqp_exchange = config.get("amqp", "exchange")

    graphite = GraphiteAggregator(socket.gethostname(), mappings, use_rates,
            apply_formats, amqp_conn, amqp_exchange)

    if config.getboolean("gmond","multicast"):
        server = McastServer((config.get("gmond","ip"),
            config.getint("gmond","port")), GangliaCollector)
    else:
        server = SocketServer.UDPServer((config.get("gmond","ip"),
            config.getint("gmond","port")), GangliaCollector)

    server.serve_forever()
