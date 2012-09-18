from collections import deque
from string import translate, maketrans, Template
from xdrlib import Unpacker, Packer
import ConfigParser
import pickle
import SocketServer
import socket
import struct
import sys
import threading
import time
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor

magic_nums = {128: "metadata",
#129 and 130 should be int16/uint16 but since xdr is padded to 4 bytes
#they don't require sepcial handling afaict.
    129: lambda x: x.unpack_int(),
    130: lambda x: x.unpack_uint(),
    131: lambda x: x.unpack_int(),
    132: lambda x: x.unpack_uint(),
    133: lambda x: x.unpack_string(),
    134: lambda x: x.unpack_float(),
    135: lambda x: x.unpack_double(),
    136: "metadata request"
    }

mdata_hash = {}

class GangliaClient(DatagramProtocol):
    def __init__(self, mcast_group='239.2.11.71', mcast_port=8649, ttl=5, *args, **kwargs):
        self.mcast_group = mcast_group
        self.mcast_port = mcast_port
        self.ttl = ttl
        #DatagramProtocol.__init__(self, *args, **kwargs)

    def startProtocol(self):
        """
        Called after protocol has started listening.
        """
        # Set the TTL>1 so multicast will cross router hops:
        self.transport.setTTL(self.ttl)
        # Join a specific multicast group:
        self.transport.joinGroup(self.mcast_group)

    def datagramReceived(self, datagram, address):
        values = dict()
        unpacker = Unpacker(datagram)
        packet_type = unpacker.unpack_uint()
        if packet_type == 128:
            self.unpack_meta(unpacker)
            return
        elif packet_type == 136:
            #unpack_metareq function works, but serves no purpose right now
            #commented out unless anyone comes up with a good reason to respond
            #to metadata requests.
            #self.unpack_metareq(unpacker)
            return
        elif 128 < packet_type < 136:
            self.unpack_data(unpacker, packet_type, address)
            return
        else:
            return

    def unpack_meta(self, unpacker):
        values = {}
        values['hostname'] = unpacker.unpack_string()
        if len(values['hostname'].split(':')) == 2:
            (values['hostaddr'], values['hostname']) = values['hostname'].split(':')
        values['metricname'] = unpacker.unpack_string()
        values['spoof'] = unpacker.unpack_bool()
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
        print "Updating metadata for %s on %s" % (values['hostname'], values['metricname'])
        mdata_hash[(values['hostname'], values['metricname'])] = values
        return

    def unpack_data(self, unpacker, packet_type, address):
        values = {}
        values['hostname'] = unpacker.unpack_string()
        if len(values['hostname'].split(':')) == 2:
            (values['hostaddr'], values['hostname']) = values['hostname'].split(':')
        values['metricname'] = unpacker.unpack_string()
        values['spoof'] = unpacker.unpack_bool()
        values['format'] = unpacker.unpack_string()
        values['value'] = magic_nums[packet_type](unpacker)
        unpacker.done()
        if values['metricname'] == 'heartbeat':
            return
        elif (values['hostname'], values['metricname']) in mdata_hash:
            values.update(mdata_hash[(values['hostname'], values['metricname'])])
            #print values
            graphite.record_stat(values)
            return
        else:
            print "Requesting metadata for %s on %s" % (values['hostname'], values['metricname'])
            self.send_metareq(values, (self.mcast_group,self.mcast_port))
            return

    def unpack_metareq(self, unpacker):
        values = {}
        values['hostname'] = unpacker.unpack_string()
        values['metricname'] = unpacker.unpack_string()
        values['spoof'] = unpacker.unpack_bool()
        unpacker.done()
        return

    def send_metareq(self, values, address):
        packer = Packer()
        packer.pack_int(136)
        if not values['spoof']:
            packer.pack_string(address[0])
        else:
            packer.pack_string(":".join((address[0], values['hostname'])))
        packer.pack_string(values['metricname'])
        packer.pack_bool(values['spoof'])
        self.transport.write(packer.get_buffer(),address)
        return




class GraphiteAggregator(object):
    def __init__(self, host, mappings, sanitize_names, apply_formats,
            amqp_spec=(None, None), carbon_spec=(None, None)):
        self.carbon_spec = carbon_spec
        self.host = host
        self.mappings = mappings
        self.sanitize_names = sanitize_names
        self.apply_formats = apply_formats
        self.amqp_conn = amqp_spec[0]
        self.amqp_exchange = amqp_spec[1]
        if carbon_spec[0] and carbon_spec[1]:
            self.carbon = True
            self.carbon_queue = []
            self.carbon_messages = []
        else:
            self.carbon = False
        if amqp_spec[0] and amqp_spec[1]:
            self.amqp = True
            self.amqp_queue = []
            self.amqp_chan = self.amqp_conn.channel()
        else:
            self.amqp = False

        self.last_value = {}
        self.last_time = {}
        self.stats_lock = threading.Lock()
        self.stats = {}
        #self.stats = deque()

        update_thread = threading.Thread(target=self.send_updates_thread)
        update_thread.setDaemon(True)
        update_thread.start()

    def record_stat(self, values):
        if self.sanitize_names:
            #replace periods in metric names with underbars to prevent them
            #from indicating a tree node in graphite style consumers
            values['metricname'] = translate(values['metricname'], maketrans('.', '_'))
        #If there is a custom mapping that matches the current metric apply it
        #otherwise fall back to the default mapping. Any key available in values
        #dict is valid for substitution. safe_substitue prevents a bad key
        #from raising an exception and instead just lets the macro through with
        #no substitution.
        if values['metricname'] in self.mappings['custom_mappings']:
            name = self.mappings['custom_mappings'][name].safe_substitute(values)
        else:
            name = self.mappings['default_mapping'].safe_substitute(values)
        if self.apply_formats:
            value = values['format'] % values['value']
        else:
            value = values['value']
        now = int(time.time())
        #wait on lock to insert
        with self.stats_lock:
            self.stats[(values['hostname'], values['metricname'])] = (name, now, value)

    def send_updates_thread(self):
        while True:
            t = time.time()
            try:
                with self.stats_lock:
                    working_set = self.cull_stats()
                    if self.carbon:
                        self.carbon_queue.extend(working_set)
                    if self.amqp:
                        self.amqp_queue.extend(working_set)
                if self.amqp:
                    self.send_amqp()
                if self.carbon:
                    self.send_carbon()
            except Exception as e:
                print >>sys.stderr, "Error sending updates: %s" % (e,)
            #send no more than twice a minute
            time.sleep(30 - (time.time() - t))

    def cull_stats(self):
        now = int(time.time())
        for item in self.stats.items():
            name = item[0]
            if now - self.stats[name][1] > mdata_hash[name]['tmax']:
                print('Dropping stale metric %s on %s' % name)
                del(self.stats[name])
        return [(stat[0], (now, stat[2])) for stat in self.stats.values()]

    def send_carbon(self):
        data = []
        print('Preparing %d metric updates' % len(self.carbon_queue))
        while self.carbon_queue:
            data.append(self.carbon_queue[:10000])
            del(self.carbon_queue[:10000])
        for chunk in data:
            serializedData = pickle.dumps(chunk, protocol=-1)
            prefix = struct.pack("!L", len(serializedData))
            self.carbon_messages.append(prefix + serializedData)
        try:
            connected = False
            sock = socket.create_connection(self.carbon_spec, timeout=5)
            connected = True
            num_messages = len(self.carbon_messages)
            print('Connected to carbon')
            print('Sending metrics in %d chunks' % num_messages)
            i = 0
            while self.carbon_messages:
                i += 1
                sock.sendall(self.carbon_messages[0])
                del(self.carbon_messages[0])
                print('Sent %d/%d' % (i, num_messages))
        except socket.timeout:
            print('Timed out while attempt to connect to %s:%r' % self.carbon_spec)
        except socket.error:
            print('Problem while sending to %s:%d' % self.carbon_spec)
        finally:
            if self.carbon_messages:
                print("Queued %d messages for later delivery" % len(self.carbon_messages))
            if connected:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                    sock.close()
                except:
                    pass

    def send_amqp(self):
        for (name, (now, value)) in self.amqp_queue:
            try:
                msg = self._stat(value, now)
                msg = amqp.Message(msg)
                self.amqp_chan.basic_publish(msg, exchange=self.amqp_exchange, routing_key=name)
            except socket.error as (errno, errstr):
                print >> sys.stderr, "amqp publish problem: %s" % (errstr,)
                sys.exit(2)

    def _stat(self, value, now):
        if self.apply_formats:
            return "%s %d" % (value, int(now))
        else:
            return "%r %d" % (value, int(now))


if __name__ == "__main__":

    default_config = '''
    [amqp]
    host = localhost
    user = guest
    pass = guest
    vhost = /
    exchange = stats
    enable = True

    [carbon]
    host = localhost
    port = 2004
    enable =False

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
    mappings['default_mapping'] = Template(config.get("mapping", "default", raw=True))
    mappings['custom_mappings'] = dict([(metric, Template(template)) for
        (metric, template) in config.items('mapping', raw=True)])
    apply_formats = True
    sanitize_names = True

    if config.getboolean("amqp", "enable"):
        amqp_conn = amqp.Connection(host=config.get("amqp", "host"),
                                    userid=config.get("amqp", "user"),
                                    password=config.get("amqp", "pass"),
                                    virtual_host=config.get("amqp", "vhost"),
                                    insist=False)
        amqp_exchange = config.get("amqp", "exchange")
        amqp_spec = (amqp_conn, amqp_exchange)
    else:
        amqp_spec = (None, None)

    if config.getboolean("carbon", "enable"):
        carbon_host = config.get("carbon", "host")
        carbon_port = config.get("carbon", "port")
        carbon_spec = (carbon_host, carbon_port)
    else:
        carbon_spec = (None, None)


    graphite = GraphiteAggregator(socket.gethostname(), mappings, sanitize_names,
            apply_formats, amqp_spec, carbon_spec)

    if config.getboolean("gmond","multicast"):
        #server = McastServer((config.get("gmond","ip"),
        #    config.getint("gmond","port")), GangliaCollector)
        reactor.listenMulticast(config.getint("gmond","port"), GangliaClient(),
                        listenMultiple=True)
reactor.run()
#    else:
#        server = SocketServer.UDPServer((config.get("gmond","ip"),
#            config.getint("gmond","port")), GangliaCollector)

