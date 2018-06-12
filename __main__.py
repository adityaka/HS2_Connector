from hive_service import ThriftHive
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-hn", "--hostname",dest="hostname", required="True", help="Hostname or IP address")
parser.add_argument("-p", "--port", dest="port", required=True, help="Port Number for HS2")
parser.add_argument("-q", "--query", dest="query", help="Query statement", required=True)

args = parser.parse_args()

try:
    transport = TSocket.TSocket(args.hostname, args.port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = ThriftHive.Client(protocol)
    transport.open()

    client.execute(args.query)
    print client.fetchAll()

    transport.close()

except Thrift.TException, tx:
    print '%s' % (tx.message)