"""
Custom xmlrpc handling for Koji
"""

import types
import xmlrpc.client

# duplicate a few values that we need
getparser = xmlrpc.client.getparser
loads = xmlrpc.client.loads
Fault = xmlrpc.client.Fault
DateTime = xmlrpc.client.DateTime


class ExtendedMarshaller(xmlrpc.client.Marshaller):

    dispatch = xmlrpc.client.Marshaller.dispatch.copy()

    def _dump(self, value, write):
        # Parent class is unfriendly to subclasses :-/
        f = self.dispatch[type(value)]
        f(self, value, write)

    def dump_generator(self, value, write):
        dump = self._dump
        write("<value><array><data>\n")
        for v in value:
            dump(v, write)
        write("</data></array></value>\n")
    dispatch[types.GeneratorType] = dump_generator

    MAXI8 = 2 ** 63 - 1
    MINI8 = -2 ** 63

    def dump_int(self, value, write):
        # python2's xmlrpclib doesn't support i8 extension for marshalling,
        # but can unmarshall it correctly.
        if (value > self.MAXI8 or value < self.MINI8):
            raise OverflowError("long int exceeds XML-RPC limits")
        elif (value > xmlrpc.client.MAXINT or
                value < xmlrpc.client.MININT):
            write("<value><i8>")
            write(str(int(value)))
            write("</i8></value>\n")
        else:
            return xmlrpc.client.Marshaller.dump_int(self, value, write)
    dispatch[int] = dump_int


def dumps(params, methodname=None, methodresponse=None, encoding=None,
          allow_none=1, marshaller=None):
    """encode an xmlrpc request or response

    Differences from the xmlrpclib version:
        - allow_none is on by default
        - uses our ExtendedMarshaller by default
        - option to specify marshaller
    """

    if isinstance(params, Fault):
        methodresponse = 1
    elif not isinstance(params, tuple):
        raise TypeError('params must be a tuple or Fault instance')
    elif methodresponse and len(params) != 1:
        raise ValueError('response tuple must be a singleton')

    if not encoding:
        encoding = "utf-8"

    if marshaller is not None:
        m = marshaller(encoding, allow_none=True)
    else:
        m = ExtendedMarshaller(encoding, allow_none=True)

    data = m.dumps(params)

    if encoding != "utf-8":
        xmlheader = "<?xml version='1.0' encoding='%s'?>\n" % str(encoding)
    else:
        xmlheader = "<?xml version='1.0'?>\n"  # utf-8 is default

    # standard XML-RPC wrappings
    if methodname:
        # a method call
        parts = (
            xmlheader,
            "<methodCall>\n"
            "<methodName>", methodname, "</methodName>\n",
            data,
            "</methodCall>\n"
        )
    elif methodresponse:
        # a method response, or a fault structure
        parts = (
            xmlheader,
            "<methodResponse>\n",
            data,
            "</methodResponse>\n"
        )
    else:
        return data  # return as is
    return ''.join(parts)
