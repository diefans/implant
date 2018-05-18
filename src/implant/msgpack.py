# u-msgpack-python v2.4.1 - v at sergeev.io
# https://github.com/vsergeev/u-msgpack-python
#
# u-msgpack-python is a lightweight MessagePack serializer and deserializer
# module, compatible with both Python 2 and 3, as well CPython and PyPy
# implementations of Python. u-msgpack-python is fully compliant with the
# latest MessagePack specification.com/msgpack/msgpack/blob/master/spec.md). In
# particular, it supports the new binary, UTF-8 string, and application ext
# types.
#
# MIT License
#
# Copyright (c) 2013-2016 vsergeev / Ivan (Vanya) A. Sergeev
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
"""This is a modified Python 3 only version of umsgpack."""

import abc
import collections
import io
import struct
import sys


##############################################################################
# Ext Class
##############################################################################
# Extension type for application-defined types and data
class Ext:

    """
    The Ext class facilitates creating a serializable extension object to store
    an application-defined type and data byte array.
    """

    def __init__(self, type, data):
        """
        Construct a new Ext object.

        Args:
            type: application-defined type integer from 0 to 127
            data: application-defined data byte array

        Raises:
            TypeError:
                Specified ext type is outside of 0 to 127 range.

        Example:
        >>> foo = umsgpack.Ext(0x05, b"\x01\x02\x03")
        >>> umsgpack.packb({u"special stuff": foo, u"awesome": True})
        '\x82\xa7awesome\xc3\xadspecial stuff\xc7\x03\x05\x01\x02\x03'
        >>> bar = umsgpack.unpackb(_)
        >>> print(bar["special stuff"])
        Ext Object (Type: 0x05, Data: 01 02 03)
        >>>
        """
        # Application ext type should be 0 <= type <= 127
        if not isinstance(type, int) or not (type >= 0 and type <= 127):
            raise TypeError("ext type out of range")
        # Check data is type bytes
        elif sys.version_info[0] == 3 and not isinstance(data, bytes):
            raise TypeError("ext data is not type \'bytes\'")
        elif sys.version_info[0] == 2 and not isinstance(data, str):
            raise TypeError("ext data is not type \'str\'")
        self.type = type
        self.data = data

    def __eq__(self, other):
        """Compare this Ext object with another for equality."""
        return (isinstance(other, self.__class__) and
                self.type == other.type and
                self.data == other.data)

    def __ne__(self, other):
        """Compare this Ext object with another for inequality."""
        return not self.__eq__(other)

    def __str__(self):
        """String representation of this Ext object."""
        s = "Ext Object (Type: 0x%02x, Data: " % self.type
        s += " ".join(["0x%02x" % ord(self.data[i:i + 1])
                       for i in range(min(len(self.data), 8))])
        if len(self.data) > 8:
            s += " ..."
        s += ")"
        return s

    def __hash__(self):
        """Provide a hash of this Ext object."""
        return hash((self.type, self.data))


class InvalidString(bytes):

    """Subclass of bytes to hold invalid UTF-8 strings."""

##############################################################################
# Exceptions
##############################################################################


# Base Exception classes
class PackException(Exception):

    """Base class for exceptions encountered during packing."""


class UnpackException(Exception):

    """Base class for exceptions encountered during unpacking."""


# Packing error
class UnsupportedTypeException(PackException):

    """Object type not supported for packing."""


# Unpacking error
class InsufficientDataException(UnpackException):

    """Insufficient data to unpack the serialized object."""


class InvalidStringException(UnpackException):

    """Invalid UTF-8 string encountered during unpacking."""


class ReservedCodeException(UnpackException):

    """Reserved code encountered during unpacking."""


class UnhashableKeyException(UnpackException):

    """Unhashable key encountered during map unpacking.

    The serialized map cannot be deserialized into a Python dictionary.
    """


class DuplicateKeyException(UnpackException):

    """Duplicate key encountered during map unpacking."""


##############################################################################
# Packing
##############################################################################

# You may notice struct.pack("B", obj) instead of the simpler chr(obj) in the
# code below. This is to allow for seamless Python 2 and 3 compatibility, as
# chr(obj) has a str return type instead of bytes in Python 3, and
# struct.pack(...) has the right return type in both versions.
class Encoder:

    """Provides methods for msgpack packing."""

    # Auto-detect system float precision
    if sys.float_info.mant_dig == 53:
        _float_precision = "double"
    else:
        _float_precision = "single"

    @classmethod                        # noqa
    def _pack_integer(cls, obj, fp, options):
        if obj < 0:
            if obj >= -32:
                fp.write(struct.pack("b", obj))
            elif obj >= -2**(8 - 1):
                fp.write(b"\xd0" + struct.pack("b", obj))
            elif obj >= -2**(16 - 1):
                fp.write(b"\xd1" + struct.pack(">h", obj))
            elif obj >= -2**(32 - 1):
                fp.write(b"\xd2" + struct.pack(">i", obj))
            elif obj >= -2**(64 - 1):
                fp.write(b"\xd3" + struct.pack(">q", obj))
            else:
                raise UnsupportedTypeException("huge signed int")
        else:
            if obj <= 127:
                fp.write(struct.pack("B", obj))
            elif obj <= 2**8 - 1:
                fp.write(b"\xcc" + struct.pack("B", obj))
            elif obj <= 2**16 - 1:
                fp.write(b"\xcd" + struct.pack(">H", obj))
            elif obj <= 2**32 - 1:
                fp.write(b"\xce" + struct.pack(">I", obj))
            elif obj <= 2**64 - 1:
                fp.write(b"\xcf" + struct.pack(">Q", obj))
            else:
                raise UnsupportedTypeException("huge unsigned int")

    @classmethod
    def _pack_nil(cls, obj, fp, options):
        fp.write(b"\xc0")

    @classmethod
    def _pack_boolean(cls, obj, fp, options):
        fp.write(b"\xc3" if obj else b"\xc2")

    @classmethod
    def _pack_float(cls, obj, fp, options):
        float_precision = options.get('force_float_precision',
                                      cls._float_precision)

        if float_precision == "double":
            fp.write(b"\xcb" + struct.pack(">d", obj))
        elif float_precision == "single":
            fp.write(b"\xca" + struct.pack(">f", obj))
        else:
            raise ValueError("invalid float precision")

    @classmethod
    def _pack_string(cls, obj, fp, options):
        obj = obj.encode('utf-8')
        if len(obj) <= 31:
            fp.write(struct.pack("B", 0xa0 | len(obj)) + obj)
        elif len(obj) <= 2**8 - 1:
            fp.write(b"\xd9" + struct.pack("B", len(obj)) + obj)
        elif len(obj) <= 2**16 - 1:
            fp.write(b"\xda" + struct.pack(">H", len(obj)) + obj)
        elif len(obj) <= 2**32 - 1:
            fp.write(b"\xdb" + struct.pack(">I", len(obj)) + obj)
        else:
            raise UnsupportedTypeException("huge string")

    @classmethod
    def _pack_binary(cls, obj, fp, options):
        if len(obj) <= 2**8 - 1:
            fp.write(b"\xc4" + struct.pack("B", len(obj)) + obj)
        elif len(obj) <= 2**16 - 1:
            fp.write(b"\xc5" + struct.pack(">H", len(obj)) + obj)
        elif len(obj) <= 2**32 - 1:
            fp.write(b"\xc6" + struct.pack(">I", len(obj)) + obj)
        else:
            raise UnsupportedTypeException("huge binary string")

    @classmethod
    def _pack_oldspec_raw(cls, obj, fp, options):
        if len(obj) <= 31:
            fp.write(struct.pack("B", 0xa0 | len(obj)) + obj)
        elif len(obj) <= 2**16 - 1:
            fp.write(b"\xda" + struct.pack(">H", len(obj)) + obj)
        elif len(obj) <= 2**32 - 1:
            fp.write(b"\xdb" + struct.pack(">I", len(obj)) + obj)
        else:
            raise UnsupportedTypeException("huge raw string")

    @classmethod
    def _pack_ext(cls, obj, fp, options):
        if len(obj.data) == 1:
            fp.write(b"\xd4" + struct.pack("B", obj.type & 0xff) + obj.data)
        elif len(obj.data) == 2:
            fp.write(b"\xd5" + struct.pack("B", obj.type & 0xff) + obj.data)
        elif len(obj.data) == 4:
            fp.write(b"\xd6" + struct.pack("B", obj.type & 0xff) + obj.data)
        elif len(obj.data) == 8:
            fp.write(b"\xd7" + struct.pack("B", obj.type & 0xff) + obj.data)
        elif len(obj.data) == 16:
            fp.write(b"\xd8" + struct.pack("B", obj.type & 0xff) + obj.data)
        elif len(obj.data) <= 2**8 - 1:
            fp.write(b"\xc7" +
                     struct.pack("BB", len(obj.data), obj.type & 0xff)
                     + obj.data)
        elif len(obj.data) <= 2**16 - 1:
            fp.write(b"\xc8" +
                     struct.pack(">HB", len(obj.data), obj.type & 0xff)
                     + obj.data)
        elif len(obj.data) <= 2**32 - 1:
            fp.write(b"\xc9" +
                     struct.pack(">IB", len(obj.data), obj.type & 0xff)
                     + obj.data)
        else:
            raise UnsupportedTypeException("huge ext data")

    @classmethod
    def _pack_array(cls, obj, fp, options):
        if len(obj) <= 15:
            fp.write(struct.pack("B", 0x90 | len(obj)))
        elif len(obj) <= 2**16 - 1:
            fp.write(b"\xdc" + struct.pack(">H", len(obj)))
        elif len(obj) <= 2**32 - 1:
            fp.write(b"\xdd" + struct.pack(">I", len(obj)))
        else:
            raise UnsupportedTypeException("huge array")

        for e in obj:
            cls.pack(e, fp, **options)

    @classmethod
    def _pack_map(cls, obj, fp, options):
        if len(obj) <= 15:
            fp.write(struct.pack("B", 0x80 | len(obj)))
        elif len(obj) <= 2**16 - 1:
            fp.write(b"\xde" + struct.pack(">H", len(obj)))
        elif len(obj) <= 2**32 - 1:
            fp.write(b"\xdf" + struct.pack(">I", len(obj)))
        else:
            raise UnsupportedTypeException("huge array")

        for k, v in obj.items():
            cls.pack(k, fp, **options)
            cls.pack(v, fp, **options)

    # Pack for Python 3, with unicode 'str' type, 'bytes' type,
    # and no 'long' type
    @classmethod                # noqa
    def pack(cls, obj, fp, **options):
        # pylint: disable=W0212
        ext_handlers = options.get("ext_handlers")
        # lookup mro except object for matching handler
        ext_handler_match = next((
            obj_cls for obj_cls in obj.__class__.__mro__[:-1]
            if obj_cls in ext_handlers
        ), None) if ext_handlers else None
        if obj is None:
            cls._pack_nil(obj, fp, options)
        elif ext_handler_match:
            cls._pack_ext(ext_handlers[ext_handler_match](obj), fp, options)
        elif isinstance(obj, bool):
            cls._pack_boolean(obj, fp, options)
        elif isinstance(obj, int):
            cls._pack_integer(obj, fp, options)
        elif isinstance(obj, float):
            cls._pack_float(obj, fp, options)
        elif isinstance(obj, str):
            cls._pack_string(obj, fp, options)
        elif isinstance(obj, bytes):
            cls._pack_binary(obj, fp, options)
        elif isinstance(obj, (tuple, list)):
            cls._pack_array(obj, fp, options)
        elif isinstance(obj, dict):
            cls._pack_map(obj, fp, options)
        elif isinstance(obj, Ext):
            cls._pack_ext(obj, fp, options)
        # default fallback
        elif ext_handlers and object in ext_handlers:
            cls._pack_ext(ext_handlers[object](obj), fp, options)
        else:
            raise UnsupportedTypeException(
                "unsupported type: %s" % str(type(obj)))

    @classmethod
    def packb(cls, obj, **options):
        r"""Serialize a Python object into MessagePack bytes.

        Args:
            obj: a Python object

        Kwargs:
            ext_handlers (dict): dictionary of Ext handlers,
                                 mapping a custom type to a callable that packs
                                 an instance of the type into an Ext object
            force_float_precision (str): "single" to force packing floats as
                                         IEEE-754 single-precision floats,
                                         "double" to force packing floats as
                                         IEEE-754 double-precision floats.

        Returns:
            A 'bytes' containing serialized MessagePack bytes.

        Raises:
            UnsupportedType(PackException):
                Object type not supported for packing.

        Example:
            >>> umsgpack.packb({u"compact": True, u"schema": 0})
            b'\x82\xa7compact\xc3\xa6schema\x00'
            >>>

        """
        fp = io.BytesIO()
        cls.pack(obj, fp, **options)
        return fp.getvalue()

    # Map packb and unpackb to the appropriate version
    dump = pack
    dumps = packb


class _DecoderMeta(type):

    _unpack_dispatch_table = {}

    def __new__(mcs, name, bases, dct):     # noqa
        cls = type.__new__(mcs, name, bases, dct)

        # Build a dispatch table for fast lookup of unpacking function
        # Fix uint
        for code in range(0, 0x7f + 1):
            cls._unpack_dispatch_table[struct.pack("B", code)]\
                = cls._unpack_integer
        # Fix map
        for code in range(0x80, 0x8f + 1):
            cls._unpack_dispatch_table[struct.pack("B", code)]\
                = cls._unpack_map
        # Fix array
        for code in range(0x90, 0x9f + 1):
            cls._unpack_dispatch_table[struct.pack("B", code)]\
                = cls._unpack_array
        # Fix str
        for code in range(0xa0, 0xbf + 1):
            cls._unpack_dispatch_table[struct.pack("B", code)]\
                = cls._unpack_string
        # Nil
        cls._unpack_dispatch_table[b'\xc0'] = cls._unpack_nil
        # Reserved
        cls._unpack_dispatch_table[b'\xc1'] = cls._unpack_reserved
        # Boolean
        cls._unpack_dispatch_table[b'\xc2'] = cls._unpack_boolean
        cls._unpack_dispatch_table[b'\xc3'] = cls._unpack_boolean
        # Bin
        for code in range(0xc4, 0xc6 + 1):
            cls._unpack_dispatch_table[struct.pack("B", code)]\
                = cls._unpack_binary
        # Ext
        for code in range(0xc7, 0xc9 + 1):
            cls._unpack_dispatch_table[struct.pack("B", code)]\
                = cls._unpack_ext
        # Float
        cls._unpack_dispatch_table[b'\xca'] = cls._unpack_float
        cls._unpack_dispatch_table[b'\xcb'] = cls._unpack_float
        # Uint
        for code in range(0xcc, 0xcf + 1):
            cls._unpack_dispatch_table[struct.pack("B", code)]\
                = cls._unpack_integer
        # Int
        for code in range(0xd0, 0xd3 + 1):
            cls._unpack_dispatch_table[struct.pack("B", code)]\
                = cls._unpack_integer
        # Fixext
        for code in range(0xd4, 0xd8 + 1):
            cls._unpack_dispatch_table[struct.pack("B", code)]\
                = cls._unpack_ext
        # String
        for code in range(0xd9, 0xdb + 1):
            cls._unpack_dispatch_table[struct.pack("B", code)]\
                = cls._unpack_string
        # Array
        cls._unpack_dispatch_table[b'\xdc'] = cls._unpack_array
        cls._unpack_dispatch_table[b'\xdd'] = cls._unpack_array
        # Map
        cls._unpack_dispatch_table[b'\xde'] = cls._unpack_map
        cls._unpack_dispatch_table[b'\xdf'] = cls._unpack_map
        # Negative fixint
        for code in range(0xe0, 0xff + 1):
            cls._unpack_dispatch_table[struct.pack("B", code)]\
                = cls._unpack_integer

        return cls

    @staticmethod
    def _read_except(fp, n):
        data = fp.read(n)
        if len(data) < n:
            raise InsufficientDataException()
        return data

    def _unpack_integer(cls, code, fp, options):    # noqa
        if (ord(code) & 0xe0) == 0xe0:
            return struct.unpack("b", code)[0]
        elif code == b'\xd0':
            return struct.unpack("b", cls._read_except(fp, 1))[0]
        elif code == b'\xd1':
            return struct.unpack(">h", cls._read_except(fp, 2))[0]
        elif code == b'\xd2':
            return struct.unpack(">i", cls._read_except(fp, 4))[0]
        elif code == b'\xd3':
            return struct.unpack(">q", cls._read_except(fp, 8))[0]
        elif (ord(code) & 0x80) == 0x00:
            return struct.unpack("B", code)[0]
        elif code == b'\xcc':
            return struct.unpack("B", cls._read_except(fp, 1))[0]
        elif code == b'\xcd':
            return struct.unpack(">H", cls._read_except(fp, 2))[0]
        elif code == b'\xce':
            return struct.unpack(">I", cls._read_except(fp, 4))[0]
        elif code == b'\xcf':
            return struct.unpack(">Q", cls._read_except(fp, 8))[0]
        raise Exception("logic error, not int: 0x%02x" % ord(code))

    @staticmethod
    def _unpack_reserved(code, fp, options):
        if code == b'\xc1':
            raise ReservedCodeException(
                "encountered reserved code: 0x%02x" % ord(code))
        raise Exception(
            "logic error, not reserved code: 0x%02x" % ord(code))

    @staticmethod
    def _unpack_nil(code, fp, options):
        if code == b'\xc0':
            return None
        raise Exception("logic error, not nil: 0x%02x" % ord(code))

    @staticmethod
    def _unpack_boolean(code, fp, options):
        if code == b'\xc2':
            return False
        elif code == b'\xc3':
            return True
        raise Exception("logic error, not boolean: 0x%02x" % ord(code))

    def _unpack_float(cls, code, fp, options):
        if code == b'\xca':
            return struct.unpack(">f", cls._read_except(fp, 4))[0]
        elif code == b'\xcb':
            return struct.unpack(">d", cls._read_except(fp, 8))[0]
        raise Exception("logic error, not float: 0x%02x" % ord(code))

    def _unpack_string(cls, code, fp, options):
        if (ord(code) & 0xe0) == 0xa0:
            length = ord(code) & ~0xe0
        elif code == b'\xd9':
            length = struct.unpack("B", cls._read_except(fp, 1))[0]
        elif code == b'\xda':
            length = struct.unpack(">H", cls._read_except(fp, 2))[0]
        elif code == b'\xdb':
            length = struct.unpack(">I", cls._read_except(fp, 4))[0]
        else:
            raise Exception("logic error, not string: 0x%02x" % ord(code))

        # Always return raw bytes in compatibility mode
        # global compatibility
        # if compatibility:
        #     return cls._read_except(fp, length)

        data = cls._read_except(fp, length)
        try:
            return bytes.decode(data, 'utf-8')
        except UnicodeDecodeError:
            if options.get("allow_invalid_utf8"):
                return InvalidString(data)
            raise InvalidStringException("unpacked string is invalid utf-8")

    def _unpack_binary(cls, code, fp, options):
        if code == b'\xc4':
            length = struct.unpack("B", cls._read_except(fp, 1))[0]
        elif code == b'\xc5':
            length = struct.unpack(">H", cls._read_except(fp, 2))[0]
        elif code == b'\xc6':
            length = struct.unpack(">I", cls._read_except(fp, 4))[0]
        else:
            raise Exception("logic error, not binary: 0x%02x" % ord(code))

        return cls._read_except(fp, length)

    def _unpack_ext(cls, code, fp, options):        # noqa
        if code == b'\xd4':
            length = 1
        elif code == b'\xd5':
            length = 2
        elif code == b'\xd6':
            length = 4
        elif code == b'\xd7':
            length = 8
        elif code == b'\xd8':
            length = 16
        elif code == b'\xc7':
            length = struct.unpack("B", cls._read_except(fp, 1))[0]
        elif code == b'\xc8':
            length = struct.unpack(">H", cls._read_except(fp, 2))[0]
        elif code == b'\xc9':
            length = struct.unpack(">I", cls._read_except(fp, 4))[0]
        else:
            raise Exception("logic error, not ext: 0x%02x" % ord(code))

        ext = Ext(ord(cls._read_except(fp, 1)), cls._read_except(fp, length))

        # Unpack with ext handler, if we have one
        ext_handlers = options.get("ext_handlers")
        if ext_handlers and ext.type in ext_handlers:
            ext = ext_handlers[ext.type](ext)

        return ext

    def _unpack_array(cls, code, fp, options):
        if (ord(code) & 0xf0) == 0x90:
            length = (ord(code) & ~0xf0)
        elif code == b'\xdc':
            length = struct.unpack(">H", cls._read_except(fp, 2))[0]
        elif code == b'\xdd':
            length = struct.unpack(">I", cls._read_except(fp, 4))[0]
        else:
            raise Exception("logic error, not array: 0x%02x" % ord(code))

        return [cls._unpack(fp, options) for i in range(length)]

    def _deep_list_to_tuple(cls, obj):
        if isinstance(obj, list):
            return tuple([cls._deep_list_to_tuple(e) for e in obj]) # noqa
        return obj

    def _unpack_map(cls, code, fp, options):
        if (ord(code) & 0xf0) == 0x80:
            length = (ord(code) & ~0xf0)
        elif code == b'\xde':
            length = struct.unpack(">H", cls._read_except(fp, 2))[0]
        elif code == b'\xdf':
            length = struct.unpack(">I", cls._read_except(fp, 4))[0]
        else:
            raise Exception("logic error, not map: 0x%02x" % ord(code))

        d = {} if not options.get('use_ordered_dict') \
            else collections.OrderedDict()
        for _ in range(length):
            # Unpack key
            k = cls._unpack(fp, options)

            if isinstance(k, list):
                # Attempt to convert list into a hashable tuple
                k = cls._deep_list_to_tuple(k)      # noqa
            elif not isinstance(k, collections.Hashable):
                raise UnhashableKeyException(
                    "encountered unhashable key: %s, %s"
                    % (str(k), str(type(k))))
            elif k in d:
                raise DuplicateKeyException(
                    "encountered duplicate key: %s, %s"
                    % (str(k), str(type(k))))

            # Unpack value
            v = cls._unpack(fp, options)

            try:
                d[k] = v
            except TypeError:
                raise UnhashableKeyException(
                    "encountered unhashable key: %s" % str(k))
        return d


class Decoder(metaclass=_DecoderMeta):
    @classmethod
    def _unpack(cls, fp, options):
        code = cls._read_except(fp, 1)
        return cls._unpack_dispatch_table[code](code, fp, options)

    @classmethod
    def unpack(cls, fp, **options):
        return cls._unpack(fp, options)

    @classmethod
    def unpackb(cls, s, **options):
        if not isinstance(s, (bytes, bytearray)):
            raise TypeError("packed data must be type 'bytes' or 'bytearray'")
        return cls._unpack(io.BytesIO(s), options)


class MsgpackMeta(abc.ABCMeta):

    """Manages ext handler and custom encoder registration."""

    ext_handlers_encode = {}
    ext_handlers_decode = {}
    custom_encoders = {}

    def register(cls, data_type=None, ext_code=None):
        def decorator(handler):
            if not issubclass(handler, Msgpack):
                raise TypeError(
                    "Msgpack handler must be a subclass of abstract `Msgpack`"
                    " class: {}".format(handler))
            if data_type is None:
                _data_type = handler
            else:
                _data_type = data_type

            if ext_code is not None:
                cls.ext_handlers_encode[_data_type] = \
                    lambda data: Ext(
                        ext_code, handler.__msgpack_encode__(data, _data_type))
                cls.ext_handlers_decode[ext_code] = \
                    lambda ext: handler.__msgpack_decode__(ext.data,
                                                           _data_type)
            else:
                cls.custom_encoders[_data_type] = handler
            return handler
        return decorator

    def encode(cls, data):
        encoded_data = Encoder.packb(data,
                                     ext_handlers=cls.ext_handlers_encode)
        return encoded_data

    def decode(cls, encoded_data):
        data = Decoder.unpackb(encoded_data,
                               ext_handlers=cls.ext_handlers_decode)
        return data

    def get_custom_encoder(cls, data_type):
        if issubclass(data_type, Msgpack):
            return data_type

        # lookup data types for registered encoders
        for subclass in data_type.__mro__:
            try:
                return cls.custom_encoders[subclass]
            except KeyError:
                continue
        return None


class Msgpack(metaclass=MsgpackMeta):

    """Add msgpack en/decoding to a type."""

    @abc.abstractclassmethod
    def __msgpack_encode__(cls, data, data_type):
        return None

    @abc.abstractclassmethod
    def __msgpack_decode__(cls, encoded_data, data_type):
        return None

    @classmethod
    def __subclasshook__(cls, C):
        if cls is Msgpack:
            if any("__msgpack_encode__" in B.__dict__ for B in C.__mro__) \
                    and any("__msgpack_decode__" in B.__dict__
                            for B in C.__mro__):
                return True
        return NotImplemented


encode = Msgpack.encode
decode = Msgpack.decode
register = Msgpack.register
get_custom_encoder = Msgpack.get_custom_encoder
