import logging
import lzma
from io import BytesIO

from .codec import Codec

log = logging.getLogger(__name__)


class Lzma(Codec):
    """Implementation of :class:`.Codec` for lzma compression."""

    def compress(self, stream):
        if lzma is None:
            return Codec.compress(self, stream)

        return BytesIO(lzma.compress(stream.read()))

    def decompress(self, stream):
        if lzma is None:
            return Codec.decompress(self, stream)

        return BytesIO(lzma.decompress(stream.read()))
