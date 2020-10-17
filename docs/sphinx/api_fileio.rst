.. _api_fileio:


fileio
------

.. currentmodule:: fast_pyspark_tester

The functionality provided by this module is used in :func:`Context.textFile`
for reading and in :func:`RDD.saveAsTextFile` for writing.

.. currentmodule:: fast_pyspark_tester.fileio

You can use this submodule with :func:`File.dump`, :func:`File.load` and
:func:`File.exists` to read, write and check for existance of a file.
All methods transparently handle various schemas (for example ``http://``,
``s3://`` and ``file://``) and compression/decompression of ``.gz`` and
``.bz2`` files (among others).


.. autoclass:: fast_pyspark_tester.fileio.File
    :members:

.. autoclass:: fast_pyspark_tester.fileio.TextFile
    :members:


File System
^^^^^^^^^^^

.. autoclass:: fast_pyspark_tester.fileio.fs.FileSystem
    :members:

.. autoclass:: fast_pyspark_tester.fileio.fs.Local
    :members:

.. autoclass:: fast_pyspark_tester.fileio.fs.GS
    :members:

.. autoclass:: fast_pyspark_tester.fileio.fs.Hdfs
    :members:

.. autoclass:: fast_pyspark_tester.fileio.fs.Http
    :members:

.. autoclass:: fast_pyspark_tester.fileio.fs.S3
    :members:


Codec
^^^^^

.. autoclass:: fast_pyspark_tester.fileio.codec.Codec
    :members:

.. autoclass:: fast_pyspark_tester.fileio.codec.Bz2
    :members:

.. autoclass:: fast_pyspark_tester.fileio.codec.Gz
    :members:

.. autoclass:: fast_pyspark_tester.fileio.codec.Lzma
    :members:

.. autoclass:: fast_pyspark_tester.fileio.codec.SevenZ
    :members:

.. autoclass:: fast_pyspark_tester.fileio.codec.Tar
    :members:

.. autoclass:: fast_pyspark_tester.fileio.codec.TarGz
    :members:

.. autoclass:: fast_pyspark_tester.fileio.codec.TarBz2
    :members:

.. autoclass:: fast_pyspark_tester.fileio.codec.Zip
    :members:
