.. image:: https://raw.githubusercontent.com/svaningelgem/fast_pyspark_tester/master/logo/logo-w100.png
    :target: https://github.com/svaningelgem/fast_pyspark_tester

.. image:: https://img.shields.io/travis/svaningelgem/fast_pyspark_tester/master
.. image:: https://img.shields.io/codecov/c/github/svaningelgem/fast_pyspark_tester/master

fast_pyspark_tester
===================

**fast_pyspark_tester** is a fork of the very excellent **pysparkling** library originally created by
Sven Kreiss. It includes lots of contributions from the pysparkling contributors (for more info, please
see their `github repository <https://github.com/pysparkling/pysparkling.git>`_. Changes will be pulled
regularly. This fork is aimed at clean and well tested code, both against itself and against Spark.

**fast_pyspark_tester** provides a faster, more responsive way to develop programs
for PySpark. It enables code intended for Spark applications to execute
entirely in Python, without incurring the overhead of initializing and
passing data through the JVM and Hadoop. The focus is on having a lightweight
and fast implementation for small datasets at the expense of some data
resilience features and some parallel processing features.

**How does it work?** To switch execution of a script from PySpark to fast_pyspark_tester,
have the code initialize a fast_pyspark_tester Context instead of a SparkContext, and
use the fast_pyspark_tester Context to set up your RDDs. The beauty is you don't have
to change a single line of code after the Context initialization, because
fast_pyspark_tester's API is (almost) exactly the same as PySpark's. Since it's so easy
to switch between PySpark and fast_pyspark_tester, you can choose the right tool for your
use case.

**When would I use it?** Say you are writing a Spark application because you
need robust computation on huge datasets, but you also want the same application
to provide fast answers on a small dataset. You're finding Spark is not responsive
enough for your needs, but you don't want to rewrite an entire separate application
for the *small-answers-fast* problem. You'd rather reuse your Spark code but somehow
get it to run fast. fast_pyspark_tester bypasses the stuff that causes Spark's long startup
times and less responsive feel.

Here are a few areas where fast_pyspark_tester excels:

* Small to medium-scale exploratory data analysis
* Application prototyping
* Low-latency web deployments
* Unit tests


Install
=======

.. code-block:: bash

    pip install fast_pyspark_tester[s3,hdfs,streaming]


`Documentation <https://fast_pyspark_tester.trivial.io>`_:

.. image:: https://raw.githubusercontent.com/svenkreiss/fast_pyspark_tester/master/docs/readthedocs.png
   :target: https://fast_pyspark_tester.trivial.io


Other links:
`Github <https://github.com/svenkreiss/fast_pyspark_tester>`_,
`Issue Tracker <https://github.com/svenkreiss/fast_pyspark_tester/issues>`_,
|pypi-badge|

.. |pypi-badge| image:: https://badge.fury.io/py/fast_pyspark_tester.svg
   :target: https://pypi.python.org/pypi/fast_pyspark_tester/


Features
========

* Supports URI schemes ``s3://``, ``hdfs://``, ``gs://``, ``http://`` and ``file://``
  for Amazon S3, HDFS, Google Storage, web and local file access.
  Specify multiple files separated by comma.
  Resolves ``*`` and ``?`` wildcards.
* Handles ``.gz``, ``.zip``, ``.lzma``, ``.xz``, ``.bz2``, ``.tar``,
  ``.tar.gz`` and ``.tar.bz2`` compressed files.
  Supports reading of ``.7z`` files.
* Parallelization via ``multiprocessing.Pool``,
  ``concurrent.futures.ThreadPoolExecutor`` or any other Pool-like
  objects that have a ``map(func, iterable)`` method.
* Plain fast_pyspark_tester does not have any dependencies (use ``pip install fast_pyspark_tester``).
  Some file access methods have optional dependencies:
  ``boto`` for AWS S3, ``requests`` for http, ``hdfs`` for hdfs


Examples
========

Some demos are in the notebooks
`docs/demo.ipynb <https://github.com/svenkreiss/fast_pyspark_tester/blob/master/docs/demo.ipynb>`_
and
`docs/iris.ipynb <https://github.com/svenkreiss/fast_pyspark_tester/blob/master/docs/iris.ipynb>`_
.

**Word Count**

.. code-block:: python

    from fast_pyspark_tester import Context

    counts = (
        Context()
        .textFile('README.rst')
        .map(lambda line: ''.join(ch if ch.isalnum() else ' ' for ch in line))
        .flatMap(lambda line: line.split(' '))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
    )
    print(counts.collect())

which prints a long list of pairs of words and their counts.
