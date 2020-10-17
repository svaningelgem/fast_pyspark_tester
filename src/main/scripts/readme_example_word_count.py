from __future__ import print_function

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
