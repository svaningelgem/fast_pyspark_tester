import tempfile

from memory_profiler import profile

import fast_pyspark_tester


@profile
def main():
    tempFile = tempfile.NamedTemporaryFile(delete=True)
    tempFile.close()

    sc = fast_pyspark_tester.Context()
    sc.parallelize(range(1000000)).saveAsTextFile(tempFile.name + '.gz')
    rdd = sc.textFile(tempFile.name + '.gz')
    rdd.collect()


if __name__ == '__main__':
    main()
