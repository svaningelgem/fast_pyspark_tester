import os

import tornado.testing

import fast_pyspark_tester


LICENSE_FILE = os.path.join(os.path.dirname(__file__), '../../../LICENS*')


class TextFile(tornado.testing.AsyncTestCase):
    def test_connect(self):
        sc = fast_pyspark_tester.Context()
        ssc = fast_pyspark_tester.streaming.StreamingContext(sc, 0.1)

        result = []
        (
            ssc.textFileStream(LICENSE_FILE, process_all=True)
            .count()
            .foreachRDD(lambda rdd: result.append(rdd.collect()[0]))
        )

        ssc.start()
        ssc.awaitTermination(timeout=0.3)
        self.assertEqual(sum(result), 44)

    def test_save(self):
        sc = fast_pyspark_tester.Context()
        ssc = fast_pyspark_tester.streaming.StreamingContext(sc, 0.1)

        (ssc.textFileStream(LICENSE_FILE).count().saveAsTextFiles('tests/textout/'))

    def test_save_gz(self):
        sc = fast_pyspark_tester.Context()
        ssc = fast_pyspark_tester.streaming.StreamingContext(sc, 0.1)

        (
            ssc.textFileStream(LICENSE_FILE)
            .count()
            .saveAsTextFiles('tests/textout/', suffix='.gz')
        )


class BinaryFile(tornado.testing.AsyncTestCase):
    def test_read_file(self):
        sc = fast_pyspark_tester.Context()
        ssc = fast_pyspark_tester.streaming.StreamingContext(sc, 0.1)

        result = []
        (
            ssc.fileBinaryStream(LICENSE_FILE, process_all=True)
            .count()
            .foreachRDD(lambda rdd: result.append(rdd.collect()[0]))
        )

        ssc.start()
        ssc.awaitTermination(timeout=0.3)
        self.assertEqual(sum(result), 1)

    def test_read_chunks(self):
        sc = fast_pyspark_tester.Context()
        ssc = fast_pyspark_tester.streaming.StreamingContext(sc, 0.1)

        result = []
        (
            ssc.fileBinaryStream(LICENSE_FILE, recordLength=40, process_all=True)
            .count()
            .foreachRDD(lambda rdd: result.append(rdd.collect()[0]))
        )

        ssc.start()
        ssc.awaitTermination(timeout=0.3)
        self.assertEqual(sum(result), 55)
