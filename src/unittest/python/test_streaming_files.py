import os

import tornado.testing
from testfixtures import log_capture

import fast_pyspark_tester

LICENSE_FILE = os.path.join(os.path.dirname(__file__), '../../../LICENS*')


class TextFile(tornado.testing.AsyncTestCase):
    def setUp(self) -> None:
        super().setUp()

        sc = fast_pyspark_tester.Context()
        self.ssc = fast_pyspark_tester.streaming.StreamingContext(sc, 0.1)

    def test_connect(self):
        result = []
        (
            self.ssc.textFileStream(LICENSE_FILE, process_all=True)
            .count()
            .foreachRDD(lambda rdd: result.append(rdd.collect()[0]))
        )

        self.ssc.start()
        self.ssc.awaitTermination(timeout=0.3)
        self.assertEqual(sum(result), 44)

    def test_save(self):
        self.ssc.textFileStream(LICENSE_FILE).count().saveAsTextFiles('tests/textout/')
        self.ssc.start()
        self.ssc.awaitTermination(timeout=1.0)

    def test_save_gz(self):
        self.ssc.textFileStream(LICENSE_FILE).count().saveAsTextFiles('tests/textout/', suffix='.gz')
        self.ssc.start()
        self.ssc.awaitTermination(timeout=1.0)

    @log_capture()
    def test_save_7z(self, log_):
        self.ssc.textFileStream(LICENSE_FILE).count().saveAsTextFiles('tests/textout/', suffix='.7z')
        self.ssc.start()
        self.ssc.awaitTermination(timeout=1.0)
        log_.check_present((
            'fast_pyspark_tester.fileio.codec.sevenz',
            'WARNING',
            'Writing of 7z compressed archives is not supported.'
        ))


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
