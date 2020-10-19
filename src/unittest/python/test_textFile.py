from __future__ import print_function

import importlib
import logging
import os
import pickle
import random
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock, skipIf

import requests
from testfixtures import log_capture

from fast_pyspark_tester import Context
from fast_pyspark_tester.fileio import File

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
S3_TEST_PATH = os.getenv('S3_TEST_PATH')
OAUTH2_CLIENT_ID = os.getenv('OAUTH2_CLIENT_ID')
GS_TEST_PATH = os.getenv('GS_TEST_PATH')
HDFS_TEST_PATH = os.getenv('HDFS_TEST_PATH')
LOCAL_FILENAME = __file__
if os.path.altsep:
    LOCAL_FILENAME = LOCAL_FILENAME.replace(os.path.altsep, os.path.sep)
LOCAL_TEST_PATH = os.path.dirname(LOCAL_FILENAME)


class TextFileTests(unittest.TestCase):
    def test_cache(self):
        # this crashes in version 0.2.28
        lines = Context().textFile('{}/*textFil*.py'.format(LOCAL_TEST_PATH))
        lines = lines.map(lambda l: '-' + l).cache()
        print(len(lines.collect()))
        lines = lines.map(lambda l: '+' + l)
        lines = lines.map(lambda l: '-' + l).cache()
        lines = lines.collect()
        print(lines)
        self.assertIn('-+-from fast_pyspark_tester import Context', lines)
    
    def test_local_textFile_1(self):
        lines = Context().textFile('{}/*textFil*.py'.format(LOCAL_TEST_PATH))
        lines = lines.collect()
        print(lines)
        self.assertIn('from fast_pyspark_tester import Context', lines)
    
    def test_local_textFile_2(self):
        line_count = Context().textFile('{}/*.py'.format(LOCAL_TEST_PATH)).count()
        print(line_count)
        self.assertGreater(line_count, 90)
    
    def test_local_textFile_name(self):
        name = Context().textFile('{}/*.py'.format(LOCAL_TEST_PATH)).name()
        print(name)
        self.assertTrue(name.startswith('{}/*.py'.format(LOCAL_TEST_PATH)))

    def test_wholeTextFiles(self):
        all_files = Context().wholeTextFiles(f'{LOCAL_TEST_PATH}/*.py')
        this_file = all_files.lookup(LOCAL_FILENAME)
        print(this_file)
        self.assertIn('test_wholeTextFiles', this_file[0])
    
    @skipIf(not AWS_ACCESS_KEY_ID, reason='no AWS env')
    def test_s3_textFile(self):
        myrdd = Context().textFile('s3n://aws-publicdatasets/common-crawl/crawl-data/' 'CC-MAIN-2015-11/warc.paths.*')
        self.assertIn(
            'common-crawl/crawl-data/CC-MAIN-2015-11/segments/1424937481488.49/'
            'warc/CC-MAIN-20150226075801-00329-ip-10-28-5-156.ec2.'
            'internal.warc.gz', myrdd.collect()
        )

    @skipIf(not AWS_ACCESS_KEY_ID, reason='no AWS env')
    def test_s3_textFile_loop(self):
        random.seed()
    
        fn = f'{S3_TEST_PATH}/pysparkling_test_{random.random() * 999999.0:.0f}.txt'
    
        rdd = Context().parallelize('Line {0}'.format(n) for n in range(200))
        rdd.saveAsTextFile(fn)
        rdd_check = Context().textFile(fn)
    
        self.assertEqual(rdd.count(), rdd_check.count())
        self.assertTrue(all(e1 == e2 for e1, e2 in zip(rdd.collect(), rdd_check.collect())))
    
    @skipIf(not HDFS_TEST_PATH, reason='no HDFS env')
    def test_hdfs_textFile_loop(self):
        random.seed()
    
        fn = f'{HDFS_TEST_PATH}/pysparkling_test_{random.random() * 999999.0:.0f}.txt'
        print('HDFS test file: {0}'.format(fn))
    
        rdd = Context().parallelize('Hello World {0}'.format(x) for x in range(10))
        rdd.saveAsTextFile(fn)
        read_rdd = Context().textFile(fn)
        print(rdd.collect())
        print(read_rdd.collect())
        assert rdd.count() == read_rdd.count() and all(r1 == r2 for r1, r2 in zip(rdd.collect(), read_rdd.collect()))

    @skipIf(not HDFS_TEST_PATH, reason='no HDFS env')
    def test_hdfs_file_exists(self):
        random.seed()
    
        fn1 = f'{HDFS_TEST_PATH}/pysparkling_test_{random.random() * 999999.0:.0f}.txt'
        fn2 = f'{HDFS_TEST_PATH}/pysparkling_test_{random.random() * 999999.0:.0f}.txt'
    
        rdd = Context().parallelize('Hello World {0}'.format(x) for x in range(10))
        rdd.saveAsTextFile(fn1)
    
        assert File(fn1).exists() and not File(fn2).exists()
    
    @skipIf(not GS_TEST_PATH, reason='no GS env')
    @skipIf(not OAUTH2_CLIENT_ID, reason='no OAUTH env')
    def test_gs_textFile_loop(self):
        random.seed()
    
        fn = f'{GS_TEST_PATH}/pysparkling_test_{random.random() * 999999.0:.0f}.txt'
    
        rdd = Context().parallelize('Line {0}'.format(n) for n in range(200))
        rdd.saveAsTextFile(fn)
        rdd_check = Context().textFile(fn)
    
        assert rdd.count() == rdd_check.count() and all(e1 == e2 for e1, e2 in zip(rdd.collect(), rdd_check.collect()))

    @skipIf(not AWS_ACCESS_KEY_ID, reason='no AWS env')
    @skipIf(not S3_TEST_PATH, reason='no S3 env')
    def test_dumpToFile(self):
        random.seed()
    
        fn = f'{S3_TEST_PATH}/pysparkling_test_{random.random() * 999999.0:.0f}.pickle'
        File(fn).dump(pickle.dumps({'hello': 'world'}))
    
    def test_http_textFile(self):
        myrdd = Context().textFile(
            'https://s3-us-west-2.amazonaws.com/human-microbiome-project/DEMO/' 'HM16STR/46333/by_subject/1139.fsa'
        )
        self.assertIn('TGCTGCGGTGAATGCGTTCCCGGGTCT', myrdd.collect())
    
    def test_saveAsTextFile(self):
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        Context().parallelize(range(10)).saveAsTextFile(tempFile.name)
        with open(tempFile.name, 'r') as f:
            r = f.readlines()
            print(r)
            self.assertIn('5\n', r)

    def test_saveAsTextFile_tar(self):
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        Context().parallelize(range(10)).saveAsTextFile(tempFile.name + '.tar')
        read_rdd = Context().textFile(tempFile.name + '.tar')
        print(read_rdd.collect())
        self.assertIn('5', read_rdd.collect())

    @skipIf(hasattr(sys, 'pypy_version_info'), 'skip on pypy')
    def test_saveAsTextFile_targz(self):
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        Context().parallelize(range(10)).saveAsTextFile(tempFile.name + '.tar.gz')
        read_rdd = Context().textFile(tempFile.name + '.tar.gz')
        print(read_rdd.collect())
        self.assertIn('5', read_rdd.collect())

    def test_saveAsTextFile_tarbz2(self):
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        Context().parallelize(range(10)).saveAsTextFile(tempFile.name + '.tar.bz2')
        read_rdd = Context().textFile(tempFile.name + '.tar.bz2')
        print(read_rdd.collect())
        self.assertIn('5', read_rdd.collect())

    def test_saveAsTextFile_gz(self):
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        Context().parallelize(range(10)).saveAsTextFile(tempFile.name + '.gz')
        read_rdd = Context().textFile(tempFile.name + '.gz')
        self.assertIn('5', read_rdd.collect())

    def test_saveAsTextFile_zip(self):
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        Context().parallelize(range(10)).saveAsTextFile(tempFile.name + '.zip')
        read_rdd = Context().textFile(tempFile.name + '.zip')
        print(read_rdd.collect())
        self.assertIn('5', read_rdd.collect())

    def test_saveAsTextFile_bz2(self):
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        Context().parallelize(range(10)).saveAsTextFile(tempFile.name + '.bz2')
        read_rdd = Context().textFile(tempFile.name + '.bz2')
        self.assertIn('5', read_rdd.collect())
    
    def test_saveAsTextFile_lzma(self):
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        Context().parallelize(range(10)).saveAsTextFile(tempFile.name + '.lzma')
        read_rdd = Context().textFile(tempFile.name + '.lzma')
        self.assertIn('5', read_rdd.collect())

    @log_capture()
    def test_read_7z(self, log_):
        # file was created with:
        # 7z a tests/data.7z tests/readme_example.py
        # (brew install p7zip)


        rdd = Context().textFile('{}/data.7z'.format(LOCAL_TEST_PATH))
        print(rdd.collect())
        self.assertIn('from fast_pyspark_tester import Context', rdd.collect())

        module_under_test = 'fast_pyspark_tester.fileio.codec.sevenz'

        log_.clear()

        # Bit of a crude way to fake not having a module available... But hey! It works ;-)
        orig_import = __import__

        def raise_on_7zimport(name, globals, locals, *args):
            if name == 'py7zlib' and globals['__name__'] == module_under_test:
                raise ImportError

            return orig_import(name, globals, locals, *args)

        with mock.patch('builtins.__import__', side_effect=raise_on_7zimport):
            importlib.reload(sys.modules[module_under_test])
            rdd = Context().textFile('{}/data.7z'.format(LOCAL_TEST_PATH))
            print(rdd.collect())
            log_.check_present(
                (
                    module_under_test,
                    'WARNING',
                    'py7zlib could not be imported. To read 7z files, install the library with "pip install pylzma".'
                ),
                order_matters=False
            )

    def test_read_tar(self):
        # file was created with:
        # tar cf data.tar test
        rdd = Context().textFile('{}/data.tar'.format(LOCAL_TEST_PATH))
        print(rdd.collect())
        self.assertIn('Hello fast_pyspark_tester!', rdd.collect())

    def test_read_tar_gz(self):
        # file was created with:
        # tar -cvzf data.tar.gz test
        rdd = Context().textFile('{}/data.tar.gz'.format(LOCAL_TEST_PATH))
        print(rdd.collect())
        self.assertIn('Hello fast_pyspark_tester!', rdd.collect())

    def test_read_tar_bz2(self):
        # file was created with:
        # tar -cvjf data.tar.bz2 test
        rdd = Context().textFile('{}/data.tar.bz2'.format(LOCAL_TEST_PATH))
        print(rdd.collect())
        self.assertIn('Hello fast_pyspark_tester!', rdd.collect())

    @skipIf(os.getenv('TRAVIS') is not None, 'skip 20news test on Travis')
    def test_read_tar_gz_20news(self):
        # 20 news dataset has some '0xff' characters that lead to encoding
        # errors before. Adding this as a test case.
        src = 'http://qwone.com/~jason/20Newsgroups/20news-19997.tar.gz'
        tgt = Path(__file__).parent / os.path.basename(src)
    
        if not os.path.isfile(os.path.basename(src)):
            # Fetch it to speed up future tests.
            tgt.write_bytes(requests.get(src).content)
    
        rdd = Context().textFile(str(tgt), use_unicode=False)
        self.assertIn('}|> 1. Mechanical driven odometer:', rdd.top(500))

    def test_pyspark_compatibility_txt(self):
        kv = Context().textFile('{}/pyspark/key_value.txt'.format(LOCAL_TEST_PATH)).collect()
        print(kv)
        self.assertIn("('a', 1)", kv)
        self.assertIn("('b', 2)", kv)
        self.assertEqual(len(kv), 2)

    def test_pyspark_compatibility_bz2(self):
        kv = Context().textFile('{}/pyspark/key_value.txt.bz2'.format(LOCAL_TEST_PATH)).collect()
        print(kv)
        self.assertIn('a\t1', kv)
        self.assertIn('b\t2', kv)
        self.assertEqual(len(kv), 2)

    def test_pyspark_compatibility_gz(self):
        kv = Context().textFile('{}/pyspark/key_value.txt.gz'.format(LOCAL_TEST_PATH)).collect()
        print(kv)
        self.assertIn('a\t1', kv)
        self.assertIn('b\t2', kv)
        self.assertEqual(len(kv), 2)

    def test_local_regex_read(self):
        # was not working before 0.3.19
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        Context().parallelize(range(30), 30).saveAsTextFile(tempFile.name)
        d = Context().textFile(tempFile.name + '/part-0000*').collect()
        print(d)
        self.assertEqual(len(d), 10)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    # test_read_7z()
