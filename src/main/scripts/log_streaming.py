from __future__ import print_function

import fast_pyspark_tester


def main():
    sc = fast_pyspark_tester.Context()
    ssc = fast_pyspark_tester.streaming.StreamingContext(sc, 1)
    ssc.textFileStream('/var/log/system.log*').pprint()
    ssc.start()
    ssc.awaitTermination(timeout=3.0)


if __name__ == '__main__':
    main()
