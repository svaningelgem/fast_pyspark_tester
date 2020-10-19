import logging

import fast_pyspark_tester


def test_trivial_sample():
    rdd = fast_pyspark_tester.Context().parallelize(range(1000), 1000)
    sampled = rdd.sample(False, 0.01, 42).collect()
    print(sampled)
    assert sampled == [97, 164, 294, 695, 807, 864, 911]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    test_trivial_sample()
