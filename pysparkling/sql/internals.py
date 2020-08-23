import json
import math
from copy import deepcopy
from functools import partial

from pysparkling import StorageLevel
from pysparkling.sql.schema_utils import infer_schema_from_rdd
from pysparkling.sql.types import StructType, create_row
from pysparkling.sql.column import parse
from pysparkling.utils import get_keyfunc, compute_weighted_percentiles, reservoir_sample_and_size


class FieldIdGenerator(object):
    """
    This metaclass adds an unique ID to all instances of its classes.

    This allows to identify that a field was, when created, associated to a DataFrame.

    Such field can be retrieved with the syntax df.name to build an operation.
    The id clarifies if it is still associated to a field and which one
    when the operation is applied.

    While id() allow the same behaviour in most cases, this one:
    - Allows deep copies which are needed for aggregation
    - Support distributed computation, e.g. multiprocessing
    """
    _id = 0

    @classmethod
    def next(cls):
        cls._id += 1
        return cls._id

    @classmethod
    def bind_schema(cls, schema):
        for field in schema.fields:
            if not hasattr(field, "id"):
                field.id = cls.next()
            if isinstance(field, StructType):
                cls.bind_schema(field)
        return schema

    @classmethod
    def unbind_schema(cls, schema):
        for field in schema.fields:
            delattr(field, "id")
            if isinstance(field, StructType):
                cls.unbind_schema(field)
        return schema


class DataFrameInternal(object):
    def __init__(self, sc, rdd, cols=None, convert_to_row=False, schema=None):
        """
        :type rdd: RDD
        """
        if convert_to_row:
            if cols is None:
                cols = ["_c{0}".format(i) for i in range(200)]
            rdd = rdd.map(partial(create_row, cols))

        self._sc = sc
        self._rdd = rdd
        if schema is None and convert_to_row is False:
            raise NotImplementedError(
                "Schema cannot be None when creating DataFrameInternal from another. "
                "As a user you should not see this error, feel free to report a bug at "
                "https://github.com/svenkreiss/pysparkling/issues"
            )
        if schema is not None:
            self._set_schema(schema)
        else:
            self._set_schema(infer_schema_from_rdd(self._rdd))

    def _set_schema(self, schema):
        bound_schema = FieldIdGenerator.bind_schema(deepcopy(schema))
        self.bound_schema = bound_schema

    @property
    def unbound_schema(self):
        schema = deepcopy(self.bound_schema)
        return FieldIdGenerator.unbind_schema(schema)

    def _with_rdd(self, rdd, schema):
        return DataFrameInternal(
            self._sc,
            rdd,
            schema=schema
        )

    def rdd(self):
        return self._rdd

    @staticmethod
    def range(sc, start, end=None, step=1, numPartitions=None):
        if end is None:
            start, end = 0, start

        rdd = sc.parallelize(
            ([i] for i in range(start, end, step)),
            numSlices=numPartitions
        )
        return DataFrameInternal(sc, rdd, ["id"], True)

    def count(self):
        return self._rdd.count()

    def collect(self):
        return self._rdd.collect()

    def toLocalIterator(self):
        return self._rdd.toLocalIterator()

    def limit(self, n):
        jdf = self._sc.parallelize(self._rdd.take(n))
        return self._with_rdd(jdf, self.bound_schema)

    def take(self, n):
        return self._rdd.take(n)

    def foreach(self, f):
        self._rdd.foreach(f)

    def foreachPartition(self, f):
        self._rdd.foreachPartition(f)

    def cache(self):
        return self._with_rdd(self._rdd.cache(), self.bound_schema)

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY):
        return self._with_rdd(self._rdd.persist(storageLevel), self.bound_schema)

    def unpersist(self, blocking=False):
        return self._with_rdd(self._rdd.unpersist(blocking), self.bound_schema)

    def coalesce(self, numPartitions):
        return self._with_rdd(self._rdd.coalesce(numPartitions), self.bound_schema)

    def distinct(self):
        return self._with_rdd(self._rdd.distinct(), self.bound_schema)

    def sample(self, withReplacement=None, fraction=None, seed=None):
        return self._with_rdd(
            self._rdd.sample(
                withReplacement=withReplacement,
                fraction=fraction,
                seed=seed
            ),
            self.bound_schema
        )

    def randomSplit(self, weights, seed):
        return self._with_rdd(
            self._rdd.randomSplit(weights=weights, seed=seed),
            self.bound_schema
        )

    @property
    def storageLevel(self):
        return getattr(self._rdd, "storageLevel", StorageLevel(False, False, False, False))

    def is_cached(self):
        return hasattr(self._rdd, "storageLevel")

    def simple_repartition(self, numPartitions):
        return self._with_rdd(self._rdd.repartition(numPartitions), self.bound_schema)

    def repartitionByValues(self, numPartitions, partitioner=None):
        return self._with_rdd(
            self._rdd.map(lambda x: (x, x)).partitionBy(numPartitions, partitioner).values(),
            self.bound_schema
        )

    def repartition(self, numPartitions, cols):
        def partitioner(row):
            return sum(hash(c.eval(row, self.bound_schema)) for c in cols)

        return self.repartitionByValues(numPartitions, partitioner)

    def repartitionByRange(self, numPartitions, *cols):
        key = get_keyfunc(cols, self.bound_schema)
        bounds = self._get_range_bounds(self._rdd, numPartitions, key=key)

        def get_range_id(value):
            return sum(1 for bound in bounds if key(bound) < key(value))

        return self.repartitionByValues(numPartitions, partitioner=get_range_id)

    @staticmethod
    def _get_range_bounds(rdd, numPartitions, key):
        if numPartitions == 0:
            return []

        # pylint: disable=W0511
        # todo: check if sample_size is set in SQLConf.get.rangeExchangeSampleSizePerPartition
        # sample_size = min(
        #   SQLConf.get.rangeExchangeSampleSizePerPartition * rdd.getNumPartitions(),
        #   1e6
        # )
        sample_size = 1e6
        sample_size_per_partition = math.ceil(3 * sample_size / numPartitions)
        sketched_rdd = DataFrameInternal.sketch_rdd(rdd, sample_size_per_partition)
        rdd_size = sum(partition_size for partition_size, sample in sketched_rdd.values())

        if rdd_size == 0:
            return []

        fraction = sample_size / rdd_size

        candidates, imbalanced_partitions = DataFrameInternal._get_initial_candidates(
            sketched_rdd,
            sample_size_per_partition,
            fraction
        )

        additional_candidates = DataFrameInternal._get_additional_candidates(
            rdd,
            imbalanced_partitions,
            fraction
        )

        candidates += additional_candidates
        bounds = compute_weighted_percentiles(
            candidates,
            min(numPartitions, len(candidates)) + 1,
            key=key
        )[1:-1]
        return bounds

    @staticmethod
    def _get_initial_candidates(sketched_rdd, sample_size_per_partition, fraction):
        candidates = []
        imbalanced_partitions = set()
        for idx, (partition_size, sample) in sketched_rdd.items():
            # Partition is bigger than (3 times) average and more than sample_size_per_partition
            # is needed to get accurate information on its distribution
            if fraction * partition_size > sample_size_per_partition:
                imbalanced_partitions.add(idx)
            else:
                # The weight is 1 over the sampling probability.
                weight = partition_size / len(sample)
                candidates += [(key, weight) for key in sample]
        return candidates, imbalanced_partitions

    @staticmethod
    def _get_additional_candidates(rdd, imbalanced_partitions, fraction):
        additional_candidates = []
        if imbalanced_partitions:
            # Re-sample imbalanced partitions with the desired sampling probability.
            def keep_imbalanced_partitions(partition_id, x):
                return x if partition_id in imbalanced_partitions else []

            resampled = (rdd.mapPartitionsWithIndex(keep_imbalanced_partitions)
                         .sample(withReplacement=False, fraction=fraction, seed=rdd.id())
                         .collect())
            weight = (1.0 / fraction).toFloat
            additional_candidates += [(x, weight) for x in resampled]
        return additional_candidates

    @staticmethod
    def sketch_rdd(rdd, sample_size_per_partition):
        """
        Get a subset per partition of an RDD

        Sampling algorithm is reservoir sampling.

        :param rdd:
        :param sample_size_per_partition:
        :return:
        """

        def sketch_partition(idx, x):
            sample, original_size = reservoir_sample_and_size(
                x,
                sample_size_per_partition,
                seed=rdd.id() + idx
            )
            return [(idx, (original_size, sample))]

        sketched_rdd_content = rdd.mapPartitionsWithIndex(sketch_partition).collect()

        return dict(sketched_rdd_content)

    def toJSON(self, use_unicode):
        """

        :rtype: RDD
        """
        return self._rdd.map(lambda row: json.dumps(row.asDict(True), ensure_ascii=not use_unicode))

    def sortWithinPartitions(self, cols, ascending):
        key = get_keyfunc([parse(c) for c in cols], self.bound_schema)

        def partition_sort(data):
            return sorted(data, key=key, reverse=not ascending)

        return self._with_rdd(
            self._rdd.mapPartitions(partition_sort),
            self.bound_schema
        )
