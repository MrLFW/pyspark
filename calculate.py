import random
import sys
from pyspark import SparkContext


def sample(p):
    x, y = random.random(), random.random()
    return 1 if x * x + y * y < 1 else 0


NUM_SAMPLES = int(sys.argv[1])
NUM_PARTITIONS = int(sys.argv[2])

sc = SparkContext(appName="miPi")
samples_rdd = sc.parallelize(range(NUM_SAMPLES), NUM_PARTITIONS)
sc.stop()

count = samples_rdd.map(lambda _: sample(0)).reduce(lambda a, b: a + b)

print("Pi:" + str(NUM_SAMPLES) + ":" + str(4.0 * count / NUM_SAMPLES))
