import math
import random

import matplotlib.pyplot as plt
from pyspark import SparkContext

sc = SparkContext(appName="miPi")


def sample(p):
    x, y = random.random(), random.random()
    return 1 if x * x + y * y < 1 else 0


def calculate_pi(num_samples, num_partitions):
    samples_rdd = sc.parallelize(range(num_samples), num_partitions)
    count = samples_rdd.map(lambda _: sample(0)).reduce(lambda a, b: a + b)
    return 4.0 * count / num_samples


def run_calculation(samples, partitions, error_list: list):
    pi_value = calculate_pi(samples, partitions)
    absolute_error = abs(math.pi - pi_value)
    error_list.append(absolute_error)
    print(
        f"Pi: {pi_value:.6f} | Samples: {samples:,} | Partitions {partitions}: | Absolute Error: {absolute_error:.6f}"
    )


fixed_partitions = 8
sample_errors = []
sample_sizes = [10**i for i in range(1, 8)]

fixed_samples = 1000000
partition_errors = []
partition_sizes = [2**i for i in range(1, 8)]

for samples in sample_sizes:
    run_calculation(samples, fixed_partitions, sample_errors)

for partitions in partition_sizes:
    run_calculation(fixed_samples, partitions, partition_errors)

plt.figure(figsize=(15, 10))

plt.subplot(2, 2, 1)
plt.plot(sample_sizes, sample_errors, "bo-")
plt.title(f"Accuracy vs Number of Samples (Partitions={fixed_partitions})")
plt.xlabel("Number of Samples")
plt.ylabel("Accuracy")
plt.xscale("log")
plt.grid(True)

plt.subplot(2, 2, 2)
plt.plot(partition_sizes, partition_errors, "ro-")
plt.title(f"Accuracy vs Number of Partitions (Samples={fixed_samples:,})")
plt.xlabel("Number of Partitions")
plt.ylabel("Accuracy")
plt.grid(True)

plt.tight_layout()
plt.show()

sc.stop()
