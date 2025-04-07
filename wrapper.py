import os
import subprocess
import math
import matplotlib.pyplot as plt

python_executable = os.path.expanduser("~/opt/anaconda3/envs/pyspark_env/bin/python")


def run_calculation(num_samples, num_partitions):
    result = subprocess.run(
        [
            python_executable,
            "calculate.py",
            str(num_samples),
            str(num_partitions),
        ],
        capture_output=True,
        text=True,
    )
    output = result.stdout.strip()

    try:
        _, _, pi_value_str = output.split(":")
        pi_value = float(pi_value_str)
    except Exception as e:
        print("Error parsing output:", output)
        print(e)
        return None

    absolute_error = abs(math.pi - pi_value)
    print(
        f"Pi: {pi_value:.6f} | Samples: {num_samples:,} | Partitions: {num_partitions} | Absolute Error: {absolute_error:.6f}"
    )
    return absolute_error


fixed_partitions = 8
sample_sizes = [10**i for i in range(1, 8)]
sample_errors = []

for samples in sample_sizes:
    error = run_calculation(samples, fixed_partitions)
    sample_errors.append(error)

fixed_samples = 1000000
partition_sizes = [2**i for i in range(1, 8)]
partition_errors = []

for partitions in partition_sizes:
    error = run_calculation(fixed_samples, partitions)
    partition_errors.append(error)

plt.figure(figsize=(15, 10))

plt.subplot(2, 2, 1)
plt.plot(sample_sizes, sample_errors, "bo-")
plt.title(f"Absolute Error vs Number of Samples (Partitions={fixed_partitions})")
plt.xlabel("Number of Samples")
plt.ylabel("Absolute Error")
plt.xscale("log")
plt.yscale("log")
plt.grid(True)

plt.subplot(2, 2, 2)
plt.plot(partition_sizes, partition_errors, "ro-")
plt.title(f"Absolute Error vs Number of Partitions (Samples={fixed_samples:,})")
plt.xlabel("Number of Partitions")
plt.ylabel("Absolute Error")
plt.grid(True)

plt.tight_layout()
plt.show()
