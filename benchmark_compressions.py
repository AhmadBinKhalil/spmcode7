#!/usr/bin/env python3
"""
Benchmark MPI and FastFlow Compression

This script creates bogus test files (with an 'input.dat' file in each directory)
of various sizes in dedicated folders. For each folder it then runs both an MPI-based 
compression and a FastFlow-based compression using the provided commands:
    mpirun -np 4 ./mpi_compressor compress input_folder output_folder
    ./ff_compressor compress input_folder output_folder

It measures the execution time over several runs and outputs the average time.
"""

import os
import subprocess
import time

# Configuration: file sizes (in bytes)
FILE_SIZES = {
    "5mb": 5 * 1024 * 1024,
    "10mb": 10 * 1024 * 1024,
    "20mb": 20 * 1024 * 1024,
    "50mb": 50 * 1024 * 1024,
    "100mb": 100 * 1024 * 1024,
    "200mb": 200 * 1024 * 1024,
    "500mb": 500 * 1024 * 1024,
    "1gb": 1024 * 1024 * 1024
}

# Number of runs to average on
NUM_RUNS = 3

def generate_file(file_path, size):
    """
    Generate a binary file with random data (bogus data) of the given size.
    Data is written in 1MB chunks.
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    chunk_size = 1024 * 1024  # 1 MB chunks
    remaining = size
    with open(file_path, "wb") as f:
        while remaining > 0:
            
            write_size = min(chunk_size, remaining)
            # Using os.urandom for random bytes; note that 1GB of random data can be heavy.
            f.write(os.urandom(write_size))
            remaining -= write_size
    print(f"Generated file '{file_path}' ({size} bytes).")

def run_command(command, num_runs=NUM_RUNS):
    """
    Runs the given command a number of times and returns the average execution time.
    Captures stdout/stderr for debugging.
    """
    times = []
    for i in range(num_runs):
        print(f"  Run {i+1}/{num_runs}: {' '.join(command)}")
        start = time.perf_counter()
        # Run the command and capture stdout/stderr.
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        end = time.perf_counter()
        run_time = end - start
        times.append(run_time)
        if result.returncode != 0:
            print(f"Error running command: {' '.join(command)}")
            print("stderr:", result.stderr.decode())
    avg_time = sum(times) / len(times)
    return avg_time

def main():
    base_dir = "test_files"
    print("Starting compression benchmark.")
    
    # Step 1: Create test files with bogus data.
    for label, size in FILE_SIZES.items():
        # Each file will reside in its own folder e.g., test_files/5mb/
        folder = os.path.join(base_dir, label)
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, "input.dat")
        # If file already exists and its size is correct, skip regeneration.
        if os.path.exists(file_path) and os.path.getsize(file_path) == size:
            print(f"File '{file_path}' already exists with correct size; skipping generation.")
        else:
            print(f"Generating file '{file_path}' of size {size} bytes.")
            generate_file(file_path, size)
    
    benchmark_results = {}
    
    # Step 2: For each test folder, run MPI and FF compressions.
    # The input for both commands is the folder containing the input file.
    for label, size in FILE_SIZES.items():
        folder = os.path.join(base_dir, label)  # input folder for the compressor
        output_folder = os.path.join(folder, "output")
        os.makedirs(output_folder, exist_ok=True)  # ensure output folder exists

        # Commands – note that both expect input directory and already created output directory.
        mpi_cmd = ["mpirun", "-np", "4", "./mpi_compressor", "compress", folder, output_folder]
        ff_cmd = ["./ff_compressor", "compress", folder, output_folder]
        
        print(f"\nBenchmarking file size '{label}' with MPI compression:")
        mpi_avg_time = run_command(mpi_cmd)
        print(f"Average MPI compression time for {label}: {mpi_avg_time:.2f} seconds.")
        
        print(f"\nBenchmarking file size '{label}' with FastFlow compression:")
        ff_avg_time = run_command(ff_cmd)
        print(f"Average FF compression time for {label}: {ff_avg_time:.2f} seconds.")
        
        benchmark_results[label] = {"mpi": mpi_avg_time, "ff": ff_avg_time}
    
    # Step 3: Print summary of benchmark results.
    print("\nBenchmark Results Summary:")
    for label in sorted(benchmark_results.keys(), key=lambda l: FILE_SIZES[l]):
        mpi_time = benchmark_results[label]["mpi"]
        ff_time = benchmark_results[label]["ff"]
        print(f"File Size {label:>4}: MPI = {mpi_time:.2f} sec, FF = {ff_time:.2f} sec.")
    
if __name__ == "__main__":
    main() 