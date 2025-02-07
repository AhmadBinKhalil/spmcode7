#define MINIZ_IMPLEMENTATION
#define MINIZ_HEADER_FILE_ONLY
#include "miniz/miniz.h"

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <errno.h>

void ensure_directory(const char *dir) {
    struct stat st = {0};
    if (stat(dir, &st) == -1) {
        if (mkdir(dir, 0755) != 0) {
            perror("mkdir failed");
            exit(EXIT_FAILURE);
        }
    }
}



#define CHUNK_SIZE (1024*1024)  // 1 MB

// Modes: 0 = compress, 1 = decompress, -1 = termination

// Compression helper functions using Miniz



void compress_chunk(unsigned char *in_buf, int in_size, unsigned char **out_buf, int *out_size) {
    int bound = compressBound(in_size);
    *out_buf = (unsigned char*) malloc(bound);
    *out_size = bound;
    if (compress(*out_buf, (uLongf*) out_size, in_buf, in_size) != Z_OK) {
        fprintf(stderr, "Compression failed\n");
        *out_size = 0;
    }
}

void decompress_chunk(unsigned char *in_buf, int in_size, int orig_size, unsigned char **out_buf, int *out_size) {
    *out_buf = (unsigned char*) malloc(orig_size);
    *out_size = orig_size;
    if (uncompress(*out_buf, (uLongf*) out_size, in_buf, in_size) != Z_OK) {
        fprintf(stderr, "Decompression failed\n");
        *out_size = 0;
    }
}

int is_regular_file(const char *path) {
    struct stat path_stat;
    stat(path, &path_stat);
    return S_ISREG(path_stat.st_mode);
}

void master_process(int nprocs, int mode, const char *input_dir, const char *output_dir) {
    ensure_directory(output_dir);
    DIR *dir;
    struct dirent *ent;
    char in_filepath[1024], out_filepath[1024];
    dir = opendir(input_dir);
    if (!dir) {
        fprintf(stderr, "Failed to open input directory %s\n", input_dir);
        return;
    }
    while ((ent = readdir(dir)) != NULL) {
        if (ent->d_name[0] == '.') continue;
        snprintf(in_filepath, sizeof(in_filepath), "%s/%s", input_dir, ent->d_name);
        if (!is_regular_file(in_filepath)) continue;
        FILE *fp = fopen(in_filepath, "rb");
        if (!fp) {
            fprintf(stderr, "Failed to open file %s\n", in_filepath);
            continue;
        }
        fseek(fp, 0, SEEK_END);
        int file_size = ftell(fp);
        fseek(fp, 0, SEEK_SET);
        unsigned char *file_buf = (unsigned char*) malloc(file_size);
        fread(file_buf, 1, file_size, fp);
        fclose(fp);
        
        int num_chunks = (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
        unsigned char **results = (unsigned char**) malloc(num_chunks * sizeof(unsigned char*));
        int *result_sizes = (int*) malloc(num_chunks * sizeof(int));
        
        // Dispatch tasks round-robin (including self, rank 0)
        for (int i = 0; i < num_chunks; i++) {
            int offset = i * CHUNK_SIZE;
            int chunk_size = (offset + CHUNK_SIZE <= file_size) ? CHUNK_SIZE : (file_size - offset);
            int target = i % nprocs;
            if (target == 0) {
                if (mode == 0) { // compress
                    unsigned char *out_buf = NULL;
                    int out_size = 0;
                    compress_chunk(file_buf + offset, chunk_size, &out_buf, &out_size);
                    results[i] = out_buf;
                    result_sizes[i] = out_size;
                } else { // decompress
                    int orig_size;
                    memcpy(&orig_size, file_buf + offset, sizeof(int));
                    unsigned char *in_data = file_buf + offset + sizeof(int);
                    int in_data_size = chunk_size - sizeof(int);
                    unsigned char *out_buf = NULL;
                    int out_size = 0;
                    decompress_chunk(in_data, in_data_size, orig_size, &out_buf, &out_size);
                    results[i] = out_buf;
                    result_sizes[i] = out_size;
                }
            } else {
                if (mode == 0) {
                    int header[3] = {0, i, chunk_size};
                    MPI_Send(header, 3, MPI_INT, target, 0, MPI_COMM_WORLD);
                    MPI_Send(file_buf + offset, chunk_size, MPI_BYTE, target, 0, MPI_COMM_WORLD);
                } else {
                    int orig_size;
                    memcpy(&orig_size, file_buf + offset, sizeof(int));
                    int header[4] = {1, i, orig_size, chunk_size - (int)sizeof(int)};
                    MPI_Send(header, 4, MPI_INT, target, 0, MPI_COMM_WORLD);
                    MPI_Send(file_buf + offset + sizeof(int), chunk_size - sizeof(int), MPI_BYTE, target, 0, MPI_COMM_WORLD);
                }
            }
        }
        // Collect results from workers
        for (int i = 0; i < num_chunks; i++) {
            int target = i % nprocs;
            if (target != 0) {
                MPI_Status status;
                if (mode == 0) {
                    int recv_header[3];
                    MPI_Recv(recv_header, 3, MPI_INT, target, 1, MPI_COMM_WORLD, &status);
                    int chunk_id = recv_header[1];
                    int comp_size = recv_header[2];
                    unsigned char *comp_data = (unsigned char*) malloc(comp_size);
                    MPI_Recv(comp_data, comp_size, MPI_BYTE, target, 1, MPI_COMM_WORLD, &status);
                    results[chunk_id] = comp_data;
                    result_sizes[chunk_id] = comp_size;
                } else {
                    int recv_header[2];
                    MPI_Recv(recv_header, 2, MPI_INT, target, 1, MPI_COMM_WORLD, &status);
                    int chunk_id = recv_header[0];
                    int decomp_size = recv_header[1];
                    unsigned char *decomp_data = (unsigned char*) malloc(decomp_size);
                    MPI_Recv(decomp_data, decomp_size, MPI_BYTE, target, 1, MPI_COMM_WORLD, &status);
                    results[chunk_id] = decomp_data;
                    result_sizes[chunk_id] = decomp_size;
                }
            }
        }
        if (mode == 0)
            snprintf(out_filepath, sizeof(out_filepath), "%s/%s.mz", output_dir, ent->d_name);
        else
            snprintf(out_filepath, sizeof(out_filepath), "%s/%s.decomp", output_dir, ent->d_name);
        FILE *out_fp = fopen(out_filepath, "wb");
        if (!out_fp)
            fprintf(stderr, "Failed to open output file %s\n", out_filepath);
        else {
            for (int i = 0; i < num_chunks; i++) {
                fwrite(results[i], 1, result_sizes[i], out_fp);
                free(results[i]);
            }
            fclose(out_fp);
        }
        free(results);
        free(result_sizes);
        free(file_buf);
    }
    closedir(dir);
    // Terminate workers
    int term = -1;
    for (int proc = 1; proc < nprocs; proc++)
        MPI_Send(&term, 1, MPI_INT, proc, 0, MPI_COMM_WORLD);
}

void worker_process() {
    while (1) {
        MPI_Status status;
        int flag;
        MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        int count;
        MPI_Get_count(&status, MPI_INT, &count);
        if (count == 1) {
            int term;
            MPI_Recv(&term, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
            if (term == -1) break;
        }
        if (count == 3) { // compress task
            int header[3];
            MPI_Recv(header, 3, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
            int chunk_id = header[1], chunk_size = header[2];
            unsigned char *in_buf = (unsigned char*) malloc(chunk_size);
            MPI_Recv(in_buf, chunk_size, MPI_BYTE, 0, 0, MPI_COMM_WORLD, &status);
            unsigned char *out_buf = NULL;
            int out_size = 0;
            compress_chunk(in_buf, chunk_size, &out_buf, &out_size);
            int send_header[3] = {0, chunk_id, out_size};
            MPI_Send(send_header, 3, MPI_INT, 0, 1, MPI_COMM_WORLD);
            MPI_Send(out_buf, out_size, MPI_BYTE, 0, 1, MPI_COMM_WORLD);
            free(in_buf);
            free(out_buf);
        } else if (count == 4) { // decompress task
            int header[4];
            MPI_Recv(header, 4, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
            int chunk_id = header[1], orig_size = header[2], comp_size = header[3];
            unsigned char *in_buf = (unsigned char*) malloc(comp_size);
            MPI_Recv(in_buf, comp_size, MPI_BYTE, 0, 0, MPI_COMM_WORLD, &status);
            unsigned char *out_buf = NULL;
            int out_size = 0;
            decompress_chunk(in_buf, comp_size, orig_size, &out_buf, &out_size);
            int send_header[2] = {chunk_id, out_size};
            MPI_Send(send_header, 2, MPI_INT, 0, 1, MPI_COMM_WORLD);
            MPI_Send(out_buf, out_size, MPI_BYTE, 0, 1, MPI_COMM_WORLD);
            free(in_buf);
            free(out_buf);
        }
    }
}

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);
    int rank, nprocs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    if (argc < 4) {
        if (rank == 0)
            fprintf(stderr, "Usage: %s [compress|decompress] <input_directory> <output_directory>\n", argv[0]);
        MPI_Finalize();
        return 1;
    }
    int mode = (strcmp(argv[1], "compress") == 0) ? 0 : 1;
    if (rank == 0)
        master_process(nprocs, mode, argv[2], argv[3]);
    else
        worker_process();
    MPI_Finalize();
    return 0;
}
