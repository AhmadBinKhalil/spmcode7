// ff_compressor.cpp
#include <ff/ff.hpp>
#include <ff/pipeline.hpp>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include "miniz.h"
#include <vector>
#include <string>
#include <iostream>
#include <map>
#include <algorithm>
#include <memory>
#include <fcntl.h>
#include <unistd.h>

using namespace ff;
using namespace std;

#define CHUNK_SIZE (1024*1024)
enum { COMPRESS = 0, DECOMPRESS = 1 };

//
// Task: Represents a chunk of a file.
// The sentinel task (EOS) is indicated by isEos==true.
// For normal tasks, fileName remains unchanged.
//
struct Task {
    bool isEos;            // true if this is the EOS sentinel
    int mode;              // COMPRESS or DECOMPRESS
    string fileName;       // For normal tasks, this is the file name (unchanged)
    int chunk_id;
    int offset;
    int size;
    vector<unsigned char> data;
    int orig_size;         // For decompression: expected output size
    Task() : isEos(false), mode(0), chunk_id(0), offset(0), size(0), orig_size(0) {}
};

//
// Emitter: Reads the input directory, splits each regular file into chunks,
// creates a Task for each chunk, and sends them downstream.
// At the end, it creates and sends a sentinel Task with isEos==true.
//
struct Emitter: public ff_node_t<Task> {
    string inputDir;
    int mode;
    Emitter(const string &inDir, int m) : inputDir(inDir), mode(m) { }
    
    Task* svc(Task* /*unused*/) override {
        cerr << "Emitter: Starting to read directory '" << inputDir << "'." << endl;
        DIR *dir = opendir(inputDir.c_str());
        if (!dir) {
            cerr << "Emitter: Failed to open directory: " << inputDir << endl;
            // Create EOS sentinel even on error.
            Task* eosTask = new Task();
            eosTask->isEos = true;
            return eosTask;
        }
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (ent->d_name[0] == '.') continue;  // Skip hidden files
            string filePath = inputDir + "/" + ent->d_name;
            struct stat st;
            if (stat(filePath.c_str(), &st) < 0) continue;
            if (!S_ISREG(st.st_mode)) continue;  // Only process regular files
            FILE *fp = fopen(filePath.c_str(), "rb");
            if (!fp) {
                cerr << "Emitter: Failed to open file: " << filePath << endl;
                continue;
            }
            fseek(fp, 0, SEEK_END);
            int fileSize = ftell(fp);
            fseek(fp, 0, SEEK_SET);
            vector<unsigned char> fileData(fileSize);
            fread(fileData.data(), 1, fileSize, fp);
            fclose(fp);
            int num_chunks = (fileSize + CHUNK_SIZE - 1) / CHUNK_SIZE;
            cerr << "Emitter: Processing file '" << ent->d_name << "' (" 
                 << fileSize << " bytes) in " << num_chunks << " chunks." << endl;
            for (int i = 0; i < num_chunks; i++) {
                int offset = i * CHUNK_SIZE;
                int chunk_size = (offset + CHUNK_SIZE <= fileSize) ? CHUNK_SIZE : (fileSize - offset);
                Task* task = new Task();
                task->isEos = false;
                task->mode = mode;
                task->fileName = ent->d_name;  // Keep original file name
                task->chunk_id = i;
                task->offset = offset;
                task->size = chunk_size;
                task->data.resize(chunk_size);
                memcpy(task->data.data(), fileData.data() + offset, chunk_size);
                if (mode == DECOMPRESS) {
                    // Assume the first sizeof(int) bytes store the original size.
                    memcpy(&task->orig_size, task->data.data(), sizeof(int));
                    task->data.erase(task->data.begin(), task->data.begin() + sizeof(int));
                    task->size -= sizeof(int);
                }
                cerr << "Emitter: Created task for file '" << task->fileName 
                     << "', chunk " << task->chunk_id << " (size " << chunk_size << " bytes)." << endl;
                ff_send_out(task);
            }
        }
        closedir(dir);
        cerr << "Emitter: Finished reading directory. Sending EOS sentinel." << endl;
        Task* eosTask = new Task();
        eosTask->isEos = true;
        ff_send_out(eosTask);
        return eosTask;
    }
};

//
// Worker: Processes each Task by compressing (or decompressing) its data.
// If it receives a sentinel task (isEos==true), it simply propagates it.
//
struct Worker: public ff_node_t<Task> {
    Task* svc(Task* task) override {
        if (task->isEos) {
            cerr << "Worker: Received EOS sentinel. Propagating it." << endl;
            return task;
        }
        if (task->mode == COMPRESS) {
            uLongf compBound = compressBound(task->size);
            vector<unsigned char> compData(compBound);
            uLongf compSize = compBound;
            int ret = compress(compData.data(), &compSize, task->data.data(), task->size);
            if (ret != Z_OK) {
                cerr << "Worker: Compression failed for file '" << task->fileName 
                     << "', chunk " << task->chunk_id << ". Error code: " << ret << endl;
                // Propagate original task in case of error.
                return task;
            }
            compData.resize(compSize);
            task->data = move(compData);
            cerr << "Worker: Compressed file '" << task->fileName << "', chunk " 
                 << task->chunk_id << ". Original size: " << task->size 
                 << ", compressed size: " << compSize << endl;
        } else {
            vector<unsigned char> decompData(task->orig_size);
            uLongf decompSize = task->orig_size;
            int ret = uncompress(decompData.data(), &decompSize, task->data.data(), task->size);
            if (ret != Z_OK) {
                cerr << "Worker: Decompression failed for file '" << task->fileName 
                     << "', chunk " << task->chunk_id << ". Error code: " << ret << endl;
                return task;
            }
            task->data = move(decompData);
            cerr << "Worker: Decompressed file '" << task->fileName << "', chunk " 
                 << task->chunk_id << ". Expected size: " << task->orig_size 
                 << ", decompressed size: " << decompSize << endl;
        }
        return task;
    }
};

//
// Collector: Collects all tasks for each file and, upon receiving the EOS sentinel,
// writes out the combined data to an output file. (File names are not changed. For compression,
// the output file is named "<original_file_name>.mz".)
//
struct Collector: public ff_node_t<Task> {
    string outputDir;
    int mode;
    map<string, vector<Task*>> fileTasks;
    Collector(const string &outDir, int m) : outputDir(outDir), mode(m) { }
    
    Task* svc(Task* task) override {
        if (task->isEos) {  // EOS sentinel received.
            cerr << "Collector: Received EOS sentinel. Finalizing file writes." << endl;
            for (auto &entry : fileTasks) {
                const string &fname = entry.first;
                vector<Task*>& tasks = entry.second;
                cerr << "Collector: For file '" << fname << "', received " << tasks.size() << " chunks." << endl;
                int totalBytes = 0;
                for (Task* t : tasks)
                    totalBytes += t->data.size();
                cerr << "Collector: Total bytes for file '" << fname << "': " << totalBytes << endl;
                
                // Sort tasks by chunk_id.
                sort(tasks.begin(), tasks.end(), [](Task* a, Task* b) {
                    return a->chunk_id < b->chunk_id;
                });
                // Construct output file name (do not change the original file name; just add extension).
                string outPath = (mode == COMPRESS)
                    ? (outputDir + "/" + fname + ".mz")
                    : (outputDir + "/" + fname + ".decomp");
                FILE *fp = fopen(outPath.c_str(), "wb");
                if (!fp) {
                    cerr << "Collector: Failed to open output file: " << outPath << endl;
                    continue;
                }
                for (Task* t : tasks) {
                    size_t written = fwrite(t->data.data(), 1, t->data.size(), fp);
                    cerr << "Collector: Writing chunk " << t->chunk_id << " of file '" << fname 
                         << "' (" << t->data.size() << " bytes), written: " << written << " bytes." << endl;
                    delete t;
                }
                fclose(fp);
                struct stat statbuf;
                if (stat(outPath.c_str(), &statbuf) == 0) {
                    cerr << "Collector: Finished writing file '" << outPath 
                         << "'. Final file size: " << statbuf.st_size << " bytes." << endl;
                } else {
                    cerr << "Collector: Finished writing file '" << outPath << "', unable to retrieve file size." << endl;
                }
            }
            return task; // Propagate EOS if needed.
        }
        // For a normal task, record it in the map.
        cerr << "Collector: Received task for file '" << task->fileName 
             << "', chunk " << task->chunk_id << " (size " << task->data.size() << " bytes)." << endl;
        fileTasks[task->fileName].push_back(task);
        return task;
    }
};

//
// Main: Builds a sequential pipeline (Emitter → Worker → Collector) and runs it.
//
int main(int argc, char* argv[]){
    if (argc < 4) {
        cerr << "Usage: " << argv[0] << " [compress|decompress] <input_directory> <output_directory>" << endl;
        return 1;
    }
    int mode = (strcmp(argv[1], "compress") == 0) ? COMPRESS : DECOMPRESS;
    Emitter emitter(argv[2], mode);
    Worker worker;
    Collector collector(argv[3], mode);
    
    ff_Pipe<Task> pipe(emitter, worker, collector);
    cerr << "Main: Starting pipeline execution." << endl;
    if (pipe.run_and_wait_end() < 0) {
        error("Main: running pipeline");
        return -1;
    }
    cerr << "Main: Pipeline execution finished." << endl;
    return 0;
}
