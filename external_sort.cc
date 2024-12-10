#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <iostream>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/loop.hh>
#include <boost/iterator/counting_iterator.hpp>
#include <seastar/core/memory.hh>
#include <seastar/core/aligned_buffer.hh>



using namespace std::chrono_literals;

constexpr int64_t kBlobSize = 4096;
constexpr int64_t kMergeFactor = 5;
constexpr char kSortedFileSuffix[] = ".sorted";

int64_t AvailableRam() {
    return seastar::memory::free_memory();
}

// Wrapper to represent a part of the file.
struct FileChunk {
    int64_t offset;
    int64_t length;

    FileChunk(int64_t o, int64_t l) : offset(o), length(l) {}

    std::string TempChunkOutFileName(const std::string& prefix) {
        return prefix + ".tmp." + std::to_string(offset) + "_" + std::to_string(length);
    }
};

// Tracks a temporary file being merged during the merge phase of the sort.
struct MergeChunkInfo {
    seastar::file file;
    std::string filename;
    int64_t pos = 0;
    int64_t length = 0;
    std::optional<std::string> next_value;
    MergeChunkInfo(seastar::file f, std::string filename, int64_t length) :
        file(std::move(f)), filename(filename), length(length) {}

    seastar::future<> MaybeFetchNext() {
        if (eof() || next_value.has_value()) return seastar::make_ready_future<>();

        return file.dma_read<char>(pos, kBlobSize).then([this](seastar::temporary_buffer<char> buf) {
            // TODO: error handling.
            next_value = std::string(buf.get(), kBlobSize);
            pos += kBlobSize;
        });
    }

    bool eof() {
        return !next_value.has_value() && pos >= length;
    }
};

// Wrapper class encapsulating merge phase metadata, including all chunks being merged.
struct MergeInfo {
    std::vector<MergeChunkInfo> chunk_infos;

    MergeChunkInfo* min_value_chunk = nullptr;

    seastar::file out_file;
    int64_t out_file_pos = 0;

    bool eof() {
        for (auto& c : chunk_infos) {
            if (!c.eof()) {
                return false;
            }
        }
        return true;
    }
};

seastar::future<> MergeChunks(std::string filename, std::vector<FileChunk>& chunks, std::string out_filename) {
    std::cout << "Merging " << chunks.size() << " chunks" << std::endl;
    return seastar::open_file_dma(out_filename, seastar::open_flags::rw | seastar::open_flags::create)
            .then([filename, chunks, out_filename] (seastar::file of) {
        MergeInfo merge_info;
        merge_info.out_file = std::move(of);
        return seastar::do_with(std::move(merge_info), std::move(chunks),
                                [filename](auto& merge_info, auto& chunks) {
            return seastar::do_for_each(chunks, [&merge_info, filename](FileChunk c) {
                auto chunk_filename = c.TempChunkOutFileName(filename);
                return seastar::open_file_dma(chunk_filename, seastar::open_flags::rw).then(
                    [&merge_info, chunk_filename] (seastar::file cf) {
                        return cf.size().then([&merge_info, cf, chunk_filename](size_t size) {
                            assert(size % kBlobSize == 0);
                            merge_info.chunk_infos.emplace_back(cf, chunk_filename, size);
                        });
                    }
                );
            }).then([&merge_info]() {
                return seastar::do_until([&merge_info] { return merge_info.eof(); }, [&merge_info] () {
                    return seastar::do_for_each(merge_info.chunk_infos, [&merge_info](MergeChunkInfo& ci) {
                        return ci.MaybeFetchNext().then([&ci, &merge_info]() {
                            if (ci.eof()) {
                                return seastar::make_ready_future<>();
                            }
                            if (!merge_info.min_value_chunk) {
                                merge_info.min_value_chunk = &ci;
                                return seastar::make_ready_future<>(); 
                            }
                            const std::string& existing_min_value = merge_info.min_value_chunk->next_value.value();
                            const std::string& current_value = ci.next_value.value();
                            if (current_value < existing_min_value) {
                                merge_info.min_value_chunk = &ci;
                            }
                            return seastar::make_ready_future<>();
                        });
                    }).then([&merge_info]() {
                        const char* src_pos = merge_info.min_value_chunk->next_value.value().data();
                        return merge_info.out_file.dma_write(/*pos=*/merge_info.out_file_pos, src_pos, kBlobSize)
                               .then([&merge_info] (size_t) {
                            merge_info.out_file_pos += kBlobSize;
                            merge_info.min_value_chunk->next_value = std::nullopt;
                            merge_info.min_value_chunk = nullptr;
                            return seastar::make_ready_future<>();
                        });
                    });
                    
                }).then([&merge_info] () {
                    return seastar::do_for_each(merge_info.chunk_infos, [&merge_info](MergeChunkInfo& ci) {
                        return seastar::remove_file(ci.filename);
                    });
                });
            });
        });
    });
}

std::vector<FileChunk> Chunkify(int64_t offset, int64_t length) {
    std::vector<FileChunk> chunks;
    int64_t max_blocks_in_chunk = std::ceil((static_cast<double>(length / kMergeFactor) / kBlobSize));
    int64_t max_chunk_length = max_blocks_in_chunk * kBlobSize;
    int64_t current = offset;
    while (current < (offset + length)) {
        int64_t chunk_offset = current;
        int64_t chunk_length = std::min(max_chunk_length, offset + length - current);
        chunks.emplace_back(chunk_offset, chunk_length);
        current += chunk_length;
    }
    return chunks;
}

// Recursive keeps shading the original file data untill each shard/chunk can
// can be sorted in-memory, then performs recursive merging of these shards.
seastar::future<> ExternalSort(const std::string& filename, FileChunk chunk, std::string out_filename) {
    // Sort the full chunk it's small enough. Although we should try to fully
    // utilize free memory, naively using only third of the memory as we will
    // make a copy from the read buffer, and also to loosely account for other
    // allocations. We can do better here.
    if (chunk.length * 3 <= AvailableRam()) {
        std::cout << "Sorting chunk [" << chunk.offset << "," << chunk.length << "] in memory" << std::endl;
        return seastar::open_file_dma(filename, seastar::open_flags::rw).then([chunk, out_filename](seastar::file f) {
            return f.dma_read<char>(static_cast<uint64_t>(chunk.offset), static_cast<size_t>(chunk.length)).then(
                    [chunk, out_filename] (seastar::temporary_buffer<char> buf) {
                assert(chunk.length % kBlobSize == 0);
                std::vector<std::string> blobs;
                int64_t num_blobs = chunk.length / kBlobSize;
                for (int64_t i = 0; i < num_blobs; ++i) {
                    blobs.push_back(std::string(buf.get() + (i * kBlobSize), kBlobSize));
                }
                std::sort(blobs.begin(), blobs.end());
                return seastar::open_file_dma(out_filename, seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate)
                        .then([blobs = std::move(blobs)](seastar::file of) {
                    return seastar::do_with(std::move(blobs), std::move(of),
                                            [](std::vector<std::string>& blobs,
                                                                    seastar::file& of) {
                        int64_t num_values = blobs.size();
                        // TODO: This is still not optimal as we are writing
                        // 4Kib chunks at a time. Suspecting some sort of
                        // batching would be very helpful.
                        return seastar::parallel_for_each(boost::counting_iterator<int64_t>(0),
                                                    boost::counting_iterator<int64_t>(num_values),
                                                    [&blobs, &of](int64_t i) {
                            size_t output_pos = i * kBlobSize;
                            return of.dma_write<char>(output_pos, blobs[i].data(), kBlobSize).then([output_pos](size_t) {
                                return seastar::make_ready_future<>();
                            });
                        });         
                    });
                });
            });
        }).then([] {
            return seastar::make_ready_future<>();
        });
    }

    std::vector<FileChunk> chunks = Chunkify(chunk.offset, chunk.length);
    return seastar::do_with(std::move(chunks), [filename, out_filename](std::vector<FileChunk>& chunks) {
        return seastar::do_for_each(chunks, [filename, out_filename](FileChunk c) {
            const std::string chunk_out_filename = c.TempChunkOutFileName(filename);
            return ExternalSort(filename, c, chunk_out_filename);
        }).then([filename, &chunks, out_filename] () {
            return MergeChunks(filename, chunks, out_filename); 
        });
    });
}

seastar::future<> ExternalSort(const std::string filename, const std::string out_filename) {
    return seastar::open_file_dma(filename, seastar::open_flags::rw).then([filename, out_filename] (seastar::file f) {
        return f.size().then([filename, out_filename] (size_t size) {
            return ExternalSort(filename, FileChunk(0, size), out_filename);
        });
    });
}


int main(int argc, char** argv) {
    seastar::app_template app;
    namespace bpo = boost::program_options;
    app.add_positional_options({{"filename", bpo::value<seastar::sstring>()->default_value(""),
    "file to sort", 1}});
    try {
        app.run(argc, argv, [&app]{
            auto& args = app.configuration();
            auto& filename = args["filename"].as<seastar::sstring>();
            auto sorted_filename = filename + kSortedFileSuffix;
            assert(!filename.empty());
            return ExternalSort(filename, sorted_filename);
        });
    } catch(...) {
        std::cerr << "Couldn't start application: " << std::current_exception() << std::endl;
    }
    return 0;
}
