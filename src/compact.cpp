#include <iostream>

#include "greylock/database.hpp"
#include "greylock/types.hpp"

#include <boost/program_options.hpp>

#include <ribosome/timer.hpp>

using namespace ioremap;

static inline const char *print_time(long tsec, long tnsec)
{
	char str[64];
	struct tm tm;

	static __thread char __dnet_print_time[128];

	localtime_r((time_t *)&tsec, &tm);
	strftime(str, sizeof(str), "%F %R:%S", &tm);

	snprintf(__dnet_print_time, sizeof(__dnet_print_time), "%s.%06llu", str, (long long unsigned) tnsec / 1000);
	return __dnet_print_time;
}

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Database compact options");
	generic.add_options()
		("help", "this help message")
		;


	std::string dpath;
	long csize_mb;
	std::string cname;
	bpo::options_description gr("Compaction options");
	gr.add_options()
		("path", bpo::value<std::string>(&dpath)->required(), "path to rocksdb database")
		("column", bpo::value<std::string>(&cname)->required(), "column name to compact")
		("size", bpo::value<long>(&csize_mb)->default_value(1024), "number of MBs to compact in one chunk")
		("full", "full compaction")
		;

	bpo::options_description cmdline_options;
	cmdline_options.add(generic).add(gr);

	bpo::variables_map vm;

	try {
		bpo::store(bpo::command_line_parser(argc, argv).options(cmdline_options).run(), vm);

		if (vm.count("help")) {
			std::cout << cmdline_options << std::endl;
			return 0;
		}

		bpo::notify(vm);
	} catch (const std::exception &e) {
		std::cerr << "Invalid options: " << e.what() << "\n" << cmdline_options << std::endl;
		return -1;
	}

	greylock::options opt;
	auto it = std::find(opt.column_names.begin(), opt.column_names.end(), cname);
	if (it == opt.column_names.end()) {
		std::cerr << "Invalig column " << cname << ", supported columns: " << greylock::dump_vector(opt.column_names) << std::endl;
		return -EINVAL;
	}

	auto column_id = std::distance(opt.column_names.begin(), it);

#define SECONDS(x) ((x) / 1000.)

	try {
		ribosome::timer tm;

		greylock::database db;
		auto err = db.open_read_write(dpath);
		if (err) {
			std::cerr << "could not open database: " << err.message();
			return err.code();
		}
		long open_time = tm.elapsed();
		printf("%.2fs : %.2fs: database %s has been opened\n", SECONDS(tm.elapsed()), SECONDS(open_time), dpath.c_str());
		long compaction_start_time = tm.elapsed();

		if (!vm.count("full")) {
			rocksdb::ReadOptions ro;
			auto it = db.iterator(column_id, ro);
			it->SeekToFirst();
			long position_time = tm.elapsed() - open_time;
			printf("%.2fs : %.2fs: database %s has been positioned\n", SECONDS(tm.elapsed()), SECONDS(position_time), dpath.c_str());

			if (!it->Valid()) {
				auto s = it->status();
				fprintf(stderr, "iterator is not valid: %s [%d]", s.ToString().c_str(), s.code());
				return -s.code();
			}

			long compact_size = csize_mb * 1024 * 1024;

			compaction_start_time = tm.elapsed();
			while (it->Valid()) {
				long compaction_tmp_start_time = tm.elapsed();

				long current_size = 0;
				std::string start, end;

				start = it->key().ToString();
				while (it->Valid() && current_size < compact_size) {
					current_size += it->value().size();
					end = it->key().ToString();

					it->Next();
				}

				db.compact(column_id, start, end);
				long compaction_time = tm.elapsed() - compaction_tmp_start_time;

				printf("%.2fs : %.2fs: %s: compaction: start: %s, end: %s, size: %.2f MB\n",
						SECONDS(tm.elapsed()), SECONDS(compaction_time), dpath.c_str(),
						start.c_str(), end.c_str(),
						current_size / (1024. * 1024.)); 
			}

			if (!it->Valid()) {
				auto s = it->status();
				if (s.code() != 0) {
					fprintf(stderr, "iterator has become invalid during iteration: %s [%d]", s.ToString().c_str(), s.code());
					return -s.code();
				}
			}
		} else {
			db.compact();
		}

		long compaction_time = tm.elapsed() - compaction_start_time;

		printf("%.2fs : %.2fs: database %s has been %s compacted\n",
				SECONDS(tm.elapsed()), SECONDS(compaction_time), dpath.c_str(),
				vm.count("full") ? "fully" : "");
	} catch (const std::exception &e) {
		std::cerr << "Exception: " << e.what() << std::endl;
	}
}

