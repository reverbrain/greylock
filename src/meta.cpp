#include <iostream>

#include "greylock/database.hpp"
#include "greylock/iterator.hpp"

#include <boost/program_options.hpp>

using namespace ioremap;

static inline const char *print_time(const struct timespec *ts)
{
	char str[64];
	struct tm tm;

	static __thread char __dnet_print_time[128];

	localtime_r((time_t *)&ts->tv_sec, &tm);
	strftime(str, sizeof(str), "%F %R:%S", &tm);

	snprintf(__dnet_print_time, sizeof(__dnet_print_time), "%s.%06llu", str, (long long unsigned) ts->tv_nsec / 1000);
	return __dnet_print_time;
}

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Index metadata reader options");
	generic.add_options()
		("help", "this help message")
		;


	std::string path;
	std::string iname;
	bool dump = false;
	bool dump_data = false;
	bpo::options_description gr("Greylock index options");
	gr.add_options()
		("index", bpo::value<std::string>(&iname)->required(), "index name")
		("rocksdb", bpo::value<std::string>(&path)->required(),
		 	"path to rocksdb, will be opened in read-only mode, safe to be called if different process is already using it")
		("dump", "dump document meta (id, timestamp, data size) to stdout")
		("dump-data", "dump document data to stdout")
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

	if (vm.count("dump")) {
		dump = true;
	}
	if (vm.count("dump-data")) {
		dump = true;
		dump_data = true;
	}

	try {
		greylock::read_only_database db;
		auto err = db.open(path);
		if (err) {
			std::cerr << "could not open database: " << err.message();
			return err.code();
		}

		for (auto it = greylock::index_iterator<greylock::read_only_database>::begin(db, iname),
				end = greylock::index_iterator<greylock::read_only_database>::end(db, iname);
				it != end;
				++it) {
			std::cout << "indexed_id: " << *it;
			if (dump) {
				greylock::document doc;
				err = it.document(&doc);
				if (err) {
					std::cout << ", error: " << err.message();
				} else {
					std::cout << ", id: " << doc.id <<
						", ts: " << print_time(&doc.ts) <<
						", data size: " << doc.data.size();

					if (dump_data) {
						std::cout << ", data: " << doc.data;
					}
				}
			}

			std::cout << std::endl;
		}
	} catch (const std::exception &e) {
		std::cerr << "Exception: " << e.what() << std::endl;
	}
}
