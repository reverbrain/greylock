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
	bpo::options_description gr("Compaction options");
	gr.add_options()
		("path", bpo::value<std::string>(&dpath)->required(), "path to rocksdb database")
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

	try {
		ribosome::timer tm;

		greylock::database db;
		auto err = db.open_read_write(dpath);
		if (err) {
			std::cerr << "could not open database: " << err.message();
			return err.code();
		}

		long open_time = tm.elapsed();

		db.compact();

		long compact_time = tm.elapsed() - open_time;

		printf("Time to open database: %.2f seconds, compact: %.2f seconds\n", open_time / 1000., compact_time / 1000.); 
	} catch (const std::exception &e) {
		std::cerr << "Exception: " << e.what() << std::endl;
	}
}

