#include "greylock/database.hpp"
#include "greylock/types.hpp"

#include <ribosome/error.hpp>

#include <boost/program_options.hpp>

#include <rocksdb/comparator.h>

using namespace ioremap;

static void list(const std::string &input, int column) {
	std::unique_ptr<greylock::database> dbu(new greylock::database());
	auto err = dbu->open_read_only(input);
	if (err) {
		ribosome::throw_error(err.code(), "could not open input database: %s: %s",
				input.c_str(), err.message().c_str());
	}

	auto it = dbu->iterator(column, rocksdb::ReadOptions());
	it->SeekToFirst();

	if (!it->Valid()) {
		auto s = it->status();
		ribosome::throw_error(-s.code(), "iterator from database %s is not valid: %s [%d]",
				input.c_str(), s.ToString().c_str(), s.code());
	}

	long data_size = 0;
	long keys = 0;
	for (; it->Valid(); it->Next()) {
		keys++;
		data_size += it->value().size();

		printf("merge: column: %s [%d], keys: %ld, total data size: %ld, key: %s, size: %ld\n",
				dbu->options().column_names[column].c_str(), column, keys, data_size,
				it->key().ToString().c_str(), it->value().size());
	}

}

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("List options");

	std::string input;
	std::string column;
	generic.add_options()
		("help", "This help message")
		("column", bpo::value<std::string>(&column)->required(), "Column name to merge")
		("rocksdb", bpo::value<std::string>(&input)->required(), "Input rocksdb database")
		;

	bpo::options_description cmdline_options;
	cmdline_options.add(generic);

	bpo::variables_map vm;

	try {
		bpo::store(bpo::command_line_parser(argc, argv).options(cmdline_options).run(), vm);

		if (vm.count("help")) {
			std::cout << generic << std::endl;
			return 0;
		}

		bpo::notify(vm);
	} catch (const std::exception &e) {
		std::cerr << "Invalid options: " << e.what() << "\n" << generic << std::endl;
		return -1;
	}

	greylock::options opt;
	auto it = std::find(opt.column_names.begin(), opt.column_names.end(), column);
	if (it == opt.column_names.end()) {
		std::cerr << "Invalig column " << column << ", supported columns: " << greylock::dump_vector(opt.column_names) << std::endl;
		return -EINVAL;
	}

	auto column_id = std::distance(opt.column_names.begin(), it);

	try {
		list(input, column_id);
	} catch (const std::exception &e) {
		std::cerr << "Exception: " << e.what() << std::endl;
		return -1;
	}
}
