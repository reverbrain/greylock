#include "greylock/database.hpp"
#include "greylock/types.hpp"

#include <boost/program_options.hpp>

using namespace ioremap;

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Options");

	std::string input;
	std::string column;
	generic.add_options()
		("help", "This help message")
		("column", bpo::value<std::string>(&column)->required(), "Column name to check")
		("input", bpo::value<std::string>(&input)->required(), "Input rocksdb database")
		("last", "show last document")
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
	auto cit = std::find(opt.column_names.begin(), opt.column_names.end(), column);
	if (cit == opt.column_names.end()) {
		std::cerr << "Invalig column " << column << ", supported columns: " << greylock::dump_vector(opt.column_names) << std::endl;
		return -EINVAL;
	}

	auto column_id = std::distance(opt.column_names.begin(), cit);
	greylock::database db;
	auto err = db.open_read_only(input);
	if (err) {
		std::cerr << "Could not open database " << input << ": " << err.message() << std::endl;
		return err.code();
	}

	auto it = db.iterator(column_id, rocksdb::ReadOptions());
	if (vm.count("last")) {
		it->SeekToLast();
	} else {
		it->SeekToFirst();
	}

	if (!it->Valid()) {
		auto s = it->status();
		std::cerr << "Iterator from database " << input << " is not valid: " << s.ToString() << " [" << s.code() << "]" << std::endl;
		return -s.code();
	}

	auto key = it->key().ToString();
	std::cout << "key: " << key << std::endl;

	auto value = it->value();

	switch (column_id) {
	case greylock::options::documents_column: {
		greylock::document doc;
		auto err = greylock::deserialize(doc, value.data(), value.size());
		if (err) {
			std::cerr << "Could not deserialize document: " << err.message() << " [" << err.code() << "]" << std::endl;
			return err.code();
		}

		std::cout << greylock::print_doc(doc) << std::endl;
		break;
	}
	case greylock::options::indexes_column: {
		greylock::disk_index idx;
		auto err = greylock::deserialize(idx, value.data(), value.size());
		if (err) {
			std::cerr << "Could not deserialize index: " << err.message() << " [" << err.code() << "]" << std::endl;
			return err.code();
		}

		for (auto &id: idx.ids) {
			std::cout << "indexed_id: " << greylock::print_id(id.indexed_id) << std::endl;
		}
		break;
	}
	default:
		break;
	}
}
