#include "greylock/database.hpp"
#include "greylock/types.hpp"

#include <ribosome/rans.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>

#include <fstream>
#include <iostream>
#include <sstream>

using namespace ioremap;

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Index metadata reader for RANS arithmetic coder options");
	generic.add_options()
		("help", "this help message")
		;


	std::string path;
	std::vector<std::string> inames;
	std::string save_stats_file, load_stats_file;
	bpo::options_description gr("Greylock index options");
	gr.add_options()
		("index", bpo::value<std::vector<std::string>>(&inames)->required()->composing(),
			"index name, format: mailbox.attribute.index")
		("save-stats", bpo::value<std::string>(&save_stats_file), "gather stats from given indexes and save to this file")
		("load-stats", bpo::value<std::string>(&load_stats_file), "load previously saved stats from given file")
		("rocksdb", bpo::value<std::string>(&path)->required(),
		 	"path to rocksdb, will be opened in read-only mode, safe to be called if different process is already using it")
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
		return -EINVAL;
	}

	if (save_stats_file.empty() && load_stats_file.empty()) {
		std::cerr << "You must specify either save or load file for rANS statistics\n" << cmdline_options << std::endl;
		return -EINVAL;
	}

	greylock::read_only_database db;
	auto err = db.open(path);
	if (err) {
		std::cerr << "Could not open database: " << err.message();
		return err.code();
	}

	ribosome::rans rans;
	if (load_stats_file.size()) {
		std::ifstream in(load_stats_file.c_str());
		std::ostringstream ss;
		ss << in.rdbuf();
		std::string ls = ss.str();
		auto err = rans.load_stats(ls.data(), ls.size());
		if (err) {
			std::cerr << "Could not load stats: " << err.message() << ", code: " << err.code() << std::endl;
			return err.code();
		}
	}

	for (auto &iname: inames) {
		std::vector<std::string> cmp;
		size_t pos = 0;
		for (int i = 0; i < 2; ++i) {
			size_t dot = iname.find('.', pos);
			if (dot == std::string::npos) {
				std::cerr << "invalid index name " << iname << ", must be mailbox.attribute.index" << std::endl;
				return -1;
			}

			cmp.push_back(iname.substr(pos, dot - pos));
			pos = dot + 1;
		}
		cmp.push_back(iname.substr(pos));

		const auto &mbox = cmp[0];
		const auto &attr = cmp[1];
		const auto &token = cmp[2];

		std::string index_base = greylock::document::generate_index_base(db.options(), mbox, attr, token);

		std::vector<size_t> shards(db.get_shards(greylock::document::generate_shard_key(db.options(), mbox, attr, token)));
		std::cout << "index: " << iname << ", shards: " << greylock::dump_vector(shards) << std::endl;

		if (shards.size() == 0)
			return 0;

		for (auto shard: shards) {
			std::string key = greylock::document::generate_index_key_shard_number(index_base, shard);
			std::string data;
			auto err = db.read(key, &data);
			if (err) {
				std::cerr << "index: " << iname <<
					", shard: " << shard <<
					", shard key: " << key <<
					", read error: " << err.message() <<
					", code: " << err.code() <<
					std::endl;
				return err.code();
			}

			if (save_stats_file.size()) {
				rans.gather_stats((const uint8_t *)data.data(), data.size());
			} else {
				size_t offset = 0;
				std::vector<uint8_t> encoded;
				auto err = rans.encode_bytes((const uint8_t *)data.data(), data.size(), &encoded, &offset);
				std::cout << "index: " << iname <<
					", shard: " << shard <<
					", size: " << data.size() << " -> " << encoded.size() - offset <<
					std::endl;
				if (err) {
					std::cerr << "could not encode data: " << err.message() << std::endl;
					return err.code();
				}

				std::vector<uint8_t> decoded;
				decoded.resize(data.size());

				err = rans.decode_bytes(encoded.data() + offset, encoded.size() - offset, &decoded);
				if (err || decoded.size() != data.size() || memcmp(decoded.data(), data.data(), data.size())) {
					std::cerr << "index: " << iname <<
						", shard: " << shard <<
						", data mismatch: " <<
						", orig size: " << data.size() <<
						", decoded size: " << decoded.size() <<
						", err: " << err.message() <<
						std::endl;
					return err.code();
				}
			}
		}
	}

	if (save_stats_file.size()) {
		std::string ds = rans.save_stats();
		if (ds.size() == 0) {
			std::cerr << "Invalid zero stats" << std::endl;
			return -EINVAL;
		}

		std::ofstream out(save_stats_file.c_str(), std::ios::trunc);
		out.write(ds.data(), ds.size());
		out.flush();
	}

	return 0;
}
