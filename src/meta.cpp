#include <iostream>

#include "greylock/sharded_database.hpp"
#include "greylock/types.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <ribosome/timer.hpp>

using namespace ioremap;

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Index metadata reader options");
	generic.add_options()
		("help", "this help message")
		;


	std::string dpath;
	std::vector<std::string> indexes_path, shards_path;
	std::string iname;
	bool dump = false;
	std::string id_str;
	std::string save_prefix;
	bpo::options_description gr("Greylock index options");
	gr.add_options()
		("index", bpo::value<std::string>(&iname), "index name, format: mailbox.attribute.index")
		("id", bpo::value<std::string>(&id_str), "read document with this indexed ID, format: ts")
		("save", bpo::value<std::string>(&save_prefix), "save index data into this directory")
		("rocksdb.docs", bpo::value<std::string>(&dpath),
		 	"path to rocksdb containing documents, "
			"will be opened in read-only mode, safe to be called if different process is already using it")
		("rocksdb.indexes", bpo::value<std::vector<std::string>>(&indexes_path)->required()->composing(),
		 	"path to rocksdb containing indexes, "
			"will be opened in read-only mode, safe to be called if different process is already using it, "
			"can be specified multiple times")
		("rocksdb.shards", bpo::value<std::vector<std::string>>(&shards_path)->composing(),
		 	"path to rocksdb containing shards, "
			"will be opened in read-only mode, safe to be called if different process is already using it, "
			"can be specified multiple times, if not specified, token_shards column from index databases will be used")
		("dump", "dump document data to stdout")
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

	if (dump || vm.count("id")) {
		if (dpath.empty()) {
			std::cerr << "You must provide documents database when using dump or id option\n" << cmdline_options << std::endl;
			return -1;
		}
	}

	try {
		greylock::sharded_database sdb;
		auto err = sdb.open_read_only(indexes_path, shards_path);
		if (err) {
			std::cerr << "could not open database: " << err.message() << std::endl;
			return err.code();
		}

		greylock::database db_docs;
		if (dpath.size()) {
			auto err = db_docs.open_read_only(dpath);
			if (err) {
				std::cerr << "could not open database: " << err.message() << std::endl;
				return err.code();
			}
		}

		ribosome::timer tm;

		if (vm.count("index")) {
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

			if (save_prefix.size()) {
				boost::system::error_code ec;
				std::string dname = save_prefix + "/" + iname;
				boost::filesystem::create_directories(dname, ec);
				if (ec && ec != boost::system::errc::file_exists) {
					fprintf(stderr, "could not create directory %s: %s [%d]\n",
							dname.c_str(), ec.message().c_str(), ec.value());
					return -ec.value();
				}

				save_prefix = dname;
			}

			const std::string &mbox = cmp[0];
			const std::string &attr = cmp[1];
			const std::string &token = cmp[2];

			std::string index_base = greylock::document::generate_index_base(greylock::options(), mbox, attr, token);
			std::string skey = greylock::document::generate_shard_key(greylock::options(), mbox, attr, token);
			std::vector<size_t> shards;
			auto err = sdb.read_shards(skey, &shards);
			if (err) {
				fprintf(stderr, "could not read shards %s: %s [%d]\n",
						skey.c_str(), err.message().c_str(), err.code());
				return err.code();
			}

			if (save_prefix.size()) {
				std::ofstream sout(save_prefix + "/shards.bin", std::ios::trunc);
				std::string sdata;
				auto err = sdb.read(greylock::options::token_shards_column, skey, &sdata);
				if (err) {
					fprintf(stderr, "could not read shards %s: %s [%d]\n",
							skey.c_str(), err.message().c_str(), err.code());
					return err.code();
				}

				sout.write(sdata.data(), sdata.size());
			}

			std::set<greylock::document_for_index> sidx;

			std::cout << "Number of shards: " << shards.size() << ", shards: " << greylock::dump_vector(shards) << std::endl;
			for (auto shard_number: shards) {
				std::string ikey = greylock::document::generate_index_key_shard_number(index_base, shard_number);
				std::string idata;
				auto err = sdb.read(greylock::options::indexes_column, ikey, &idata);
				if (err) {
					fprintf(stderr, "could not read index %s: %s [%d]\n",
							ikey.c_str(), err.message().c_str(), err.code());
					return err.code();
				}

				if (save_prefix.size()) {
					std::ofstream sout(save_prefix + "/idx_shard." + std::to_string(shard_number), std::ios::trunc);
					sout.write(idata.data(), idata.size());
				}


				greylock::disk_index idx;
				auto gerr = greylock::deserialize(idx, idata.data(), idata.size());
				if (gerr) {
					fprintf(stderr, "could not deserialize index %s, size: %ld: %s [%d]\n",
							ikey.c_str(), idata.size(), gerr.message().c_str(), gerr.code());
					return err.code();
				}

				std::cout << "shard: " << shard_number << ", indexes: " << idx.ids.size() << std::endl;
				sidx.insert(idx.ids.begin(), idx.ids.end());

				for (auto &id: idx.ids) {
					std::cout << "indexed_id: " << greylock::print_id(id.indexed_id);
					if (dump) {
						greylock::document doc;

						std::string doc_data;
						std::string dkey = id.indexed_id.to_string();
						auto err = db_docs.read(greylock::options::documents_column, dkey, &doc_data);
						if (err) {
							fprintf(stderr, "could not read document %s: %s [%d]\n",
									dkey.c_str(), err.message().c_str(), err.code());
							return err.code();
						}

						err = greylock::deserialize(doc, doc_data.data(), doc_data.size());
						if (err) {
							fprintf(stderr, "could not deserialize document %s, size: %ld: %s [%d]\n",
									dkey.c_str(), doc_data.size(), err.message().c_str(), err.code());
							return err.code();
						}

						std::cout << ", doc: " << greylock::print_doc(doc);
					}

					std::cout << std::endl;
				}
			}

			if (save_prefix.size()) {
				greylock::disk_index idx;
				idx.ids.insert(idx.ids.begin(), sidx.begin(), sidx.end());

				std::ofstream sout(save_prefix + "/idx_merged.bin", std::ios::trunc);
				std::string mdata = serialize(idx);
				sout.write(mdata.data(), mdata.size());
			}

		}

		if (vm.count("id")) {
			greylock::id_t indexed_id(id_str.c_str());

			std::string doc_data;
			auto err = db_docs.read(greylock::options::documents_column, indexed_id.to_string(), &doc_data);
			if (err) {
				std::cout << "could not read document with indexed_id: " << id_str <<
					", error: " << err.message() << std::endl;
				return err.code();
			}

			greylock::document doc;
			err = greylock::deserialize(doc, doc_data.data(), doc_data.size());
			if (err) {
				std::cout << "could not deserialize document with indexed_id: " << id_str <<
					", data_size: " << doc_data.size() <<
					", error: " << err.message() << std::endl;
				return err.code();
			}

			std::cout << "indexed_id: " << greylock::print_id(doc.indexed_id) <<
				", doc: " << greylock::print_doc(doc) << std::endl;
		}

		printf("Operation took %.2f seconds\n", tm.elapsed() / 1000.); 
	} catch (const std::exception &e) {
		std::cerr << "Exception: " << e.what() << std::endl;
	}
}
