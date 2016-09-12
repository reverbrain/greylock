#include <iostream>

#include "greylock/database.hpp"
#include "greylock/types.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>

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

	bpo::options_description generic("Index metadata reader options");
	generic.add_options()
		("help", "this help message")
		;


	std::string path;
	std::string iname;
	bool dump = false;
	std::string id_str;
	bpo::options_description gr("Greylock index options");
	gr.add_options()
		("index", bpo::value<std::string>(&iname), "index name, format: mailbox.attribute.index")
		("id", bpo::value<std::string>(&id_str), "read document with this indexed ID, format: ts.seq")
		("rocksdb", bpo::value<std::string>(&path)->required(),
		 	"path to rocksdb, will be opened in read-only mode, safe to be called if different process is already using it")
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

	try {
		greylock::database db;
		auto err = db.open_read_only(path);
		if (err) {
			std::cerr << "could not open database: " << err.message();
			return err.code();
		}

		auto print_index = [&](const greylock::id_t &id) -> std::string {
			long tsec, tnsec;
			id.get_timestamp(&tsec, &tnsec);

			std::ostringstream ss;
			ss << id.to_string() <<
				", raw_ts: " << id.timestamp <<
				", hash: " << id.seq <<
				", ts: " << print_time(tsec, tnsec);
			return ss.str();
		};

		auto print_doc = [&](const greylock::document &doc) -> std::string {
			std::ostringstream ss;

			ss << "id: " << doc.id << ", author: " << doc.author;

			ss << "\n          content: " << doc.ctx.content;
			ss << "\n            title: " << doc.ctx.title;
			ss << "\n            links: " << greylock::dump_vector(doc.ctx.links);
			ss << "\n           images: " << greylock::dump_vector(doc.ctx.images);

			return ss.str();
		};

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

			const std::string &mbox = cmp[0];
			const std::string &attr = cmp[1];
			const std::string &token = cmp[2];

			std::string index_base = greylock::document::generate_index_base(db.options(), mbox, attr, token);
			std::vector<size_t> shards(db.get_shards(greylock::document::generate_shard_key(db.options(), mbox, attr, token)));

			std::cout << "Number of shards: " << shards.size() << ", shards: " << greylock::dump_vector(shards) << std::endl;
			for (auto shard_number: shards) {
				std::string ikey = greylock::document::generate_index_key_shard_number(index_base, shard_number);
				std::string idata;
				auto err = db.read(ikey, &idata);
				if (err) {
					fprintf(stderr, "could not read index %s: %s [%d]\n",
							ikey.c_str(), err.message().c_str(), err.code());
					return err.code();
				}

				greylock::disk_index idx;
				err = greylock::deserialize(idx, idata.data(), idata.size());
				if (err) {
					fprintf(stderr, "could not deserialize index %s, size: %ld: %s [%d]\n",
							ikey.c_str(), idata.size(), err.message().c_str(), err.code());
					return err.code();
				}

				std::cout << "shard: " << shard_number << ", indexes: " << idx.ids.size() << std::endl;

				for (auto &id: idx.ids) {
					std::cout << "indexed_id: " << print_index(id.indexed_id);
					if (dump) {
						greylock::document doc;

						std::string doc_data;
						std::string dkey = db.options().document_prefix + id.indexed_id.to_string();
						auto err = db.read(dkey, &doc_data);
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

						std::cout << ", doc: " << print_doc(doc);
					}

					std::cout << std::endl;
				}
			}
		}

		if (vm.count("id")) {
			auto pos = id_str.find('.');
			if (pos == 0 || pos == std::string::npos) {
				std::cerr << "invalid ID string: " << id_str << std::endl;
				return -1;
			}

			greylock::id_t indexed_id(id_str.c_str());

			std::string doc_data;
			auto err = db.read(db.options().document_prefix + indexed_id.to_string(), &doc_data);
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

			std::cout << "indexed_id: " << print_index(doc.indexed_id) <<
				", doc: " << print_doc(doc) << std::endl;
		}
	} catch (const std::exception &e) {
		std::cerr << "Exception: " << e.what() << std::endl;
	}
}
