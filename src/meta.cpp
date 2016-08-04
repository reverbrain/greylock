#include <iostream>

#include "greylock/database.hpp"
#include "greylock/iterator.hpp"

#include <boost/algorithm/string.hpp>
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

template <typename T>
std::string dump_vector(const std::vector<T> &vec) {
	std::ostringstream ss;
	for (size_t i = 0; i < vec.size(); ++i) {
		ss << vec[i];
		if (i != vec.size() - 1)
			ss << " ";
	}

	return ss.str();
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
	size_t indexed_id;
	bpo::options_description gr("Greylock index options");
	gr.add_options()
		("index", bpo::value<std::string>(&iname), "index name")
		("indexed-id", bpo::value<size_t>(&indexed_id), "read document with this indexed ID")
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

		auto print_doc = [&](const greylock::document &doc) -> std::string {
			std::ostringstream ss;

			ss << ", id: " << doc.id <<
				", author: " << doc.author <<
				", ts: " << print_time(&doc.ts) <<
				", data size: " << doc.data.size();

			if (dump_data) {
				ss << "\n  data: " << doc.data;
				ss << "\n  content:";
				ss << "\n           content: " << dump_vector(doc.ctx.content);
				ss << "\n   stemmed content: " << dump_vector(doc.ctx.stemmed_content);
				ss << "\n             title: " << dump_vector(doc.ctx.title);
				ss << "\n     stemmed title: " << dump_vector(doc.ctx.stemmed_title);
				ss << "\n             links: " << dump_vector(doc.ctx.links);
				ss << "\n            images: " << dump_vector(doc.ctx.images);
			}

			return ss.str();
		};

		if (vm.count("index")) {
			std::vector<std::string> cmp;
			boost::split(cmp, iname, boost::is_any_of("."));

			for (auto it = greylock::index_iterator<greylock::read_only_database>::begin(db, cmp[0], cmp[1], cmp[2]),
					end = greylock::index_iterator<greylock::read_only_database>::end(db, cmp[0], cmp[1], cmp[2]);
					it != end;
					++it) {
				std::cout << "indexed_id: " << it->indexed_id;
				if (dump) {
					greylock::document doc;
					err = it.document(&doc);
					if (err) {
						std::cout << ", error: " << err.message();
					} else {
						std::cout << print_doc(doc);
					}
				}

				std::cout << std::endl;
			}
		}

		if (vm.count("indexed-id")) {
			std::string doc_data;
			auto err = db.read(db.opts.document_prefix + std::to_string(indexed_id), &doc_data);
			if (err) {
				std::cout << "could not read document with indexed_id: " << indexed_id <<
					", error: " << err.message() << std::endl;
				return err.code();
			}

			greylock::document doc;
			err = greylock::deserialize(doc, doc_data.data(), doc_data.size());
			if (err) {
				std::cout << "could not deserialize document with indexed_id: " << indexed_id <<
					", data_size: " << doc_data.size() <<
					", error: " << err.message() << std::endl;
				return err.code();
			}

			std::cout << "doc: indexed_id: " << indexed_id << print_doc(doc) << std::endl;
		}
	} catch (const std::exception &e) {
		std::cerr << "Exception: " << e.what() << std::endl;
	}
}
