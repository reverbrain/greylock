#include "greylock/database.hpp"
#include "greylock/types.hpp"

#include <ribosome/error.hpp>

#include <boost/program_options.hpp>

#include <rocksdb/comparator.h>

using namespace ioremap;

class merger {
public:
	void merge(const std::string &output, const std::vector<std::string> &inputs) {
		greylock::database odb;
		auto err = odb.open_read_write(output);
		if (err) {
			ribosome::throw_error(err.code(), "could not open output database: %s: %s",
					output.c_str(), err.message().c_str());
		}

		std::vector<std::unique_ptr<greylock::database>> dbs;
		std::vector<rocksdb::Iterator *> its;
		rocksdb::ReadOptions ro;

		for (auto &path: inputs) {
			std::unique_ptr<greylock::database> dbu(new greylock::database());
			err = dbu->open_read_only(path);
			if (err) {
				ribosome::throw_error(err.code(), "could not open input database: %s: %s",
						path.c_str(), err.message().c_str());
			}

			auto it = dbu->iterator(ro);
			it->SeekToFirst();

			if (!it->Valid()) {
				ribosome::throw_error(-EINVAL, "iterator from database %s is not valid", path.c_str());
			}

			its.emplace_back(it);
			dbs.emplace_back(std::move(dbu));
		}

		auto cmp = rocksdb::BytewiseComparator();

		while (true) {
			rocksdb::Slice key;
			std::vector<size_t> positions;
			std::vector<size_t> to_remove;

			for (size_t pos = 0; pos < its.size(); ++pos) {
				auto &it = its[pos];
				if (!it->Valid()) {
					to_remove.push_back(pos);
					continue;
				}

				if (key.size() == 0) {
					key = it->key();
					positions.push_back(pos);
					continue;
				}

				int cval = cmp->Compare(it->key(), key);
				if (cval < 0) {
					key = it->key();
					positions.clear();
					positions.push_back(pos);
					continue;
				}

				if (cval > 0) {
					continue;
				}

				positions.push_back(pos);
			}

			if (key.size() == 0)
				break;

			rocksdb::WriteBatch batch;

			for (auto pos: positions) {
				auto &it = its[pos];

				batch.Merge(key, it->value());
			}

			err = odb.write(&batch);
			if (err) {
				ribosome::throw_error(err.code(), "key: %s, could not write batch of %ld elements: %s",
						key.ToString().c_str(), positions.size(), err.message().c_str());
			}

			for (auto pos: positions) {
				auto &it = its[pos];
				it->Next();
			}

			for (auto it = to_remove.rbegin(); it != to_remove.rend(); ++it) {
				its.erase(its.begin() + (*it));
			}
		}

		long max_seq = 0;
		for (auto &db: dbs) {
			auto &m = db->metadata();
			long seq = m.get_sequence();
			if (seq > max_seq) {
				max_seq = seq;
			}
		}

		odb.metadata().set_sequence(max_seq);
	}
private:
};

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Merge options");

	std::string output;
	std::vector<std::string> inputs;
	int thread_num;
	generic.add_options()
		("help", "This help message")
		("input", bpo::value<std::vector<std::string>>(&inputs)->required()->composing(), "Input rocksdb database")
		("output", bpo::value<std::string>(&output)->required(), "Output rocksdb database")
		("threads", bpo::value<int>(&thread_num)->default_value(8), "Number of merge threads")
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

	try {
		merger m;
		m.merge(output, inputs);
	} catch (const std::exception &e) {
		std::cerr << "Exception: " << e.what() << std::endl;
		return -1;
	}
}
