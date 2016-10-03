#include "greylock/database.hpp"
#include "greylock/types.hpp"

#include <ribosome/error.hpp>
#include <ribosome/timer.hpp>

#include <boost/program_options.hpp>

#include <rocksdb/comparator.h>

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

class merger {
public:
	merger(long print_interval) : m_print_interval(print_interval) {
	}

	void merge(int column, const std::string &output, const std::vector<std::string> &inputs, bool compact) {
		ribosome::timer tm;

		greylock::database odb;
		auto err = odb.open_read_write(output);
		if (err) {
			ribosome::throw_error(err.code(), "could not open output database: %s: %s",
					output.c_str(), err.message().c_str());
		}

		printf("Output databse %s has been opened\n", output.c_str());

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

			printf("Input databse %s has been opened\n", path.c_str());

			auto it = dbu->iterator(column, ro);
			it->SeekToFirst();

			printf("Input databse %s has been positioned\n", path.c_str());

			if (!it->Valid()) {
				auto s = it->status();
				ribosome::throw_error(-s.code(), "iterator from database %s is not valid: %s [%d]",
						path.c_str(), s.ToString().c_str(), s.code());
			}

			its.emplace_back(it);
			dbs.emplace_back(std::move(dbu));
		}

		auto cmp = rocksdb::BytewiseComparator();

		long data_size = 0;
		long written_keys = 0;
		std::string first_key, last_key;
		long prev_written_keys = 0;
		long prev_data_size = 0;

		ribosome::timer merge_tm;

		auto print_stats = [&] () {
			struct timespec ts;
			clock_gettime(CLOCK_REALTIME, &ts);

			float kspeed = (float)written_keys * 1000.0 / (float)merge_tm.elapsed();
			float kspeed_moment = (float)(written_keys - prev_written_keys) * 1000.0 / (float)tm.elapsed();

			float dspeed = (float)data_size * 1000.0 / (float)merge_tm.elapsed() / (1024.0 * 1024.0);
			float dspeed_moment = (float)(data_size - prev_data_size) * 1000.0 / (float)tm.elapsed() / (1024.0 * 1024.0);

			printf("%s: column: %s [%d], written keys: %ld, speed: %.2f [%.2f] keys/s, "
				"written data size: %.2f MBs, speed: %.2f [%.2f] MB/s, "
				"first_key: %s, last_key: %s\n",
				print_time(ts.tv_sec, ts.tv_nsec),
				odb.options().column_names[column].c_str(), column,
				written_keys, kspeed, kspeed_moment,
				(float)data_size / (1024.0 * 1024.0), dspeed, dspeed_moment,
				first_key.c_str(), last_key.c_str());

			prev_written_keys = written_keys;
			prev_data_size = data_size;
			tm.restart();
		};

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

			long ds = 0;
			for (auto pos: positions) {
				auto &it = its[pos];

				batch.Merge(odb.cfhandle(column), key, it->value());
				ds += it->value().size();
			}

			err = odb.write(&batch);
			if (err) {
				ribosome::throw_error(err.code(), "key: %s, inputs: %s, could not write batch of %ld elements: %s",
						key.ToString().c_str(), greylock::dump_vector(positions).c_str(),
						positions.size(), err.message().c_str());
			}

			if (written_keys == 0) {
				first_key = key.ToString();
			}

			written_keys++;
			data_size += ds;
			last_key = key.ToString();

			for (auto pos: positions) {
				auto &it = its[pos];
				it->Next();
			}

			for (auto it = to_remove.rbegin(); it != to_remove.rend(); ++it) {
				its.erase(its.begin() + (*it));
			}

			if (tm.elapsed() > m_print_interval) {
				print_stats();
			}
		}

		print_stats();

		if (compact) {
			struct timespec ts;

			clock_gettime(CLOCK_REALTIME, &ts);
			printf("%s: starting compaction\n", print_time(ts.tv_sec, ts.tv_nsec));
			tm.restart();

			odb.compact();
			clock_gettime(CLOCK_REALTIME, &ts);
			printf("%s: compaction 1 took %.1f seconds\n", print_time(ts.tv_sec, ts.tv_nsec), tm.restart() / 1000.0);

			odb.compact();
			clock_gettime(CLOCK_REALTIME, &ts);
			printf("%s: compaction 2 took %.1f seconds\n", print_time(ts.tv_sec, ts.tv_nsec), tm.restart() / 1000.0);
		}
	}
private:
	long m_print_interval;
};

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Merge options");

	std::string output;
	std::vector<std::string> inputs;
	int thread_num;
	std::string column;
	long print_interval;
	generic.add_options()
		("help", "This help message")
		("column", bpo::value<std::string>(&column)->required(), "Column name to merge")
		("compact", "Whether to compact output database or not")
		("input", bpo::value<std::vector<std::string>>(&inputs)->required()->composing(), "Input rocksdb database")
		("output", bpo::value<std::string>(&output)->required(), "Output rocksdb database")
		("threads", bpo::value<int>(&thread_num)->default_value(8), "Number of merge threads")
		("print-interval", bpo::value<long>(&print_interval)->default_value(10000), "Period to dump merge stats (in milliseconds)")
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
		merger m(print_interval);
		m.merge(column_id, output, inputs, vm.count("compact") != 0);
	} catch (const std::exception &e) {
		std::cerr << "Exception: " << e.what() << std::endl;
		return -1;
	}
}
