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

class checker {
public:
	checker(long print_interval) : m_print_interval(print_interval) {
	}

	void check(int column, const std::string &input) {
		std::unique_ptr<greylock::database> dbu(new greylock::database());
		auto err = dbu->open_read_only(input);
		if (err) {
			ribosome::throw_error(err.code(), "could not open input database: %s: %s",
					input.c_str(), err.message().c_str());
		}

		printf("Input database %s has been opened\n", input.c_str());

		rocksdb::ReadOptions ro;
		rocksdb::Iterator *it = dbu->iterator(column, ro);
		it->SeekToFirst();

		printf("Input database %s has been positioned\n", input.c_str());

		if (!it->Valid()) {
			auto s = it->status();
			ribosome::throw_error(-s.code(), "iterator from database %s is not valid: %s [%d]",
					input.c_str(), s.ToString().c_str(), s.code());
		}

		size_t prev_shard_number = 0;
		size_t shard_number = 0;
		size_t prev_documents = 0;
		size_t documents = 0;
		size_t shard_documents = 0;

		ribosome::timer tm, last_print;
		greylock::document doc;

		auto print_stats = [&] () -> char * {
			struct timespec ts;
			clock_gettime(CLOCK_REALTIME, &ts);

			static char tmp[1024];

			snprintf(tmp, sizeof(tmp),
				"%s: %ld seconds: documents: %ld, speed: %.2f [%.2f] docs/s, "
					"shard: %ld, docs: %ld, id: %s, doc: %s",
				print_time(ts.tv_sec, ts.tv_nsec),
				tm.elapsed() / 1000,
				documents,
				(float)documents * 1000.0 / (float)tm.elapsed(),
				(float)(documents - prev_documents) * 1000.0 / (float)last_print.elapsed(),
				prev_shard_number, shard_documents,
				doc.indexed_id.to_string().c_str(), doc.id.c_str());

			prev_documents = documents;
			last_print.restart();
			return tmp;
		};

		for (; it->Valid(); it->Next()) {
			auto sl = it->value();

			auto gerr = deserialize(doc, sl.data(), sl.size());
			if (gerr) {
				ribosome::throw_error(err.code(), "could not deserialize document, key: %s, size: %ld, error: %s [%d]",
						it->key().ToString().c_str(), sl.size(), gerr.message().c_str(), gerr.code());
			}

			shard_number = greylock::document::generate_shard_number(greylock::options(), doc.indexed_id);
			if (shard_number > 10000) {
				printf("shard_number: %ld [%lx], id: %s, doc: %s\n",
						shard_number, shard_number, doc.indexed_id.to_string().c_str(),
						doc.id.c_str());
			}

			if (shard_number < prev_shard_number) {
				printf("shard_number: %ld -> %ld, id: %s, doc: %s, error: shard number decreased\n",
						prev_shard_number, shard_number, doc.indexed_id.to_string().c_str(),
						doc.id.c_str());
			}

			if ((last_print.elapsed() > m_print_interval) || (prev_shard_number != shard_number)) {
				std::cout << print_stats() << std::endl;
			}

			if (prev_shard_number != shard_number) {
				shard_documents = 0;
			}

			documents++;
			shard_documents++;

			prev_shard_number = shard_number;
		}
		std::cout << print_stats() << std::endl;
	}

private:
	long m_print_interval;
};

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Merge options");

	std::string input;
	std::string column;
	long print_interval;
	generic.add_options()
		("help", "This help message")
		("column", bpo::value<std::string>(&column)->required(), "Column name to check")
		("input", bpo::value<std::string>(&input)->required(), "Input rocksdb database")
		("print-interval", bpo::value<long>(&print_interval)->default_value(1000), "Period to dump merge stats (in milliseconds)")
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
		checker c(print_interval);
		c.check(column_id, input);
	} catch (const std::exception &e) {
		std::cerr << "Exception: " << e.what() << std::endl;
		return -1;
	}
}
