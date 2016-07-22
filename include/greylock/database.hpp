#pragma once

#include "greylock/error.hpp"
#include "greylock/types.hpp"

#pragma GCC diagnostic push 
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/transaction_db.h>
#pragma GCC diagnostic pop

#include <memory>

namespace ioremap { namespace greylock {

struct read_only_database {
	std::unique_ptr<rocksdb::DB> db;
	options opts;

	greylock::error_info open(const std::string &path) {
		rocksdb::Options options;
		options.max_open_files = 1000;
		options.compression = rocksdb::kNoCompression;

		rocksdb::BlockBasedTableOptions table_options;
		table_options.block_cache = rocksdb::NewLRUCache(100 * 1048576); // 100MB of uncompresseed data cache
		table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
		options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

		rocksdb::DB *db;

		rocksdb::Status s = rocksdb::DB::OpenForReadOnly(options, path, &db);
		if (!s.ok()) {
			return greylock::create_error(-s.code(), "failed to open rocksdb database: '%s', error: %s",
					path.c_str(), s.ToString().c_str());
		}
		this->db.reset(db);

		return greylock::error_info(); 
	}

	greylock::error_info read(const std::string &key, std::string *ret) {
		auto s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(key), ret);
		if (!s.ok()) {
			return greylock::create_error(-s.code(), "could not read key: %s, error: %s", key.c_str(), s.ToString().c_str());
		}
		return greylock::error_info();
	}
};

struct database {
	std::unique_ptr<rocksdb::TransactionDB> db;
	options opts;
	metadata meta;

	greylock::error_info sync_metadata(rocksdb::Transaction* tx) {
		if (!meta.dirty)
			return greylock::error_info();

		rocksdb::Status s;
		if (tx) {
			s = tx->Put(rocksdb::Slice(opts.metadata_key), rocksdb::Slice(meta.serialize()));
		} else {
			s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(opts.metadata_key), rocksdb::Slice(meta.serialize()));
		}

		if (!s.ok()) {
			return greylock::create_error(-s.code(), "could not write metadata key: %s, error: %s",
					opts.metadata_key.c_str(), s.ToString().c_str());
		}

		meta.dirty = false;
		return greylock::error_info();
	}

	greylock::error_info open(const std::string &path) {
		rocksdb::TransactionDBOptions tdbo;

		rocksdb::Options dbo;
		dbo.max_open_files = 1000;

		dbo.compression = rocksdb::kLZ4Compression;

		dbo.create_if_missing = true;
		dbo.create_missing_column_families = true;

		//dbo.prefix_extractor.reset(NewFixedPrefixTransform(3));
		//dbo.memtable_prefix_bloom_bits = 100000000;
		//dbo.memtable_prefix_bloom_probes = 6;

		rocksdb::BlockBasedTableOptions table_options;
		table_options.block_cache = rocksdb::NewLRUCache(opts.lru_cache_size); // 100MB of uncompresseed data cache
		table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(opts.bits_per_key, true));
		dbo.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

		rocksdb::TransactionDB *db;

		rocksdb::Status s = rocksdb::TransactionDB::Open(dbo, tdbo, path, &db);
		if (!s.ok()) {
			return greylock::create_error(-s.code(), "failed to open rocksdb database: '%s', error: %s",
					path.c_str(), s.ToString().c_str());
		}
		this->db.reset(db);

		std::string meta;
		s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(opts.metadata_key), &meta);
		if (!s.ok() && !s.IsNotFound()) {
			return greylock::create_error(-s.code(), "could not read key: %s, error: %s",
					opts.metadata_key.c_str(), s.ToString().c_str());
		}

		try {
			if (s.ok()) {
				this->meta.deserialize(meta.data(), meta.size());
			}
		} catch (const std::exception &e) {
			return greylock::create_error(-EINVAL, "could not unpack metadata, key: %s, size: %ld, error: %s",
					opts.metadata_key.c_str(), meta.size(), e.what());
		}

		return greylock::error_info(); 
	}

	greylock::error_info read(const std::string &key, std::string *ret) {
		auto s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(key), ret);
		if (!s.ok()) {
			return greylock::create_error(-s.code(), "could not read key: %s, error: %s", key.c_str(), s.ToString().c_str());
		}
		return greylock::error_info();
	}
};

}} // namespace ioremap::greylock
