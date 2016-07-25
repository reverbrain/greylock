#pragma once

#include "greylock/error.hpp"
#include "greylock/types.hpp"

#pragma GCC diagnostic push 
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/transaction_db.h>
#pragma GCC diagnostic pop

#include <memory>

#include <iostream>

namespace ioremap { namespace greylock {

struct read_only_database {
	std::unique_ptr<rocksdb::DB> db;
	options opts;

	greylock::error_info open(const std::string &path) {
		rocksdb::Options options;
		options.max_open_files = 1000;

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

class disk_index_merge_operator : public rocksdb::MergeOperator {
public:
	virtual const char* Name() const override {
		return "disk_index_merge_operator";
	}

	virtual bool FullMerge(const rocksdb::Slice& key, const rocksdb::Slice* old_value,
			const std::deque<std::string>& operand_list,
			std::string* new_value,
			rocksdb::Logger *logger) const override {

		struct greylock::disk_index index;
		greylock::error_info err;

		if (old_value) {
			err = deserialize(index, old_value->data(), old_value->size());
			if (err) {
				rocksdb::Error(logger, "merge: key: %s, index deserialize failed: %s [%d]",
						key.ToString().c_str(), err.message().c_str(), err.code());
				return false;
			}
		}

		for (const auto& value : operand_list) {
			document_for_index doc;
			err = deserialize(doc, value.data(), value.size());
			if (err) {
				rocksdb::Error(logger, "merge: key: %s, document deserialize failed: %s [%d]",
						key.ToString().c_str(), err.message().c_str(), err.code());
				return false;
			}

			index.ids.insert(doc.indexed_id);
		}

		*new_value = serialize(index);

		return true;
	}

	virtual bool PartialMerge(const rocksdb::Slice& key,
			const rocksdb::Slice& left_operand, const rocksdb::Slice& right_operand,
			std::string* new_value,
			rocksdb::Logger* logger) const {

		(void) key;
		(void) left_operand;
		(void) right_operand;
		(void) new_value;
		(void) logger;

		return false;
	}
};

struct database {
	std::unique_ptr<rocksdb::DB> db;
	options opts;
	metadata meta;

	~database() {
		struct rocksdb::CompactRangeOptions opts;
		opts.change_level = true;
		opts.target_level = 0;
		db->CompactRange(opts, NULL, NULL);
	}

	greylock::error_info sync_metadata(rocksdb::WriteBatch *batch) {
		if (!meta.dirty)
			return greylock::error_info();

		rocksdb::Status s;
		if (batch) {
			batch->Put(rocksdb::Slice(opts.metadata_key), rocksdb::Slice(serialize(meta)));
		} else {
			s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(opts.metadata_key), rocksdb::Slice(serialize(meta)));
		}

		if (!s.ok()) {
			return greylock::create_error(-s.code(), "could not write metadata key: %s, error: %s",
					opts.metadata_key.c_str(), s.ToString().c_str());
		}

		meta.dirty = false;
		return greylock::error_info();
	}

	greylock::error_info open(const std::string &path) {
		rocksdb::Options dbo;
		dbo.max_open_files = 1000;
		//dbo.disableDataSync = true;

		dbo.compression = rocksdb::kLZ4HCCompression;

		dbo.create_if_missing = true;
		dbo.create_missing_column_families = true;

		dbo.merge_operator.reset(new disk_index_merge_operator);

		//dbo.prefix_extractor.reset(NewFixedPrefixTransform(3));
		//dbo.memtable_prefix_bloom_bits = 100000000;
		//dbo.memtable_prefix_bloom_probes = 6;

		rocksdb::BlockBasedTableOptions table_options;
		table_options.block_cache = rocksdb::NewLRUCache(opts.lru_cache_size); // 100MB of uncompresseed data cache
		table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(opts.bits_per_key, true));
		dbo.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

		rocksdb::DB *db;

		rocksdb::Status s = rocksdb::DB::Open(dbo, path, &db);
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

		if (s.ok()) {
			auto err = deserialize(this->meta, meta.data(), meta.size());
			if (err)
				return greylock::create_error(err.code(), "metadata deserialization failed, key: %s, error: %s",
					opts.metadata_key.c_str(), err.message().c_str());
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
