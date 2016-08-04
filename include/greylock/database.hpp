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

class disk_index_merge_operator : public rocksdb::MergeOperator {
public:
	virtual const char* Name() const override {
		return "disk_index_merge_operator";
	}

	bool merge_index(const rocksdb::Slice& key, const rocksdb::Slice* old_value,
			const std::deque<std::string>& operand_list,
			std::string* new_value,
			rocksdb::Logger *logger) const {

		struct greylock::disk_index index;
		greylock::error_info err;
		std::set<document_for_index> unique_index;

		if (old_value) {
			err = deserialize(index, old_value->data(), old_value->size());
			if (err) {
				rocksdb::Error(logger, "merge: key: %s, index deserialize failed: %s [%d]",
						key.ToString().c_str(), err.message().c_str(), err.code());
				return false;
			}

			unique_index.insert(index.ids.begin(), index.ids.end());
		}

		for (const auto& value : operand_list) {
			document_for_index did;
			err = deserialize(did, value.data(), value.size());
			if (err) {
				rocksdb::Error(logger, "merge: key: %s, document deserialize failed: %s [%d]",
						key.ToString().c_str(), err.message().c_str(), err.code());
				return false;
			}

			unique_index.emplace(did);
			//printf("full merge: key: %s, indexed_id: %ld\n", key.ToString().c_str(), doc.indexed_id);
		}

		index.ids.clear();
		index.ids.insert(index.ids.end(), unique_index.begin(), unique_index.end());
		*new_value = serialize(index);

		return true;
	}

	template <typename T>
	std::string dump_iterable(const T &iter) const {
		std::ostringstream ss;
		for (auto it = iter.begin(), end = iter.end(); it != end; ++it) {
			if (it != iter.begin())
				ss << " ";
			ss << *it;
		}
		return ss.str();
	}
	bool merge_token_shards(const rocksdb::Slice& key, const rocksdb::Slice* old_value,
			const std::deque<std::string>& operand_list,
			std::string* new_value,
			rocksdb::Logger *logger) const {

		struct greylock::token_disk td;
		std::set<size_t> shards;
		greylock::error_info err;

		if (old_value) {
			err = deserialize(td, old_value->data(), old_value->size());
			if (err) {
				rocksdb::Error(logger, "merge: key: %s, token_disk deserialize failed: %s [%d]",
						key.ToString().c_str(), err.message().c_str(), err.code());
				return false;
			}

			shards.insert(td.shards.begin(), td.shards.end());
		}

		for (const auto& value : operand_list) {
			token_disk s;
			err = deserialize(s, value.data(), value.size());
			if (err) {
				rocksdb::Error(logger, "merge: key: %s, token_disk operand deserialize failed: %s [%d]",
						key.ToString().c_str(), err.message().c_str(), err.code());
				return false;
			}

			shards.insert(s.shards.begin(), s.shards.end());
		}

		td.shards = std::vector<size_t>(shards.begin(), shards.end());
		*new_value = serialize(td);

		return true;
	}

	virtual bool FullMerge(const rocksdb::Slice& key, const rocksdb::Slice* old_value,
			const std::deque<std::string>& operand_list,
			std::string* new_value,
			rocksdb::Logger *logger) const override {
		if (key.starts_with(rocksdb::Slice("token_shards."))) {
			return merge_token_shards(key, old_value, operand_list, new_value, logger);
		}
		if (key.starts_with(rocksdb::Slice("index."))) {
			return merge_index(key, old_value, operand_list, new_value, logger);
		}

		return false;
	}

	virtual bool PartialMerge(const rocksdb::Slice& key,
			const rocksdb::Slice& left_operand, const rocksdb::Slice& right_operand,
			std::string* new_value,
			rocksdb::Logger* logger) const {
#if 0
		auto dump = [](const rocksdb::Slice &v) {
			std::ostringstream ss;

			msgpack::unpacked msg;
			msgpack::unpack(&msg, v.data(), v.size());

			ss << msg.get();
			return ss.str();
		};

		printf("partial merge: key: %s, left: %s, right: %s\n",
				key.ToString().c_str(), dump(left_operand).c_str(), dump(right_operand).c_str());
#endif
		(void) key;
		(void) left_operand;
		(void) right_operand;
		(void) new_value;
		(void) logger;

		return false;
	}
};

struct read_only_database {
	std::unique_ptr<rocksdb::DB> db;
	options opts;

	greylock::error_info open(const std::string &path) {
		rocksdb::Options options;
		options.max_open_files = 1000;
		options.merge_operator.reset(new disk_index_merge_operator);

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

	std::vector<size_t> get_shards(const std::string &key) {
		token_disk td;

		std::string ser_shards;
		auto err = read(key, &ser_shards);
		if (err) {
			//printf("could not read shards, key: %s, err: %s [%d]\n", key.c_str(), err.message().c_str(), err.code());
			return td.shards;
		}

		err = deserialize(td, ser_shards.data(), ser_shards.size());
		if (err)
			return td.shards;

		return td.shards;
	}
};


struct database {
	std::unique_ptr<rocksdb::DB> db;
	options opts;
	metadata meta;

	~database() {
	}

	void compact() {
		struct rocksdb::CompactRangeOptions opts;
		opts.change_level = true;
		opts.target_level = 0;
		db->CompactRange(opts, NULL, NULL);
	}

	greylock::error_info sync_metadata(rocksdb::WriteBatch *batch) {
		if (!meta.dirty)
			return greylock::error_info();

		std::string meta_serialized = serialize(meta);

		rocksdb::Status s;
		if (batch) {
			batch->Put(rocksdb::Slice(opts.metadata_key), rocksdb::Slice(meta_serialized));
		} else {
			s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(opts.metadata_key), rocksdb::Slice(meta_serialized));
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

	std::vector<size_t> get_shards(const std::string &key) {
		token_disk td;

		std::string ser_shards;
		auto err = read(key, &ser_shards);
		if (err)
			return td.shards;

		err = deserialize(td, ser_shards.data(), ser_shards.size());
		if (err)
			return td.shards;

		return td.shards;
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
