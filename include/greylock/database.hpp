#pragma once

#include "greylock/error.hpp"
#include "greylock/id.hpp"
#include "greylock/utils.hpp"

#include <ribosome/expiration.hpp>

#pragma GCC diagnostic push 
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/transaction_db.h>
#pragma GCC diagnostic pop

#include <msgpack.hpp>

#include <iostream>
#include <memory>
#include <string>
#include <set>
#include <vector>

namespace ioremap { namespace greylock {

struct options {
	size_t tokens_shard_size = 3600 * 1 * 24;

	int max_threads = 8;

	int bits_per_key = 10; // bloom filter parameter

	long lru_cache_size = 100 * 1024 * 1024; // 100 MB of uncompressed data cache

	long sync_metadata_timeout = 60000; // 60 seconds

	// mininmum size of the token which will go into separate index,
	// if token size is smaller, it will be combined into 2 indexes
	// with the previous and next tokens.
	// This options greatly speeds up requests with small words (like [to be or not to be]),
	// but heavily increases index size.
	unsigned int ngram_index_size = 0;

	enum {
		default_column = 0,
		documents_column,
		document_ids_column,
		token_shards_column,
		indexes_column,
		meta_column,
		__column_size,
	};

	std::vector<std::string> column_names;
	std::string metadata_key;

	options(): metadata_key("greylock.meta.key") {
		column_names.resize(__column_size);
		column_names[default_column] = rocksdb::kDefaultColumnFamilyName;
		column_names[documents_column] = "documents";
		column_names[document_ids_column] = "document_ids";
		column_names[token_shards_column] = "token_shards";
		column_names[indexes_column] = "indexes";
		column_names[meta_column] = "meta";
	}

	std::string column_name(int cnum) const {
		if (cnum < 0 || cnum >= __column_size)
			return "";

		return column_names[cnum];
	}
};

class metadata {
public:
	metadata() : m_dirty(false), m_seq(0) {}

	bool dirty() const {
		return m_dirty;
	}
	void clear_dirty() {
		m_dirty = false;
	}

	long get_sequence() {
		m_dirty = true;
		return m_seq++;
	}

	void set_sequence(long seq) {
		m_dirty = true;
		m_seq = seq;
	}

	enum {
		serialize_version_2 = 2,
	};

	template <typename Stream>
	void msgpack_pack(msgpack::packer<Stream> &o) const {
		o.pack_array(metadata::serialize_version_2);
		o.pack((int)metadata::serialize_version_2);
		o.pack(m_seq.load());
	}

	void msgpack_unpack(msgpack::object o) {
		if (o.type != msgpack::type::ARRAY) {
			std::ostringstream ss;
			ss << "could not unpack metadata, object type is " << o.type <<
				", must be array (" << msgpack::type::ARRAY << ")";
			throw std::runtime_error(ss.str());
		}

		int version;
		long seq;

		msgpack::object *p = o.via.array.ptr;
		p[0].convert(&version);

		if (version != (int)o.via.array.size) {
			std::ostringstream ss;
			ss << "could not unpack document, invalid version: " << version << ", array size: " << o.via.array.size;
			throw std::runtime_error(ss.str());
		}

		switch (version) {
		case metadata::serialize_version_2:
			p[1].convert(&seq);
			m_seq.store(seq);
			break;
		default: {
			std::ostringstream ss;
			ss << "could not unpack metadata, invalid version " << version;
			throw std::runtime_error(ss.str());
		}
		}
	}

private:
	bool m_dirty;
	std::atomic_long m_seq;
};

struct document_for_index {
	id_t indexed_id;
	MSGPACK_DEFINE(indexed_id);

	bool operator<(const document_for_index &other) const {
		return indexed_id < other.indexed_id;
	}
};

namespace {
	static const uint32_t disk_cookie = 0x45589560;
}

struct disk_index {
	typedef document_for_index value_type;
	typedef document_for_index& reference;
	typedef document_for_index* pointer;

	std::vector<document_for_index> ids;

	template <typename Stream>
	void msgpack_pack(msgpack::packer<Stream> &o) const {
		o.pack_array(2);
		o.pack(disk_cookie);
		o.pack(ids);
	}

	void msgpack_unpack(msgpack::object o) {
		if (o.type != msgpack::type::ARRAY) {
			std::ostringstream ss;
			ss << "could not unpack disk index, object type is " << o.type <<
				", must be array (" << msgpack::type::ARRAY << ")";
			throw std::runtime_error(ss.str());
		}

		uint32_t cookie;

		msgpack::object *p = o.via.array.ptr;
		p[0].convert(&cookie);

		if (cookie != disk_cookie) {
			std::ostringstream ss;
			ss << "could not unpack disk index, cookie mismatch: " << std::hex << cookie <<
				", must be: " << std::hex << disk_cookie;
			throw std::runtime_error(ss.str());
		}

		p[1].convert(&ids);
	}
};

struct disk_token {
	std::vector<size_t> shards;
	MSGPACK_DEFINE(shards);

	disk_token() {}
	disk_token(const std::set<size_t> &s): shards(s.begin(), s.end()) {}
	disk_token(const std::vector<size_t> &s): shards(s) {}
};

class indexes_merge_operator : public rocksdb::MergeOperator {
public:
	virtual const char* Name() const override {
		return "indexes_merge_operator";
	}

	bool merge_indexes(const rocksdb::Slice& key, const rocksdb::Slice* old_value,
			const std::deque<std::string>& operand_list,
			std::string* new_value,
			rocksdb::Logger *logger) const {

		disk_index index;
		greylock::error_info err;
		std::set<document_for_index> unique_index;
		size_t ocount = 0;

		if (old_value) {
			err = deserialize(index, old_value->data(), old_value->size());
			if (err) {
				rocksdb::Error(logger, "merge: key: %s, index deserialize failed: %s [%d]",
						key.ToString().c_str(), err.message().c_str(), err.code());
				return false;
			}

			unique_index.insert(index.ids.begin(), index.ids.end());
			ocount = unique_index.size();
		}

		for (const auto& value : operand_list) {
			msgpack::unpacked msg;
			msgpack::unpack(&msg, value.data(), value.size());

			try {
				msgpack::object o = msg.get();

				if (o.type != msgpack::type::ARRAY) {
					document_for_index did;
					o.convert(&did);
					unique_index.emplace(did);
					continue;
				}

				disk_index idx;
				o.convert(&idx);

				unique_index.insert(idx.ids.begin(), idx.ids.end());
			} catch (const std::exception &e) {
				rocksdb::Error(logger, "merge: key: %s, document deserialize failed: %s",
						key.ToString().c_str(), e.what());
				return false;
			}
		}

		index.ids.clear();
		index.ids.insert(index.ids.end(), unique_index.begin(), unique_index.end());
		*new_value = serialize(index);

		if (new_value->size() > 1024 * 1024) {
			size_t osize = 0;
			if (old_value)
				osize = old_value->size();
			rocksdb::Info(logger, "index_merge: key: %s, size: %ld -> %ld, counts: %ld -> %ld",
					key.ToString().c_str(), osize, new_value->size(), ocount, index.ids.size());
		}

		return true;
	}

	virtual bool FullMerge(const rocksdb::Slice& key, const rocksdb::Slice* old_value,
			const std::deque<std::string>& operand_list,
			std::string* new_value,
			rocksdb::Logger *logger) const override {
		return merge_indexes(key, old_value, operand_list, new_value, logger);
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

class token_shards_merge_operator : public rocksdb::MergeOperator {
public:
	virtual const char* Name() const override {
		return "token_shards_merge_operator";
	}

	bool merge_token_shards(const rocksdb::Slice& key, const rocksdb::Slice* old_value,
			const std::deque<std::string>& operand_list,
			std::string* new_value,
			rocksdb::Logger *logger) const {

		disk_token dt;
		std::set<size_t> shards;
		greylock::error_info err;

		if (old_value) {
			err = deserialize(dt, old_value->data(), old_value->size());
			if (err) {
				rocksdb::Error(logger, "merge: key: %s, disk_token deserialize failed: %s [%d]",
						key.ToString().c_str(), err.message().c_str(), err.code());
				return false;
			}

			shards.insert(dt.shards.begin(), dt.shards.end());
		}

		for (const auto& value : operand_list) {
			disk_token s;
			err = deserialize(s, value.data(), value.size());
			if (err) {
				rocksdb::Error(logger, "merge: key: %s, disk_token operand deserialize failed: %s [%d]",
						key.ToString().c_str(), err.message().c_str(), err.code());
				return false;
			}

			shards.insert(s.shards.begin(), s.shards.end());
		}

		dt.shards = std::vector<size_t>(shards.begin(), shards.end());
		*new_value = serialize(dt);

		if (new_value->size() > 1024 * 1024) {
			size_t osize = 0;
			if (old_value) {
				osize = old_value->size();
			}

			rocksdb::Warn(logger, "shard_merge: key: %s, size: %ld -> %ld",
					key.ToString().c_str(), osize, new_value->size());
		}

		return true;
	}

	virtual bool FullMerge(const rocksdb::Slice& key, const rocksdb::Slice* old_value,
			const std::deque<std::string>& operand_list,
			std::string* new_value,
			rocksdb::Logger *logger) const override {
		return merge_token_shards(key, old_value, operand_list, new_value, logger);
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

class database {
public:
	~database() {
		if (!m_ro) {
			m_expiration_timer.stop();
			sync_metadata(NULL);
		}
	}

	const greylock::options &options() const {
		return m_opts;
	}
	greylock::metadata &metadata() {
		return m_meta;
	}

	rocksdb::ColumnFamilyHandle *cfhandle(int c) {
		return m_handles[c];
	}

	void compact() {
		if (m_db) {
			for (auto h: m_handles) {
				struct rocksdb::CompactRangeOptions opts;
				opts.change_level = true;
				opts.target_level = 0;
				m_db->CompactRange(opts, h, NULL, NULL);
			}
		}
	}

	greylock::error_info sync_metadata(rocksdb::WriteBatch *batch) {
		if (m_ro) {
			return greylock::create_error(-EROFS, "read-only database");
		}

		if (!m_db) {
			return greylock::create_error(-EINVAL, "database is not opened");
		}

		if (!m_meta.dirty())
			return greylock::error_info();

		std::string meta_serialized = serialize(m_meta);

		rocksdb::Status s;
		if (batch) {
			batch->Put(m_handles[options::meta_column], rocksdb::Slice(m_opts.metadata_key), rocksdb::Slice(meta_serialized));
		} else {
			s = m_db->Put(rocksdb::WriteOptions(), m_handles[options::meta_column],
					rocksdb::Slice(m_opts.metadata_key), rocksdb::Slice(meta_serialized));
		}

		if (!s.ok()) {
			return greylock::create_error(-s.code(), "could not write metadata key: %s, error: %s",
					m_opts.metadata_key.c_str(), s.ToString().c_str());
		}

		m_meta.clear_dirty();
		return greylock::error_info();
	}

	greylock::error_info open_read_only(const std::string &path) {
		return open(path, true, false);
	}
	greylock::error_info open_read_write(const std::string &path) {
		return open(path, false, false);
	}

	greylock::error_info open(const std::string &path, bool ro, bool bulk) {
		if (m_db) {
			return greylock::create_error(-EINVAL, "database is already opened");
		}

		rocksdb::Options dbo;
		dbo.max_open_files = 1000;
		//dbo.disableDataSync = true;
		dbo.IncreaseParallelism(m_opts.max_threads);

		dbo.max_bytes_for_level_base = 1024 * 1024 * 1024 * 100UL;
		//dbo.write_buffer_size = 1024 * 1024 * 1024UL;
		//dbo.max_write_buffer_number = 10;
		//dbo.min_write_buffer_number_to_merge = 4;

		dbo.compression = rocksdb::kZSTDNotFinalCompression;
		dbo.num_levels = 10;
#if 0
		dbo.compression_per_level =
			std::vector<rocksdb::CompressionType>({
					rocksdb::kZSTDNotFinalCompression,
					rocksdb::kZSTDNotFinalCompression,
					rocksdb::kZSTDNotFinalCompression,
					rocksdb::kZSTDNotFinalCompression,
					rocksdb::kZSTDNotFinalCompression,
				});
#endif
		dbo.compression_opts = rocksdb::CompressionOptions(-14, 5, 0, 0);

		dbo.create_if_missing = true;
		dbo.create_missing_column_families = true;

		if (!ro && bulk) {
			dbo.PrepareForBulkLoad();
		}

		dbo.statistics = rocksdb::CreateDBStatistics();
		dbo.stats_dump_period_sec = 60;

		rocksdb::BlockBasedTableOptions table_options;
		table_options.block_cache = rocksdb::NewLRUCache(m_opts.lru_cache_size);
		table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(m_opts.bits_per_key, true));
		dbo.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

		rocksdb::DB *db;
		rocksdb::Status s;

		rocksdb::ColumnFamilyOptions cfo(dbo);

		std::vector<rocksdb::ColumnFamilyDescriptor> column_families;

		for (size_t i = 0; i < options().column_names.size(); ++i) {
			auto cname = options().column_names[i];

			cfo.merge_operator.reset();

			if (i == greylock::options::token_shards_column) {
				cfo.merge_operator.reset(new token_shards_merge_operator);
			}
			if (i == greylock::options::indexes_column) {
				cfo.merge_operator.reset(new indexes_merge_operator);
			}

			column_families.push_back(rocksdb::ColumnFamilyDescriptor(cname, cfo));
		}

		if (ro) {
			s = rocksdb::DB::OpenForReadOnly(dbo, path, column_families, &m_handles, &db);
		} else {
			s = rocksdb::DB::Open(dbo, path, column_families, &m_handles, &db);
		}
		if (!s.ok()) {
			return greylock::create_error(-s.code(), "failed to open rocksdb database: '%s', read-only: %d, error: %s",
					path.c_str(), ro, s.ToString().c_str());
		}
		m_db.reset(db);
		m_ro = ro;

		std::string meta;
		s = m_db->Get(rocksdb::ReadOptions(), m_handles[options::meta_column], rocksdb::Slice(m_opts.metadata_key), &meta);
		if (!s.ok() && !s.IsNotFound()) {
			return greylock::create_error(-s.code(), "could not read key: %s, error: %s",
					m_opts.metadata_key.c_str(), s.ToString().c_str());
		}

		if (s.ok()) {
			auto err = deserialize(m_meta, meta.data(), meta.size());
			if (err)
				return greylock::create_error(err.code(), "metadata deserialization failed, key: %s, error: %s",
					m_opts.metadata_key.c_str(), err.message().c_str());
		}

		if (m_opts.sync_metadata_timeout > 0 && !ro) {
			sync_metadata_callback();
		}

		return greylock::error_info(); 
	}

	std::vector<size_t> get_shards(const std::string &key) {
		disk_token dt;
		if (!m_db) {
			return dt.shards;
		}

		std::string ser_shards;
		auto err = read(options::token_shards_column, key, &ser_shards);
		if (err)
			return dt.shards;

		err = deserialize(dt, ser_shards.data(), ser_shards.size());
		if (err)
			return dt.shards;

		return dt.shards;
	}

	rocksdb::Iterator *iterator(int column, const rocksdb::ReadOptions &ro) {
		return m_db->NewIterator(ro, m_handles[column]);
	}

	greylock::error_info read(int column, const std::string &key, std::string *ret) {
		if (!m_db) {
			return greylock::create_error(-EINVAL, "database is not opened");
		}

		auto s = m_db->Get(rocksdb::ReadOptions(), m_handles[column], rocksdb::Slice(key), ret);
		if (!s.ok()) {
			return greylock::create_error(-s.code(), "could not read key: %s, error: %s", key.c_str(), s.ToString().c_str());
		}
		return greylock::error_info();
	}

	greylock::error_info write(rocksdb::WriteBatch *batch) {
		if (!m_db) {
			return greylock::create_error(-EINVAL, "database is not opened");
		}

		if (m_ro) {
			return greylock::create_error(-EROFS, "read-only database");
		}

		auto wo = rocksdb::WriteOptions();

		auto s = m_db->Write(wo, batch);
		if (!s.ok()) {
			return greylock::create_error(-s.code(), "could not write batch: %s", s.ToString().c_str());
		}

		return greylock::error_info();
	}

	greylock::error_info write(int column, const std::string &key, const std::string &value) {
		if (!m_db) {
			return greylock::create_error(-EINVAL, "database is not opened");
		}

		if (m_ro) {
			return greylock::create_error(-EROFS, "read-only database");
		}

		auto wo = rocksdb::WriteOptions();

		auto s = m_db->Merge(wo, m_handles[column], rocksdb::Slice(key), rocksdb::Slice(value));
		if (!s.ok()) {
			return greylock::create_error(-s.code(), "could not write batch: %s", s.ToString().c_str());
		}

		return greylock::error_info();
	}

private:
	bool m_ro = false;
	std::vector<rocksdb::ColumnFamilyHandle*> m_handles;
	std::unique_ptr<rocksdb::DB> m_db;
	greylock::options m_opts;
	greylock::metadata m_meta;

	ribosome::expiration m_expiration_timer;

	void sync_metadata_callback() {
		sync_metadata(NULL);

		auto expires_at = std::chrono::system_clock::now() + std::chrono::milliseconds(m_opts.sync_metadata_timeout);
		m_expiration_timer.insert(expires_at, std::bind(&database::sync_metadata_callback, this));
	}
};

}} // namespace ioremap::greylock
