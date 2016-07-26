#pragma once

#include <msgpack.hpp>

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <sstream>
#include <vector>

namespace ioremap { namespace greylock {

template <typename T>
greylock::error_info deserialize(T &t, const char *data, size_t size) {
	msgpack::unpacked msg;
	try {
		msgpack::unpack(&msg, data, size);

		msg.get().convert(&t);
	} catch (const std::exception &e) {
		std::ostringstream ss;
		ss << msg.get();
		return greylock::create_error(-EINVAL, "could not unpack data, size: %ld, value: %s, error: %s",
				size, ss.str().c_str(), e.what());
	}

	return greylock::error_info();
}

template <typename T>
std::string serialize(const T &t) {
	std::stringstream buffer;
	msgpack::pack(buffer, t);
	buffer.seekg(0);
	return buffer.str();
}

typedef int pos_t;

struct token {
	std::string name;
	std::vector<pos_t> positions;

	token(const std::string &name): name(name) {}
	void insert_position(pos_t pos) {
		positions.push_back(pos);
	}

	std::string key;
};

struct attribute {
	std::string name;
	std::vector<token> tokens;

	attribute(const std::string &name): name(name) {}
	void insert(const std::string &tname, pos_t pos) {
		auto it = std::find_if(tokens.begin(), tokens.end(), [&](const token &t) {
					return t.name == tname;
				});
		if (it == tokens.end()) {
			token t(tname);
			t.insert_position(pos);
			tokens.emplace_back(t);
			return;
		}

		it->insert_position(pos);
	}
};

struct indexes {
	std::vector<attribute> attributes;
};

struct document {
	typedef uint64_t id_t;
	enum {
		serialize_version_5 = 5,
	};

	std::string mbox;
	struct timespec ts;

	std::string data;
	std::string id;

	id_t indexed_id = 0;

	indexes idx;

	template <typename Stream>
	void msgpack_pack(msgpack::packer<Stream> &o) const {
		o.pack_array(document::serialize_version_5);
		o.pack((int)document::serialize_version_5);
		o.pack(data);
		o.pack(id);
		o.pack(ts.tv_sec);
		o.pack(ts.tv_nsec);
	}

	void msgpack_unpack(msgpack::object o) {
		if (o.type != msgpack::type::ARRAY) {
			std::ostringstream ss;
			ss << "could not unpack document, object type is " << o.type <<
				", must be array (" << msgpack::type::ARRAY << ")";
			throw std::runtime_error(ss.str());
		}

		int version;

		msgpack::object *p = o.via.array.ptr;
		p[0].convert(&version);

		if (version != (int)o.via.array.size) {
			std::ostringstream ss;
			ss << "could not unpack document, invalid version: " << version << ", array size: " << o.via.array.size;
			throw std::runtime_error(ss.str());
		}

		switch (version) {
		case document::serialize_version_5:
			p[1].convert(&data);
			p[2].convert(&id);
			p[3].convert(&ts.tv_sec);
			p[4].convert(&ts.tv_nsec);
			break;
		default: {
			std::ostringstream ss;
			ss << "could not unpack document, invalid version " << version;
			throw std::runtime_error(ss.str());
		}
		}
	}
};

struct document_for_index {
	document::id_t indexed_id;
	MSGPACK_DEFINE(indexed_id);
};

struct disk_index {
	typedef document::id_t value_type;
	typedef document::id_t& reference;
	typedef document::id_t* pointer;

	std::set<document::id_t> ids;

	MSGPACK_DEFINE(ids);
};

struct options {
	size_t toknes_shard_size = 10000;
	long transaction_expiration = 60000; // 60 seconds
	long transaction_lock_timeout = 60000; // 60 seconds

	// if zero, sync metadata after each update
	// if negative, only sync at database close (server stop)
	// if positive, sync every @sync_metadata_timeout milliseconds
	int sync_metadata_timeout = 60000;

	int bits_per_key = 10; // bloom filter parameter

	long lru_cache_size = 100 * 1024 * 1024; // 100 MB of uncompressed data cache

	
	std::string document_prefix;
	std::string metadata_key;

	options():
		document_prefix("documents."),
		metadata_key("greylock.meta.key")
	{
	}
};

struct metadata {
	std::map<std::string, document::id_t> ids;
	document::id_t document_index;

	std::map<std::string, size_t> token_shards;

	bool dirty = false;

	void insert(const options &options, document &doc) {
		for (auto &attr: doc.idx.attributes) {
			for (auto &t: attr.tokens) {
				char ckey[doc.mbox.size() + attr.name.size() + t.name.size() + 3 + 17];

				size_t csize = snprintf(ckey, sizeof(ckey), "%s.%s.%s",
						doc.mbox.c_str(), attr.name.c_str(), t.name.c_str());
				size_t shard_number = 0;
				std::string prefix(ckey, csize);
				auto it = token_shards.find(prefix);
				if (it == token_shards.end()) {
					token_shards[prefix] = 0;
				} else {
					shard_number = ++it->second / options.toknes_shard_size;
				}

				csize = snprintf(ckey, sizeof(ckey), "%s.%s.%s.%ld",
						doc.mbox.c_str(), attr.name.c_str(), t.name.c_str(), shard_number);
				t.key.assign(ckey, csize);
				dirty = true;
			}
		}

		auto it = ids.find(doc.id);
		if (it == ids.end()) {
			doc.indexed_id = document_index++;
		} else {
			doc.indexed_id = it->second;
		}
	}

	enum {
		serialize_version_4 = 4,
	};
	template <typename Stream>
	void msgpack_pack(msgpack::packer<Stream> &o) const {
		o.pack_array(metadata::serialize_version_4);
		o.pack((int)metadata::serialize_version_4);
		o.pack(ids);
		o.pack(document_index);
		o.pack(token_shards);
	}

	void msgpack_unpack(msgpack::object o) {
		if (o.type != msgpack::type::ARRAY) {
			std::ostringstream ss;
			ss << "could not unpack metadata, object type is " << o.type <<
				", must be array (" << msgpack::type::ARRAY << ")";
			throw std::runtime_error(ss.str());
		}

		int version;

		msgpack::object *p = o.via.array.ptr;
		p[0].convert(&version);

		if (version != (int)o.via.array.size) {
			std::ostringstream ss;
			ss << "could not unpack metadata, invalid version: " << version << ", array size: " << o.via.array.size;
			throw std::runtime_error(ss.str());
		}

		switch (version) {
		case metadata::serialize_version_4:
			p[1].convert(&ids);
			p[2].convert(&document_index);
			p[3].convert(&token_shards);
			break;
		default: {
			std::ostringstream ss;
			ss << "could not unpack document, invalid version " << version;
			throw std::runtime_error(ss.str());
		}
		}
	}
};


}} // namespace ioremap::greylock
