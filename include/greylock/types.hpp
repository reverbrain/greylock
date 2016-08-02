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

	std::string shard_key;
	std::set<size_t> shards;

	token(const std::string &name): name(name) {}
	void insert_position(pos_t pos) {
		positions.push_back(pos);
	}
	void insert_positions(const std::vector<pos_t> &pos) {
		positions.insert(positions.end(), pos.begin(), pos.end());
	}

	std::string key;
};

struct token_disk {
	std::vector<size_t> shards;
	MSGPACK_DEFINE(shards);

	token_disk() {}
	token_disk(const std::set<size_t> &s): shards(s.begin(), s.end()) {}
	token_disk(const std::vector<size_t> &s): shards(s) {}
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

	void insert(const std::string &tname, const std::vector<pos_t> &positions) {
		auto it = std::find_if(tokens.begin(), tokens.end(), [&](const token &t) {
					return t.name == tname;
				});
		if (it == tokens.end()) {
			token t(tname);
			t.insert_positions(positions);
			tokens.emplace_back(t);
			return;
		}

		it->insert_positions(positions);
	}
};

struct indexes {
	std::vector<attribute> attributes;
};

struct content {
	std::vector<std::string> content;
	std::vector<std::string> title;
	std::vector<std::string> links;
	std::vector<std::string> images;

	MSGPACK_DEFINE(content, title, links, images);
};

struct document {
	typedef uint64_t id_t;
	enum {
		serialize_version_7 = 7,
	};

	std::string mbox;
	struct timespec ts;

	std::string author;
	std::string data;
	std::string id;

	content ctx;

	id_t indexed_id = 0;

	indexes idx;

	template <typename Stream>
	void msgpack_pack(msgpack::packer<Stream> &o) const {
		o.pack_array(document::serialize_version_7);
		o.pack((int)document::serialize_version_7);
		o.pack(data);
		o.pack(author);
		o.pack(ctx);
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
		case document::serialize_version_7:
			p[1].convert(&data);
			p[2].convert(&author);
			p[3].convert(&ctx);
			p[4].convert(&id);
			p[5].convert(&ts.tv_sec);
			p[6].convert(&ts.tv_nsec);
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

	bool operator<(const document_for_index &other) const {
		return indexed_id < other.indexed_id;
	}
};

struct disk_index {
	typedef document_for_index value_type;
	typedef document_for_index& reference;
	typedef document_for_index* pointer;

	std::deque<document_for_index> ids;

	MSGPACK_DEFINE(ids);
};

struct options {
	size_t tokens_shard_size = 10000;
	long transaction_expiration = 60000; // 60 seconds
	long transaction_lock_timeout = 60000; // 60 seconds

	// if zero, sync metadata after each update
	// if negative, only sync at database close (server stop)
	// if positive, sync every @sync_metadata_timeout milliseconds
	int sync_metadata_timeout = 60000;

	int bits_per_key = 10; // bloom filter parameter

	long lru_cache_size = 100 * 1024 * 1024; // 100 MB of uncompressed data cache

	// mininmum size of the token which will go into separate index,
	// if token size is smaller, it will be combined into 2 indexes
	// with the previous and next tokens.
	// This options greatly speeds up requests with small words (like [to be or not to be]),
	// but heavily increases index size.
	unsigned int ngram_index_size = 0;
	
	std::string document_prefix;
	std::string token_shard_prefix;
	std::string index_prefix;
	std::string metadata_key;

	options():
		document_prefix("documents."),
		token_shard_prefix("token_shards."),
		index_prefix("index."),
		metadata_key("greylock.meta.key")
	{
	}
};

struct metadata {
	std::map<std::string, document::id_t> ids;
	document::id_t document_index;

	bool dirty = false;

	static std::string generate_index_base(const options &options,
			const std::string &mbox, const std::string &attr, const std::string &token) {
		char ckey[mbox.size() + attr.size() + token.size() + 3 + 17];
		size_t csize = snprintf(ckey, sizeof(ckey), "%s%s.%s.%s",
				options.index_prefix.c_str(),
				mbox.c_str(), attr.c_str(), token.c_str());

		return std::string(ckey, csize);
	}

	static std::string generate_index_key_shard_number(const std::string &base, size_t sn) {
		char ckey[base.size() + 17];
		size_t csize = snprintf(ckey, sizeof(ckey), "%s.%ld", base.c_str(), sn);

		return std::string(ckey, csize);
	}
	static std::string generate_index_key(const options &options, const std::string &base, const document::id_t &indexed_id) {
		size_t shard_number = indexed_id / options.tokens_shard_size;
		return generate_index_key_shard_number(base, shard_number);
	}
	static std::string generate_index_key(const options &options,
			const std::string &mbox, const std::string &attr, const std::string &token,
			const document::id_t &indexed_id) {
		std::string base = generate_index_base(options, mbox, attr, token);
		return generate_index_key(options, base, indexed_id);
	}

	static std::string generate_shard_key(const options &options,
			const std::string &mbox, const std::string &attr, const std::string &token) {
		char ckey[options.token_shard_prefix.size() + mbox.size() + attr.size() + token.size() + 5];
		size_t csize = snprintf(ckey, sizeof(ckey), "%s%s.%s.%s",
				options.token_shard_prefix.c_str(),
				mbox.c_str(), attr.c_str(), token.c_str());

		return std::string(ckey, csize);
	}

	void insert(const options &options, document &doc) {
		auto it = ids.find(doc.id);
		if (it == ids.end()) {
			doc.indexed_id = document_index++;
		} else {
			doc.indexed_id = it->second;
		}

		for (auto &attr: doc.idx.attributes) {
			for (auto &t: attr.tokens) {
				t.key = generate_index_key(options, doc.mbox, attr.name, t.name, doc.indexed_id);
				t.shard_key = generate_shard_key(options, doc.mbox, attr.name, t.name);

				size_t shard_number = doc.indexed_id / options.tokens_shard_size;
				t.shards.insert(shard_number);
			}
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
		//o.pack(token_shards);
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
			//p[3].convert(&token_shards);
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
