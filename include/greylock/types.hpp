#pragma once

#include "greylock/database.hpp"
#include "greylock/json.hpp"
#include "greylock/id.hpp"

#include <msgpack.hpp>

#include <algorithm>
#include <atomic>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <sstream>
#include <vector>

#include <ribosome/split.hpp>

namespace ioremap { namespace greylock {

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
	std::vector<attribute> exact;
	std::vector<attribute> negation;

	std::vector<attribute> merge(const std::vector<attribute> &our, const std::vector<attribute> &other) const {
		std::map<std::string, attribute> attrs;

		auto merge_one = [&] (const std::vector<attribute> &v) {
			for (auto &a: v) {
				if (a.tokens.empty())
					return;

				auto it = attrs.find(a.name);
				if (it == attrs.end()) {
					attrs.insert(std::make_pair(a.name, a));
				} else {
					for (auto &t: a.tokens) {
						it->second.insert(t.name, t.positions);
					}
				}
			}
		};

		merge_one(our);
		merge_one(other);

		std::vector<attribute> ret;
		ret.reserve(attrs.size());
		for (auto &p: attrs) {
			ret.push_back(p.second);
		}
		return ret;
	}

	void merge_query(const indexes &other) {
		attributes = merge(attributes, other.attributes);
	}

	void merge_exact(const indexes &other) {
		exact = merge(exact, other.attributes);
	}

	void merge_negation(const indexes &other) {
		negation = merge(negation, other.attributes);
	}

	std::string to_string() const {
		std::ostringstream ss;

		auto dump_attributes = [] (const std::vector<attribute> &v) {
			return dump_vector<attribute>(v, [] (const attribute &a) -> std::string {
					std::ostringstream ss;
					ss << a.name;
					if (a.tokens.size()) {
						ss << ": tokens: ";
						for (size_t i = 0; i < a.tokens.size(); ++i) {
							auto &token = a.tokens[i];
							ss << token.name;
							if (i != a.tokens.size() - 1)
								ss << " ";
						}
					}
					return ss.str();
				});
		};

		ss << "negation: [" << dump_attributes(negation) << "] " <<
			"exact: [" << dump_attributes(exact) << "] " <<
			"query: [" << dump_attributes(attributes) << "] ";
		return ss.str();
	}

	static indexes get_indexes(const greylock::options &options, const rapidjson::Value &idxs) {
		indexes ireq;

		if (!idxs.IsObject())
			return ireq;

		ribosome::split spl;
		for (rapidjson::Value::ConstMemberIterator it = idxs.MemberBegin(), idxs_end = idxs.MemberEnd(); it != idxs_end; ++it) {
			const char *aname = it->name.GetString();
			const rapidjson::Value &avalue = it->value;

			if (!avalue.IsString())
				continue;

			greylock::attribute a(aname);

			std::vector<ribosome::lstring> indexes =
				spl.convert_split_words(avalue.GetString(), avalue.GetStringLength());
			for (size_t pos = 0; pos < indexes.size(); ++pos) {
				auto &idx = indexes[pos];
				if (idx.size() >= options.ngram_index_size) {
					a.insert(ribosome::lconvert::to_string(idx), pos);
				} else {
					if (pos > 0) {
						auto &prev = indexes[pos - 1];
						a.insert(ribosome::lconvert::to_string(prev + idx), pos);
					}

					if (pos < indexes.size() - 1) {
						auto &next = indexes[pos + 1];
						a.insert(ribosome::lconvert::to_string(idx + next), pos);
					}
				}
			}

			ireq.attributes.emplace_back(a);
		}

		return ireq;
	}

};

struct content {
	std::string content;
	std::string title;
	std::vector<std::string> links;
	std::vector<std::string> images;

	MSGPACK_DEFINE(content, title, links, images);
};

struct document {
	id_t indexed_id;

	enum {
		serialize_version_7 = 7,
	};

	std::string mbox;

	bool is_comment = false;

	std::string author;
	std::string id;

	content ctx;

	indexes idx;

	template <typename Stream>
	void msgpack_pack(msgpack::packer<Stream> &o) const {
		o.pack_array(document::serialize_version_7);
		o.pack((int)document::serialize_version_7);
		o.pack(is_comment);
		o.pack(author);
		o.pack(ctx);
		o.pack(id);
		o.pack(indexed_id);
		o.pack(0); // unused
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
			p[1].convert(&is_comment);
			p[2].convert(&author);
			p[3].convert(&ctx);
			p[4].convert(&id);
			p[5].convert(&indexed_id);
			//p[6].convert(); unused
			break;
		default: {
			std::ostringstream ss;
			ss << "could not unpack document, invalid version " << version;
			throw std::runtime_error(ss.str());
		}
		}
	}

	void assign_id(const char *cid, long seq, long tsec, long tnsec) {
		id.assign(cid);
		(void) tnsec;
		indexed_id.set_timestamp(tsec, seq);
	}

	void generate_token_keys(const options &options) {
		size_t shard_number = generate_shard_number(options, indexed_id);

		for (auto &attr: idx.attributes) {
			for (auto &t: attr.tokens) {
				std::string index_base = generate_index_base(options, mbox, attr.name, t.name);
				t.key = generate_index_key_shard_number(index_base, shard_number);
				t.shard_key = generate_shard_key(options, mbox, attr.name, t.name);

				t.shards.insert(shard_number);
			}
		}
	}

	static size_t generate_shard_number(const options &options, const id_t &indexed_id) {
		long tsec, tnsec;
		indexed_id.get_timestamp(&tsec, &tnsec);
		return tsec / options.tokens_shard_size;
	}

	static std::string generate_index_base(const options &options,
			const std::string &mbox, const std::string &attr, const std::string &token) {
		(void) options;
		char ckey[mbox.size() + attr.size() + token.size() + 3 + 17];
		size_t csize = snprintf(ckey, sizeof(ckey), "%s.%s.%s",
				mbox.c_str(), attr.c_str(), token.c_str());

		return std::string(ckey, csize);
	}

	static std::string generate_index_key_shard_number(const std::string &base, size_t sn) {
		char ckey[base.size() + 17];
		size_t csize = snprintf(ckey, sizeof(ckey), "%016lx.%s", sn, base.c_str());

		return std::string(ckey, csize);
	}
	static std::string generate_index_key(const options &options, const std::string &base, const id_t &indexed_id) {
		size_t shard_number = generate_shard_number(options, indexed_id);
		return generate_index_key_shard_number(base, shard_number);
	}
	static std::string generate_index_key(const options &options,
			const std::string &mbox, const std::string &attr, const std::string &token,
			const id_t &indexed_id) {
		std::string base = generate_index_base(options, mbox, attr, token);
		return generate_index_key(options, base, indexed_id);
	}

	static std::string generate_shard_key(const options &options,
			const std::string &mbox, const std::string &attr, const std::string &token) {
		(void) options;
		char ckey[mbox.size() + attr.size() + token.size() + 5];
		size_t csize = snprintf(ckey, sizeof(ckey), "%s.%s.%s",
				mbox.c_str(), attr.c_str(), token.c_str());

		return std::string(ckey, csize);
	}
};

}} // namespace ioremap::greylock
