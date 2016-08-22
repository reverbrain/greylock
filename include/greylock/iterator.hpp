#pragma once

#include "greylock/types.hpp"

#include <iterator>

//#define STDOUT_DEBUG
#ifdef STDOUT_DEBUG
#define dprintf(fmt, a...) printf(fmt, ##a)
#else
#define dprintf(fmt, ...)
#endif

namespace ioremap { namespace greylock {

template <typename DBT>
class index_iterator {
private:
	disk_index m_current;
	typename decltype(m_current.ids)::iterator m_idx_current, m_idx_end;
public:
	typedef index_iterator self_type;
	typedef disk_index::value_type value_type;
	typedef typename decltype(m_current.ids)::iterator::reference reference;
	typedef typename decltype(m_current.ids)::iterator::pointer pointer;
	typedef std::forward_iterator_tag iterator_category;
	typedef std::ptrdiff_t difference_type;

	static index_iterator begin(DBT &db, const std::string &mbox, const std::string &attr, const std::string &token) {
		std::string index_base = document::generate_index_base(db.options(), mbox, attr, token);
		std::vector<size_t> shards(db.get_shards(document::generate_shard_key(db.options(), mbox, attr, token)));
		if (shards.size() == 0) {
			return end(db, index_base);
		}

		return index_iterator(db, index_base, shards);
	}
	static index_iterator begin(DBT &db, const std::string &mbox, const std::string &attr, const std::string &token,
			const std::vector<size_t> &shards) {
		std::string index_base = document::generate_index_base(db.options(), mbox, attr, token);
		if (shards.size() == 0) {
			return end(db, index_base);
		}

		return index_iterator(db, index_base, shards);
	}

	static index_iterator end(DBT &db, const std::string &base) {
		return index_iterator(db, base);
	}
	static index_iterator end(DBT &db, const std::string &mbox, const std::string &attr, const std::string &token) {
		std::string index_base = document::generate_index_base(db.options(), mbox, attr, token);
		return index_iterator(db, index_base);
	}

	index_iterator(const index_iterator &src): m_db(src.m_db) {
		m_current = src.m_current;
		if (src.m_idx_current == src.m_idx_end) {
			m_idx_current = m_idx_end = m_current.ids.end();
		} else {
			typename decltype(src.m_current.ids)::const_iterator sib = src.m_current.ids.begin();
			typename decltype(src.m_current.ids)::const_iterator sic = src.m_idx_current;

			auto diff = std::distance(sib, sic);
			dprintf("src: %s, diff: %ld\n", src.to_string().c_str(), diff);

			m_idx_current = std::next(m_current.ids.begin(), diff);
			m_idx_end = m_current.ids.end();
		}

		m_base = src.m_base;
		m_shards = src.m_shards;
		m_shards_idx = src.m_shards_idx;
	}

	self_type &operator++() {
		++m_idx_current;
		if (m_idx_current == m_idx_end) {
			load_next();
		}
		return *this;
	}

	self_type &rewind_to_index(const id_t &idx) {
		size_t rewind_shard = document::generate_shard_number(m_db.options(), idx);
		dprintf("rewind: %s, idx: %s, rewind_shard: %ld\n", to_string().c_str(), idx.to_string().c_str(), rewind_shard);

		auto rewind_shard_it = std::lower_bound(m_shards.begin(), m_shards.end(), rewind_shard);
		if (rewind_shard_it == m_shards.end()) {
			set_shard_index(-1);
			dprintf("could not increase iterator: %s\n", to_string().c_str());
			return *this;
		}

		int rewind_shard_idx = std::distance(m_shards.begin(), rewind_shard_it);
		if (rewind_shard_idx != m_shards_idx - 1) {
			set_shard_index(rewind_shard_idx);
			load_next();
		}

		if (m_shards_idx >= 0) {
			document_for_index did;
			did.indexed_id = idx;

			do {
				m_idx_current = std::lower_bound(m_idx_current, m_idx_end, did);
				if (m_idx_current == m_idx_end) {
					load_next();
					if (m_shards_idx < 0)
						break;
				}

			} while (m_idx_current->indexed_id < idx);
		}

		dprintf("increased iterator: %s\n", to_string().c_str());
		return *this;
	}

	reference operator*() {
		return *m_idx_current;
	}
	pointer operator->() {
		return &(*m_idx_current);
	}

	error_info document(document *doc) {
		std::string doc_data;
		auto err = m_db.read(m_db.options().document_prefix + m_idx_current->indexed_id.to_string(), &doc_data);
		if (err)
			return err;

		deserialize(*doc, doc_data.data(), doc_data.size());
		return greylock::error_info();
	}

	std::string to_string() const {
		auto dump_shards = [&]() -> std::string {
			std::ostringstream out;
			for (size_t i = 0; i < m_shards.size(); ++i) {
				out << m_shards[i];
				if (i != m_shards.size() - 1)
					out << " ";
			}
			return out.str();
		};
		std::ostringstream ss;
		ss << "base: " << m_base <<
			", next_shard_idx: " << m_shards_idx <<
			", shards: [" << dump_shards() << "] " <<
			", ids_size: " << m_current.ids.size() <<
			", current_is_end: " << (m_idx_current == m_idx_end) <<
			", indexed_id: " << ((m_idx_current == m_idx_end) ? "none" : m_idx_current->indexed_id.to_string());
		return ss.str();
	}

	bool operator==(const self_type& rhs) {
		if (m_base != rhs.m_base)
			return false;
		if (m_shards.size() != rhs.m_shards.size())
			return false;
		if (m_shards != rhs.m_shards)
			return false;
		if (m_shards_idx != rhs.m_shards_idx)
			return false;

		if ((m_idx_current == m_idx_end) && (rhs.m_idx_current == rhs.m_idx_end))
			return true;

		if (m_idx_current->indexed_id != rhs.m_idx_current->indexed_id)
			return false;

		return true;
	}
	bool operator!=(const self_type& rhs) {
		return !operator==(rhs);
	}

private:
	DBT &m_db;
	std::string m_base;
	std::vector<size_t> m_shards;
	int m_shards_idx = -1;

	index_iterator(DBT &db, const std::string &base): m_db(db), m_base(base) {
	}

	index_iterator(DBT &db, const std::string &base, const std::vector<size_t> shards): m_db(db), m_base(base), m_shards(shards) {
		set_shard_index(0);
		load_next();
	}

	void set_shard_index(int idx) {
		m_shards_idx = idx;
		if (idx < 0) {
			m_shards.clear();

			m_current.ids.clear();
			m_idx_current = m_current.ids.begin();
			m_idx_end = m_current.ids.end();
		}
	}

	void load_next() {
		do {
			load_next_one();
		} while (m_shards_idx >= 0 && m_current.ids.empty());
	}

	void load_next_one() {
		dprintf("loading: %s\n", to_string().c_str());
		m_current.ids.clear();
		m_idx_current = m_current.ids.begin();
		m_idx_end = m_current.ids.end();

		if (m_shards_idx < 0 || m_shards_idx >= (int)m_shards.size()) {
			set_shard_index(-1);
			return;
		}

		std::string key = document::generate_index_key_shard_number(m_base, m_shards[m_shards_idx]);
		std::string data;
		auto err = m_db.read(key, &data);
		if (err) {
			set_shard_index(-1);
			return;
		}

		try {
			deserialize(m_current, data.data(), data.size());

			m_idx_current = m_current.ids.begin();
			m_idx_end = m_current.ids.end();
		} catch (...) {
			set_shard_index(-1);
			return;
		}

		set_shard_index(m_shards_idx + 1);
		dprintf("loaded: %s\n", to_string().c_str());
	}
};
}} // namespace ioremap::greylock
