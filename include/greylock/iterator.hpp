#pragma once

#include "greylock/types.hpp"

#include <iterator>

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

	static index_iterator begin(DBT &db, const std::string &key) {
		return begin(db, key, 0);
	}

	static index_iterator begin(DBT &db, const std::string &key, size_t start) {
		return index_iterator(db, key, start);
	}

	static index_iterator end(DBT &db, const std::string &key) {
		return index_iterator(db, key, -1);
	}

	index_iterator(const index_iterator &src): m_db(src.m_db) {
		m_current = src.m_current;
		m_idx_current = m_current.ids.begin();
		m_idx_end = m_current.ids.end();

		m_key = src.m_key;
		m_current_shard_number = src.m_current_shard_number;
	}

	self_type operator++() {
		++m_idx_current;
		if (m_idx_current == m_idx_end) {
			load_next();
		}
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
		auto err = m_db.read(m_db.opts.document_prefix + std::to_string(*m_idx_current), &doc_data);
		if (err)
			return err;

		deserialize(*doc, doc_data.data(), doc_data.size());
		return greylock::error_info();
	}

	std::string to_string() const {
		std::ostringstream ss;
		ss << "key: " << m_key <<
			", shard_number: " << m_current_shard_number <<
			", ids_size: " << m_current.ids.size() <<
			", is_end: " << (m_idx_current == m_idx_end) <<
			", *current: " << *m_idx_current <<
			", *end: " << *m_idx_end;
		return ss.str();
	}

	bool operator==(const self_type& rhs) {
		if (m_key != rhs.m_key)
			return false;
		if (m_current_shard_number != rhs.m_current_shard_number)
			return false;

		if ((m_idx_current == m_idx_end) && (rhs.m_idx_current == rhs.m_idx_end))
			return true;

		if (*m_idx_current != *rhs.m_idx_current)
			return false;

		return true;
	}
	bool operator!=(const self_type& rhs) {
		return !operator==(rhs);
	}

private:
	DBT &m_db;
	std::string m_key;
	long m_current_shard_number = 0;

	index_iterator(DBT &db, const std::string &key, size_t shard_number): m_db(db), m_key(key), m_current_shard_number(shard_number) {
		load_next();
	}

	void set_current_shard_number(long sn) {
		m_current_shard_number = sn;
	}


	void load_next() {
		m_current.ids.clear();
		m_idx_current = m_current.ids.begin();
		m_idx_end = m_current.ids.end();

		if (m_current_shard_number < 0)
			return;

		std::string key = m_key + "." + std::to_string(m_current_shard_number);
		std::string data;
		auto err = m_db.read(key, &data);
		if (err) {
			set_current_shard_number(-1);
			return;
		}

		try {
			deserialize(m_current, data.data(), data.size());

			m_idx_current = m_current.ids.begin();
			m_idx_end = m_current.ids.end();
		} catch (...) {
			set_current_shard_number(-1);
			return;
		}

		set_current_shard_number(m_current_shard_number + 1);
	}
};
}} // namespace ioremap::greylock
