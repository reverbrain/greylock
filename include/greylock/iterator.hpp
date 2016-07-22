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
		auto ret = index_iterator(db, key);
		ret.set_current_shard_number(0);
		ret.load_next();

		return ret;
	}

	static index_iterator end(DBT &db, const std::string &key) {
		auto ret = index_iterator(db, key);
		ret.set_current_shard_number(-1);

		return ret;
	}

	self_type operator++() {
		if (m_idx_current == m_idx_end) {
			return *this;
		}

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

		doc->deserialize(doc_data.data(), doc_data.size());
		return greylock::error_info();
	}

	bool operator==(const self_type& rhs) {
		if (m_key != rhs.m_key)
			return false;
		if (m_current_shard_number != rhs.m_current_shard_number)
			return false;
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

	index_iterator(DBT &db, const std::string &key) : m_db(db), m_key(key) {
		m_idx_current = m_current.ids.begin();
		m_idx_end = m_current.ids.end();
	}

	void set_current_shard_number(long sn) {
		m_current_shard_number = sn;
	}


	void load_next() {
		m_current.ids.clear();
		m_idx_current = m_current.ids.begin();
		m_idx_end = m_current.ids.end();

		std::string key = m_key + "." + std::to_string(m_current_shard_number);
		std::string data;
		auto err = m_db.read(key, &data);
		if (err) {
			set_current_shard_number(-1);
			return;
		}

		try {
			m_current.deserialize(data.data(), data.size());

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
