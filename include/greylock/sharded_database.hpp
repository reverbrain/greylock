#pragma once

#include "greylock/database.hpp"
#include "greylock/types.hpp"

#include <ribosome/error.hpp>

namespace ioremap { namespace greylock {

struct range {
	std::string begin, end;
	database db;

	bool operator<(const range &other) const {
		return begin < other.begin;
	}
	bool operator<(const std::string &other) const {
		return begin < other;
	}

	bool contains(const std::string &key) const {
		return key >= begin && key <= end;
	}

	std::string str() const {
		return "[" + begin + "-" + end + "]";
	}
};

struct range_cmp {
	bool operator()(const range &left, const range &right) const {
		return left.begin < right.begin;
	}
	bool operator()(const range &left, const std::string &right) const {
		return left.begin < right;
	}
	bool operator()(const std::string &left, const range &right) const {
		return left < right.begin;
	}
};


class sharded_database {
public:
	ribosome::error_info open(const std::vector<std::string> &indexes, const std::vector<std::string> &shards, bool ro) {
		for (auto &path: indexes) {
			range r;

			auto err = r.db.open(path, ro, false);
			if (err) {
				m_dbs.clear();
				return ribosome::create_error(err.code(), "could not open index: %s", err.message().c_str());
			}

			auto it = r.db.iterator(greylock::options::indexes_column, rocksdb::ReadOptions());
			if (!it) {
				m_dbs.clear();
				return ribosome::create_error(-EINVAL, "%s: could not create index iterator", path.c_str());
			}

			it->SeekToFirst();
			if (!it->Valid()) {
				m_dbs.clear();
				auto s = it->status();
				return ribosome::create_error(-s.code(), "%s: index iterator is not valid while pointing to the first element: %s",
						path.c_str(), s.ToString().c_str());
			}
			r.begin = it->key().ToString();

			it->SeekToLast();
			if (!it->Valid()) {
				m_dbs.clear();
				auto s = it->status();
				return ribosome::create_error(-s.code(), "%s: index iterator is not valid while pointing to the last element: %s",
						path.c_str(), s.ToString().c_str());
			}
			r.end = it->key().ToString();

			m_dbs.emplace_back(std::move(r));
		}

		for (auto &path: shards) {
			database db;
			auto err = db.open_read_write(path);
			if (err) {
				m_dbs.clear();
				return ribosome::create_error(err.code(), "could not open token shards: %s", err.message().c_str());
			}

			m_shards.emplace_back(std::move(db));
			m_shard_pointers.push_back(&m_shards.back());
		}

		if (m_shards.empty()) {
			for (auto &r: m_dbs) {
				m_shard_pointers.push_back(&r.db);
			}
		}

		m_ro = ro;
		return ribosome::error_info();
	}

	ribosome::error_info open_read_only(const std::vector<std::string> &indexes, const std::vector<std::string> &shards) {
		return open(indexes, shards, true);
	}
	ribosome::error_info open_read_write(const std::vector<std::string> &indexes, const std::vector<std::string> &shards) {
		return open(indexes, shards, false);
	}

	ribosome::error_info open(const std::vector<std::string> &docs, bool ro) {
		for (auto &path: docs) {
			range r;

			auto err = r.db.open(path, ro, false);
			if (err) {
				m_dbs.clear();
				return ribosome::create_error(err.code(), "could not open database: %s", err.message().c_str());
			}

			r.begin = "\0";
			r.end = "\xff";
			m_dbs.emplace_back(std::move(r));
		}

		return ribosome::error_info();
	}
	ribosome::error_info open_read_only(const std::vector<std::string> &docs) {
		return open(docs, true);
	}
	ribosome::error_info open_read_write(const std::vector<std::string> &docs) {
		return open(docs, false);
	}


	ribosome::error_info read(int column, const std::string &key, std::string *res) {
		if (column < 0 || column >= options::__column_size) {
			return ribosome::create_error(-EINVAL, "invalid column %d, must be in range [%d, %d)",
					column, options::default_column, options::__column_size);
		}

		if (m_dbs.empty()) {
			return ribosome::create_error(-EINVAL, "range vector is empty");
		}

		auto it = std::upper_bound(m_dbs.begin(), m_dbs.end(), key, range_cmp());
		if (it == m_dbs.begin()) {
			return ribosome::create_error(-EINVAL, "starting range %s is higher than key %s",
					m_dbs.front().str().c_str(), key.c_str());
		}
		if (it == m_dbs.end()) {
			it = m_dbs.begin() + m_dbs.size();
		}

		ribosome::error_info err = ribosome::create_error(-ENOENT, "could not read index key %s among %ld shards", key.c_str(), m_dbs.size());
		while (it != m_dbs.begin()) {
			it--;

			if (!it->contains(key))
				break;

			auto gerr = it->db.read(column, key, res);
			if (!gerr) {
				return ribosome::error_info();
			} else {
				err = ribosome::create_error(err.code(), "%s", err.message().c_str());
			}
		}

		return err;
	}

	ribosome::error_info read_index(const std::string &key, disk_index *res) {
		std::string idata;
		auto err = read(greylock::options::indexes_column, key, &idata);
		if (err) {
			return err;
		}

		auto gerr = deserialize(*res, idata.data(), idata.size());
		if (gerr) {
			return ribosome::create_error(err.code(), "%s", err.message().c_str());
		}

		return ribosome::error_info();
	}

	ribosome::error_info read_shards(const std::string &key, std::vector<size_t> *res) {
		ribosome::error_info ret_err = ribosome::create_error(-ENOENT, "there are no shards for key %s among %ld shard pointers",
				key.c_str(), m_shard_pointers.size());

		std::set<size_t> shards_set;
		for (auto ptr: m_shard_pointers) {
			auto shards = ptr->get_shards(key);

			if (shards.size()) {
				shards_set.insert(shards.begin(), shards.end());
				ret_err = ribosome::error_info();
			}
		}

		res->assign(shards_set.begin(), shards_set.end());
		return ret_err;
	}
	std::vector<size_t> get_shards(const std::string &key) {
		std::vector<size_t> ret;
		read_shards(key, &ret);
		return ret;
	}

	ribosome::error_info write(rocksdb::WriteBatch *batch) {
		if (m_dbs.empty()) {
			return ribosome::create_error(-EINVAL, "database is not opened");
		}

		if (m_ro) {
			return ribosome::create_error(-EROFS, "read-only database");
		}

		auto err = m_dbs.back().db.write(batch);
		if (err) {
			return ribosome::create_error(err.code(), "could not write batch: %s", err.message().c_str());
		}

		return ribosome::error_info();
	}

	ribosome::error_info write(int column, const std::string &key, const std::string &value) {
		if (m_dbs.empty()) {
			return ribosome::create_error(-EINVAL, "database is not opened");
		}

		if (m_ro) {
			return ribosome::create_error(-EROFS, "read-only database");
		}

		auto err = m_dbs.back().db.write(column, key, value);
		if (err) {
			return ribosome::create_error(err.code(), "could not write batch: %s", err.message().c_str());
		}

		return ribosome::error_info();
	}

	greylock::options options() {
		if (m_dbs.empty())
			return greylock::options();

		return m_dbs.back().db.options();
	}

	void compact() {
		for (auto &r: m_dbs) {
			r.db.compact();
		}

		for (auto p: m_shard_pointers) {
			p->compact();
		}
	}

	rocksdb::ColumnFamilyHandle *cfhandle(int c) {
		if (m_dbs.empty()) {
			ribosome::throw_error(-EINVAL, "empty set of databases");
		}

		return m_dbs.back().db.cfhandle(c);
	}

private:
	bool m_ro = true;
	std::vector<range> m_dbs;
	std::vector<database> m_shards;
	std::vector<database *> m_shard_pointers;
};

}} // namespace ioremap::greylock
