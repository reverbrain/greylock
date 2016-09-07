#include "greylock/database.hpp"
#include "greylock/json.hpp"
#include "greylock/types.hpp"

#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/program_options.hpp>

#include <fstream>
#include <iostream>

#include <ribosome/error.hpp>
#include <ribosome/lstring.hpp>
#include <ribosome/html.hpp>
#include <ribosome/split.hpp>
#include <ribosome/timer.hpp>

#include <warp/language_model.hpp>
#include <warp/stem.hpp>

#include <signal.h>

using namespace ioremap;

static const std::string drop_characters = "`~-=!@#$%^&*()_+[]\\{}|';\":/.,?><\n\r\t ";
static ribosome::alphabet supported_alphabet;
static ribosome::numbers_alphabet numbers_alphabet;

static bool global_need_exit = false;

static void signal_handler(int signo)
{
	(void) signo;

	global_need_exit = true;
}

static inline const char *print_time(long tsec, long tnsec)
{
	char str[64];
	struct tm tm;

	static __thread char __dnet_print_time[128];

	localtime_r((time_t *)&tsec, &tm);
	strftime(str, sizeof(str), "%F %R:%S", &tm);

	snprintf(__dnet_print_time, sizeof(__dnet_print_time), "%s.%06llu", str, (long long unsigned) tnsec / 1000);
	return __dnet_print_time;
}


struct cache_control {
	long word_cache_size = 1000000;
	long index_cache_sync_interval = 60;
	long index_cached_documents = 1000;
};


struct post {
	bool is_comment = false;
	std::string id;
	std::string author;
	std::string title;
	std::string content;
	long issued;
	std::string parent;
	std::string url;

	post():
		is_comment(false),
		issued(time(NULL))
	{
	}

	post(const rapidjson::Value &val) : is_comment(false) {
		id.assign(greylock::get_string(val, "Guid", ""));
		author.assign(greylock::get_string(val, "Author", ""));
		title.assign(greylock::get_string(val, "Title", ""));
		content.assign(greylock::get_string(val, "Content", ""));
		parent.assign(greylock::get_string(val, "Parent", ""));
		url.assign(greylock::get_string(val, "Url", ""));
		issued = greylock::get_int64(val, "Issued", time(NULL));
	}
};

template <typename T>
struct use_control {
	T word;
	size_t position;

	use_control(const T &w, size_t pos): word(w), position(pos) {}
};

template <typename T>
struct lru_element {
	T token;
	int count;

	lru_element(lru_element &&other) {
		swap(*this, other);
	}
	lru_element &operator=(lru_element &&other) {
		swap(*this, other);
		return *this;
	}

	friend void swap(lru_element<T> &f, lru_element<T> &s) {
		std::swap(f.token, s.token);
		std::swap(f.count, s.count);
	}

	lru_element(const T &t): token(t), count(1) {}
	lru_element(): count(0) {}
};

template <typename T>
class lru {
public:
	lru(size_t limit): m_limit(limit) {}

	lru_element<T> insert(const T &t) {
		lru_element<T> ret;
		if (m_lru.size() == m_limit - 1) {
			swap(ret, m_lru.back());
			m_lru.pop_back();
		}

		m_lru.emplace_back(t);
		ret.count = m_lru.size() - 1;
		return ret;
	}

	size_t touch(size_t pos) {
		if (m_lru.size() <= pos) {
			ribosome::throw_error(-EINVAL, "invalid position: %ld, lru-size: %ld", pos, m_lru.size());
		}

		auto &e = m_lru[pos];
		if (e.count < 4)
			e.count++;

		if (pos > 0) {
			auto &prev = m_lru[pos - 1];
			if (e.count >= prev.count) {
				std::swap(e, prev);
				return pos - 1;
			}

			return pos;
		}

		return pos;
	}

private:
	size_t m_limit;
	std::deque<lru_element<T>> m_lru;
};

struct word_cache_stats {
	size_t hits = 0;
	size_t misses = 0;
	size_t removed = 0;
	size_t inserted = 0;
};

template <typename W, typename S>
class word_cache {
public:
	word_cache(int limit): m_lru(limit) {}

	void insert(const W &word, const S &stem) {
		lru_element<W> ret = m_lru.insert(word);
		if (ret.token.size()) {
			m_cstats.removed++;
			m_words.erase(ret.token);
		}

		m_words.insert(std::pair<W, use_control<S>>(word, use_control<S>(stem, ret.count)));
		m_cstats.inserted++;
	}

	S *get(const W &word) {
		auto it = m_words.find(word);
		if (it == m_words.end()) {
			m_cstats.misses++;
			return NULL;
		}

		m_cstats.hits++;

		size_t new_pos = m_lru.touch(it->second.position);
		it->second.position = new_pos;
		return &it->second.word;
	}

	const word_cache_stats &cstats() const {
		return m_cstats;
	}

private:
	lru<W> m_lru;
	std::unordered_map<W, use_control<S>> m_words;

	word_cache_stats m_cstats;
};

struct worker_stats {
	long documents = 0;
	long lines = 0;
	long empty_authors = 0;
	long skipped_documents = 0;
	long processed_text_size = 0;
	long skipped_text_size = 0;
	long written_data_size = 0;

	struct word_cache_stats cstats;
};

class index_cache {
public:
	typedef std::list<greylock::document_for_index> index_container_t;
	typedef std::list<size_t> shard_container_t;

	typedef std::map<std::string, index_container_t> token_indexes_t;
	typedef std::map<std::string, shard_container_t> token_shards_t;

	index_cache(greylock::database &db) : m_db(db) {}

	long cached_documents() const {
		return m_documents;
	}
	long cached_empty_authors() const {
		return m_empty_authors;
	}

	ribosome::error_info index(greylock::document &doc) {
		std::string mbox = doc.mbox;

		auto err = generate_indexes(doc);
		if (err) {
			return ribosome::create_error(err.code(),
					"id: %s, indexed_id: %s, could not generate index1 for mbox %s: %s",
					doc.id.c_str(), doc.indexed_id.to_string().c_str(),
					doc.mbox.c_str(), err.message().c_str());
		}

		if (mbox == "post") {
			if (doc.author.size()) {
				doc.mbox = "journal." + doc.author + "." + mbox;
				err = generate_indexes(doc);
				if (err) {
					return ribosome::create_error(err.code(),
							"id: %s, indexed_id: %s, could not generate index2 for mbox %s: %s",
							doc.id.c_str(), doc.indexed_id.to_string().c_str(),
							doc.mbox.c_str(), err.message().c_str());
				}
			} else {
				m_empty_authors++;
			}
		} else {
			size_t pos = doc.id.rfind('?');
			pos = doc.id.rfind('/', pos);
			if (pos != 0) {
				std::string journal = doc.id.substr(0, pos);
				doc.mbox = "journal." + journal + "." + mbox;
				err = generate_indexes(doc);
				if (err) {
					return ribosome::create_error(err.code(),
							"id: %s, indexed_id: %s, could not generate index3 for mbox %s: %s",
							doc.id.c_str(), doc.indexed_id.to_string().c_str(),
							doc.mbox.c_str(), err.message().c_str());
				}
			}

			if (doc.author.size()) {
				doc.mbox = "author." + doc.author + "." + mbox;
				err = generate_indexes(doc);
				if (err) {
					return ribosome::create_error(err.code(),
							"id: %s, indexed_id: %s, could not generate index4 for mbox %s: %s",
							doc.id.c_str(), doc.indexed_id.to_string().c_str(),
							doc.mbox.c_str(), err.message().c_str());
				}
			}
		}

		m_documents++;

		doc.mbox = mbox;
		return ribosome::error_info();
	}
#if 0
	ribosome::error_info write_indexes(worker_stats *wstats) {
		rocksdb::WriteBatch batch;

		for (auto &p: m_token_indexes) {
			std::set<greylock::document_for_index> u(p.second.begin(), p.second.end());
			greylock::disk_index di;
			di.ids.insert(di.ids.end(), u.begin(), u.end());
			std::string sdi = serialize(di);
			batch.Merge(rocksdb::Slice(p.first), rocksdb::Slice(sdi));

			wstats->written_data_size += sdi.size();
		}

		for (auto &p: m_token_shards) {
			std::set<size_t> u(p.second.begin(), p.second.end());
			greylock::disk_token dt(u);
			std::string sdt = serialize(dt);
			batch.Merge(rocksdb::Slice(p.first), rocksdb::Slice(sdt));

			wstats->written_data_size += sdt.size();
		}

		auto err = m_db.write(&batch);
		if (err) {
			return ribosome::create_error(err.code(), "could not write batch: %s", err.message().c_str());
		}

		wstats->documents += m_documents;
		wstats->empty_authors += m_empty_authors;

		clear();
		return ribosome::error_info();
	}
#endif
	void clear() {
		m_token_indexes.clear();
		m_token_shards.clear();

		m_documents = 0;
		m_empty_authors = 0;
	}

	void swap(index_cache &other) {
		std::swap(m_token_indexes, other.m_token_indexes);
		std::swap(m_token_shards, other.m_token_shards);

		long tmp;
		tmp = m_documents;
		m_documents = other.m_documents;
		other.m_documents = tmp;

		tmp = m_empty_authors;
		m_empty_authors = other.m_empty_authors;
		other.m_empty_authors = tmp;
	}

	token_indexes_t &token_indexes() {
		return m_token_indexes;
	}

	token_shards_t &token_shards() {
		return m_token_shards;
	}

private:
	greylock::database &m_db;

	token_indexes_t m_token_indexes;
	token_shards_t m_token_shards;

	long m_documents = 0;
	long m_empty_authors = 0;

	ribosome::error_info generate_indexes(greylock::document &doc) {
		doc.generate_token_keys(m_db.options());

		greylock::document_for_index did;
		did.indexed_id = doc.indexed_id;

		for (auto &attr: doc.idx.attributes) {
			for (auto &t: attr.tokens) {
				auto it = m_token_indexes.find(t.key);
				if (it == m_token_indexes.end()) {
					std::list<greylock::document_for_index> tmp;
					tmp.push_back(did);
					m_token_indexes[t.key] = std::move(tmp);
				} else {
					it->second.push_back(did);
				}

				auto sh = m_token_shards.find(t.shard_key);
				if (sh == m_token_shards.end()) {
					std::list<size_t> tmp(t.shards.begin(), t.shards.end());
					m_token_shards[t.shard_key] = std::move(tmp);
				} else {
					sh->second.insert(sh->second.end(), t.shards.begin(), t.shards.end());
				}
			}
		}

		return ribosome::error_info();
	}
};

class lj_worker {
public:
	lj_worker(greylock::database &db, warp::language_checker &lch, const cache_control &cc):
		m_db(db), m_lch(lch),
		m_word_cache(cc.word_cache_size),
		m_index_cache(db)
	{
	}

	~lj_worker() {
	}

	void swap(index_cache &dst) {
		std::lock_guard<std::mutex> guard(m_lock);
		m_index_cache.swap(dst);
		m_index_cache.clear();
	}
#if 0
	ribosome::error_info write_indexes() {
		std::lock_guard<std::mutex> guard(m_lock);
		return m_index_cache.write_indexes(&m_wstats);
	}
#endif
	long cached_documents() const {
		return m_index_cache.cached_documents();
	}

	ribosome::error_info process(const std::string &line) {
		size_t pos = line.find('\t');
		if (pos == std::string::npos) {
			return ribosome::create_error(-EINVAL, "invalid line, could not find delimiter: '%s'", line.c_str());
		}

		std::string id = line.substr(0, pos);

		post p;
		auto err = feed_data(line.data() + pos + 1, line.size() - pos - 1, &p);
		if (err) {
			std::cerr << "parser feed error: " << err.message() << std::endl;
			return err;
		}

		if (p.id.empty()) {
			p.id = id;
		}

		if (p.author.empty()) {
			static const std::string users_prefix = "http://users.livejournal.com/";
			size_t apos = id.find(users_prefix);
			if (apos == 0) {
				std::string user;
				char *start = (char *)id.c_str() + users_prefix.size();
				char *end = strchr(start, '/');
				if (end) {
					user.assign(start, end - start);
				} else {
					user.assign(start);
				}

				p.author = "http://" + user + ".livejournal.com";

			}
		}

		return convert_to_document(std::move(p));
	}

	const worker_stats &wstats() {
		m_wstats.cstats = m_word_cache.cstats();
		return m_wstats;
	}

	void clear_index_cache() {
		m_index_cache.clear();
	}

private:
	greylock::database &m_db;
	warp::language_checker &m_lch;

	word_cache<std::string, std::string> m_word_cache;

	ribosome::html_parser m_html;
	warp::stemmer m_stemmer;

	worker_stats m_wstats;

	std::mutex m_lock;

	ribosome::timer m_last_index_write;
	index_cache m_index_cache;

	ribosome::error_info feed_data(const char *data, size_t size, post *p) {
		rapidjson::Document doc;
		doc.Parse<0>(data);
		(void) size;

		if (doc.HasParseError()) {
			return ribosome::create_error(-EINVAL, "could not parse document: %s, error offset: %ld",
					doc.GetParseError(), doc.GetErrorOffset());
		}

		if (!doc.IsObject()) {
			return ribosome::create_error(-EINVAL, "search: document must be object");
		}

		const auto &obj = greylock::get_object(doc, "Comment");
		if (obj.IsObject()) {
			*p = std::move(post(obj));
			p->is_comment = true;
		} else {
			*p = std::move(post(doc));
		}

		return ribosome::error_info();
	}

	ribosome::error_info convert_to_document(post &&p) {
		greylock::document doc;

		auto cut_scheme = [&] (const std::string &url) -> std::string {
			static const std::vector<std::string> schemes({"http://", "https://"});
			for (const auto &s: schemes) {
				if (url.find(s) == 0) {
					return url.substr(s.size());
				}
			}

			return url;
		};

		auto replace_minus = [] (std::string &s) -> void {
			for (size_t i = 0; i < s.size(); ++i) {
				if (s[i] == '-') {
					s[i] = '_';
				}
			}
		};

		doc.assign_id("", m_db.metadata().get_sequence(), p.issued, 0);
		doc.id = cut_scheme(p.id);
		replace_minus(doc.id);

		if (p.author.size()) {
			doc.author = cut_scheme(p.author);
			replace_minus(doc.author);
		} else {
			if (!p.is_comment) {
				size_t pos = doc.id.find('/');
				doc.author = doc.id.substr(0, pos);
			}
		}

		if (p.is_comment)
			doc.mbox = "comment";
		else
			doc.mbox = "post";

		ribosome::split spl;
		bool numbers_only = true;

		auto split_content = [&] (const std::string &content, greylock::attribute *a) {
			std::set<std::string> stems;

			m_html.feed_text(content);

			doc.ctx.links.insert(doc.ctx.links.end(), m_html.links().begin(), m_html.links().end());
			doc.ctx.images.insert(doc.ctx.images.end(), m_html.images().begin(), m_html.images().end());

			auto get_stem = [&] (const std::string &word, const ribosome::lstring &idx) -> std::string {
				std::string *stem_ptr = m_word_cache.get(word);
				if (!stem_ptr) {
					std::string lang = m_lch.language(word, idx);
					std::string stem = m_stemmer.stem(word, lang, "");
					m_word_cache.insert(word, stem);

					stems.insert(stem);
					return stem;
				} else {
					stems.insert(*stem_ptr);
					return *stem_ptr;
				}
			};


			for (auto &t: m_html.tokens()) {
				ribosome::lstring lt = ribosome::lconvert::from_utf8(t);
				auto lower_request = ribosome::lconvert::to_lower(lt);

				auto all_words = spl.convert_split_words_allow_alphabet(lower_request, supported_alphabet);
				for (size_t pos = 0; pos < all_words.size(); ++pos) {
					auto &idx = all_words[pos];
					std::string word = ribosome::lconvert::to_string(idx);

					if (numbers_alphabet.ok(idx)) {
						stems.emplace(word);
						continue;
					}

					numbers_only = false;

					if (idx.size() >= m_db.options().ngram_index_size) {
						get_stem(word, idx);
					} else {
						if (pos > 0) {
							auto &prev = all_words[pos - 1];
							auto prev_word = ribosome::lconvert::to_string(prev);
							auto st = get_stem(prev_word, prev);
							stems.emplace(st + word);
						}

						if (pos < all_words.size() - 1) {
							auto &next = all_words[pos + 1];
							auto next_word = ribosome::lconvert::to_string(next);
							auto st = get_stem(next_word, next);
							stems.emplace(word + st);
						}
					}
				}
			}

			if (stems.size()) {
				m_wstats.processed_text_size += content.size();

				for (auto &s: stems) {
					a->tokens.emplace_back(s);
				}
			} else {
				m_wstats.skipped_text_size += content.size();
			}
		};

		greylock::attribute ft("fixed_title");
		greylock::attribute fc("fixed_content");
		greylock::attribute urls("urls");

		split_content(p.content, &fc);
		split_content(p.title, &ft);

		m_wstats.lines++;

		if ((ft.tokens.empty() && fc.tokens.empty()) || numbers_only) {
			m_wstats.skipped_documents++;
			return ribosome::error_info();
		}

		for (auto &url: doc.ctx.links) {
			auto all_urls = spl.convert_split_words(url.c_str(), url.size());
			for (auto &u: all_urls) {
				urls.insert(ribosome::lconvert::to_string(u), 0);
			}
		}

		doc.ctx.content = std::move(p.content);
		doc.ctx.title = std::move(p.title);

		doc.idx.attributes.emplace_back(ft);
		doc.idx.attributes.emplace_back(fc);
		doc.idx.attributes.emplace_back(urls);

		ribosome::error_info err;
		std::lock_guard<std::mutex> guard(m_lock);

		err = m_index_cache.index(doc);
		if (err) {
			return err;
		}

		err = write_document(doc);
		if (err) {
			return err;
		}

		return ribosome::error_info();
	}

	ribosome::error_info write_document(const greylock::document &doc) {
		rocksdb::WriteBatch batch;

		std::string doc_serialized = serialize(doc);
		std::string dkey = m_db.options().document_prefix + doc.indexed_id.to_string();
		batch.Put(rocksdb::Slice(dkey), rocksdb::Slice(doc_serialized));

		std::string doc_indexed_id_serialized = serialize(doc.indexed_id);
		std::string dids_key = m_db.options().document_id_prefix + doc.id;
		batch.Put(rocksdb::Slice(dids_key), rocksdb::Slice(doc_indexed_id_serialized));

		m_wstats.written_data_size += doc_serialized.size() + doc_indexed_id_serialized.size();

		auto err = m_db.write(&batch);
		if (err) {
			return ribosome::create_error(err.code(), "could not write batch: %s", err.message().c_str());
		}

		return ribosome::error_info();
	}
};

struct parse_stats {
	long documents = 0;
	long lines = 0;
	long skipped_documents = 0;
	long skipped_text_size = 0;
	long processed_text_size = 0;
	long written_data_size = 0;

	word_cache_stats wcstats;

	void merge(const worker_stats &ws) {
		documents += ws.documents;
		lines += ws.lines;
		skipped_documents += ws.skipped_documents;
		skipped_text_size += ws.skipped_text_size;
		processed_text_size += ws.processed_text_size;
		written_data_size += ws.written_data_size;

		wcstats.hits += ws.cstats.hits;
		wcstats.misses += ws.cstats.misses;
		wcstats.removed += ws.cstats.removed;
		wcstats.inserted += ws.cstats.inserted;
	}

	friend parse_stats operator-(const parse_stats &l, const parse_stats &r) {
		parse_stats ps = l;
		ps -= r;
		return ps;
	}

	parse_stats &operator-=(const parse_stats &other) {
		documents -= other.documents;
		lines -= other.lines;
		skipped_documents -= other.skipped_documents;
		skipped_text_size -= other.skipped_text_size;
		processed_text_size -= other.processed_text_size;
		written_data_size -= other.written_data_size;

		wcstats.hits -= other.wcstats.hits;
		wcstats.misses -= other.wcstats.misses;
		wcstats.removed -= other.wcstats.removed;
		wcstats.inserted -= other.wcstats.inserted;

		return *this;
	}

	std::string print_stats() {
		char buf[1024];
		size_t sz = snprintf(buf, sizeof(buf),
			"skipped_documents: %ld, skipped_text: %.2f MBs, processed_text: %.2f MBs, "
				"written_data: %.2f MBs, cache: hits: %.1f%%",
			skipped_documents,
			skipped_text_size / 1024 / 1024.0, processed_text_size / 1024 / 1024.0,
			written_data_size / 1024 / 1024.0,
			(float)wcstats.hits / (float)(wcstats.hits + wcstats.misses) * 100.0);

		return std::string(buf, sz);
	}
};

class lj_parser {
public:
	lj_parser(int n, const struct cache_control &cc) : m_cc(cc) {
		m_pool.reserve(n);
		m_workers.reserve(n);

		for (int i = 0; i < n; ++i) {
			m_pool.emplace_back(std::bind(&lj_parser::callback, this, i));
			m_workers.emplace_back(std::unique_ptr<lj_worker>(new lj_worker(m_db, m_lch, cc)));
		}

		m_sync_thread = std::thread(std::bind(&lj_parser::sync_callback, this));
	}

	~lj_parser() {
		m_need_exit = true;
		m_pool_wait.notify_all();
		for (auto &t: m_pool) {
			t.join();
		}

		sync_wait();
		m_sync_thread.join();

		write_indexes();
	}

	void compact() {
		m_db.compact();
	}

	ribosome::error_info open(const std::string &dbpath) {
		auto err = m_db.open_read_write(dbpath);
		if (err)
			return ribosome::create_error(err.code(), "%s", err.message().c_str());

		return ribosome::error_info();
	}

	ribosome::error_info load_langdetect_stats(const std::string &path) {
		return m_lch.load_langdetect_stats(path.c_str());
	}

	ribosome::error_info load_language_model(const warp::language_model &lm) {
		return m_lch.load_language_model(lm);
	}

	void queue_work(std::string &&str) {
		std::unique_lock<std::mutex> guard(m_lock);
		m_lines.emplace_back(std::move(str));

		if (m_lines.size() > m_pool.size() * 2) {
			m_parser_wait.wait(guard, [&] {return m_lines.size() < m_pool.size();});
		}

		guard.unlock();
		m_pool_wait.notify_one();
	}

	parse_stats pstats() const {
		parse_stats ps;
		ps.merge(m_icache_wstats);

		for (auto &w: m_workers) {
			auto &wstat = w->wstats();
			ps.merge(wstat);
		}

		return ps;
	}


	template <typename T>
	struct iter {
		typename T::iterator it, end;

		iter(T &i):
			it(i.begin()),
			end(i.end())
		{
		}
	};

	template <typename T, typename C>
	void iterate(std::vector<iter<T>> &its, rocksdb::WriteBatch *batch, std::function<std::string (const C&)> sfunc) {
		while (true) {
			std::string name;

			std::vector<size_t> erase;
			std::vector<size_t> positions;
			for (size_t i = 0; i < its.size(); ++i) {
				auto &it = its[i];
				if (it.it == it.end) {
					erase.push_back(i);
					continue;
				}

				if (name.empty()) {
					name = it.it->first;
					positions.push_back(i);
					continue;
				}

				if (it.it->first > name) {
					continue;
				}

				if (it.it->first < name) {
					positions.clear();
					name = it.it->first;
					positions.push_back(i);
					continue;
				}

				positions.push_back(i);
			}

			if (positions.empty())
				break;

			C c;
			for (auto pos: positions) {
				auto &it = its[pos];
				c.insert(it.it->second.begin(), it.it->second.end());
				++it.it;
			}

			for (long i = erase.size() - 1; i >= 0; --i) {
				its.erase(its.begin() + erase[i]);
			}


			std::string sdata = sfunc(c);
			batch->Merge(rocksdb::Slice(name), rocksdb::Slice(sdata));
		}
	}

	greylock::error_info write_indexes() {
		greylock::error_info err;

		ribosome::timer total_tm, tmp_tm;
		struct timespec start;
		clock_gettime(CLOCK_REALTIME, &start);


		std::list<std::unique_ptr<index_cache>> indexes;
		long documents = 0;
		long empty_authors = 0;

		// protect against write_indexes() running in parallel from sync thread and called directly
		std::unique_lock<std::mutex> guard(m_sync_lock);
		for (auto &w: m_workers) {
			std::unique_ptr<index_cache> idx(new index_cache(m_db));
			w->swap(*idx);

			documents += idx->cached_documents();
			empty_authors += idx->cached_empty_authors();

			indexes.emplace_back(std::move(idx));
		}
		guard.unlock();

		long swap_time = tmp_tm.restart();

		std::vector<iter<index_cache::token_indexes_t>> idx_iter;
		std::vector<iter<index_cache::token_shards_t>> shard_iter;
		for (auto &idx: indexes) {
			idx_iter.emplace_back(idx->token_indexes());
			shard_iter.emplace_back(idx->token_shards());
		}

		long iter_create_time = tmp_tm.restart();
		long indexes_num = 0;
		long shards_num = 0;
		long indexes_data_size = 0;
		long shards_data_size = 0;

		rocksdb::WriteBatch batch;
		iterate<index_cache::token_indexes_t, std::set<greylock::document_for_index>>(idx_iter, &batch,
				[&] (const std::set<greylock::document_for_index> &c) {
					indexes_num += c.size();
					greylock::disk_index di;
					di.ids.insert(di.ids.end(), c.begin(), c.end());
					std::string ret = serialize(di);
					indexes_data_size += ret.size();
					return ret;
				});

		idx_iter.clear();
		idx_iter.shrink_to_fit();

		long token_indexes_iter_time = tmp_tm.restart();

		iterate<index_cache::token_shards_t, std::set<size_t>>(shard_iter, &batch,
				[&] (const std::set<size_t> &c) {
					shards_num += c.size();
					greylock::disk_token dt(c);
					std::string ret = serialize(dt);
					shards_data_size += ret.size();
					return ret;
				});
		shard_iter.clear();
		shard_iter.shrink_to_fit();

		long token_shards_iter_time = tmp_tm.restart();

		indexes.clear();

		long indexes_clear_time = tmp_tm.restart();

		if (indexes_num || shards_num) {
			err = m_db.write(&batch);
			if (!err) {
				// protect against write_indexes() running in parallel from sync thread and called directly
				guard.lock();
				m_icache_wstats.documents += documents;
				m_icache_wstats.empty_authors += empty_authors;
				m_icache_wstats.written_data_size += indexes_data_size + shards_data_size;
				guard.unlock();
			}
		}

		long db_write_time = tmp_tm.restart();

		if (indexes_num || shards_num) {
			printf("write_indexes: started: %s, workers: %ld, documents: %ld/%ld, "
				"swap: %ld, iter_create: %ld, "
				"indexes: %ld, size: %ld, iteration: %ld, "
				"shards: %ld, size: %ld, iteration: %ld, "
				"indexes_clear: %ld, db_write: %ld, total_time: %ld\n",
				print_time(start.tv_sec, start.tv_nsec), m_workers.size(), documents, m_icache_wstats.documents,
				swap_time, iter_create_time,
				indexes_num, indexes_data_size, token_indexes_iter_time,
				shards_num, shards_data_size, token_shards_iter_time,
				indexes_clear_time, db_write_time, total_tm.elapsed());
		}

		return err;
	}

	void sync_wait() {
		std::unique_lock<std::mutex> guard(m_sync_lock);
		if (m_sync_state == sync_exited)
			return;

		m_sync_state = sync_scheduled;
		m_sync_wait.notify_one();
		m_sync_wait.wait(guard, [&] () { return m_sync_state == sync_completed || m_sync_state == sync_exited; });
	}


private:
	greylock::database m_db;
	warp::language_checker m_lch;
	cache_control m_cc;

	std::vector<std::unique_ptr<lj_worker>> m_workers;

	bool m_need_exit = false;
	std::mutex m_lock;
	std::list<std::string> m_lines;
	std::condition_variable m_pool_wait, m_parser_wait;
	std::vector<std::thread> m_pool;

	worker_stats m_icache_wstats;

	std::mutex m_sync_lock;
	std::condition_variable m_sync_wait;
	std::thread m_sync_thread;
	enum {
		sync_not_started = 0,
		sync_scheduled,
		sync_in_progress,
		sync_completed,
		sync_exited,
	} m_sync_state = sync_not_started;


	void sync_callback() {
		while (!m_need_exit) {
			std::unique_lock<std::mutex> guard(m_sync_lock);
			auto status = m_sync_wait.wait_for(guard, std::chrono::seconds(m_cc.index_cache_sync_interval));

			if ((m_sync_state != sync_scheduled) && (status == std::cv_status::no_timeout))
				continue;

			m_sync_state = sync_in_progress;

			do {
				guard.unlock();
				write_indexes();
				guard.lock();
			// someone had scheduled sync, while we were writing indexes, run iteration again
			} while (m_sync_state == sync_scheduled);

			m_sync_state = sync_completed;
			m_sync_wait.notify_all();
		}

		std::unique_lock<std::mutex> guard(m_sync_lock);
		m_sync_state = sync_exited;
		m_sync_wait.notify_all();
	}

	void callback(int idx) {
		auto &worker = m_workers[idx];

		while (!m_need_exit) {
			std::unique_lock<std::mutex> guard(m_lock);
			m_pool_wait.wait_for(guard, std::chrono::milliseconds(100), [&] {return !m_lines.empty();});

			while (!m_lines.empty()) {
				std::string line = std::move(m_lines.front());
				m_lines.pop_front();
				guard.unlock();

				m_parser_wait.notify_one();

				auto err = worker->process(line);
				if (err) {
					std::cerr << "could not process line: " << err.message() << std::endl;
					exit(err.code());
				}

				if (worker->cached_documents() > m_cc.index_cached_documents) {
					sync_wait();
				}

				guard.lock();
			}
		}
	}
};


int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Parser options");

	std::vector<std::string> als;
	std::vector<std::string> lang_models;
	std::string input, output, lang_path;
	size_t rewind = 0;
	int thread_num;
	cache_control cc;
	long print_interval;
	generic.add_options()
		("help", "This help message")
		("input", bpo::value<std::string>(&input)->required(), "Livejournal dump file packed with bzip2")
		("output", bpo::value<std::string>(&output)->required(), "Output rocksdb database")
		("rewind", bpo::value<size_t>(&rewind), "Rewind input to this line number")
		("threads", bpo::value<int>(&thread_num)->default_value(8), "Number of parser threads")
		("alphabet", bpo::value<std::vector<std::string>>(&als)->composing(), "Allowed alphabet")
		("compact", "Compact database on exit")
		("print-interval", bpo::value<long>(&print_interval)->default_value(1000), "Statistics print interval in milliseconds")
		("word-cache", bpo::value<long>(&cc.word_cache_size)->default_value(100000), "Word->stem per thread cLRU cache size")
		("index-cache-interval", bpo::value<long>(&cc.index_cache_sync_interval)->default_value(60),
			"Per-thread index cache flush interval in seconds")
		("index-cached-documents", bpo::value<long>(&cc.index_cached_documents)->default_value(1000),
			"Maximum number of documents parsed and cached per thread")
		("lang_stats", bpo::value<std::string>(&lang_path)->required(), "Language stats file")
		("lang_model", bpo::value<std::vector<std::string>>(&lang_models)->composing(),
			"Language models, format: language:model_path")
		;

	bpo::options_description cmdline_options;
	cmdline_options.add(generic);

	bpo::variables_map vm;

	try {
		bpo::store(bpo::command_line_parser(argc, argv).options(cmdline_options).run(), vm);

		if (vm.count("help")) {
			std::cout << generic << std::endl;
			return 0;
		}

		bpo::notify(vm);
	} catch (const std::exception &e) {
		std::cerr << "Invalid options: " << e.what() << "\n" << generic << std::endl;
		return -1;
	}

	signal(SIGTERM, signal_handler);
	signal(SIGINT, signal_handler);

	for (auto &a: als) {
		supported_alphabet.merge(a);
	}

	lj_parser parser(thread_num, cc);
	auto err = parser.load_langdetect_stats(lang_path);
	if (err) {
		std::cerr << "could not open load language stats: " << err.message() << std::endl;
		return err.code();
	}

	for (const auto &mp: lang_models) {
		size_t pos = mp.find(':');
		if (pos == std::string::npos) {
			std::cerr << "invalid language model path: " << mp << std::endl;
			return -1;
		}

		warp::language_model lm;
		lm.language = mp.substr(0, pos);
		lm.lang_model_path = mp.substr(pos+1);

		err = parser.load_language_model(lm);
		if (err) {
			std::cerr << "could not load language model: " << err.message() << std::endl;
			return err.code();
		}
	}

	err = parser.open(output);
	if (err) {
		std::cerr << "could not open rocksdb database: " << err.message() << std::endl;
		return err.code();
	}


	namespace bio = boost::iostreams;
	ribosome::timer tm, realtm, last_print;

	std::ifstream file(input, std::ios::in | std::ios::binary);
	bio::filtering_streambuf<bio::input> bin;
	bin.push(bio::gzip_decompressor());
	bin.push(file);

	std::istream in(&bin);


	size_t rewind_lines = rewind;
	size_t total_size = 0;
	size_t real_size = 0;

	parse_stats prev_ps;

	auto print_stats = [&] () -> char * {
		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);

		static char tmp[1024];

		parse_stats ps = parser.pstats();
		parse_stats dps = ps - prev_ps;

		snprintf(tmp, sizeof(tmp),
			"%s: %ld seconds: loaded: %.2f MBs, documents: %ld, lines: %ld, speed: %.2f MB/s %.2f [%.2f] lines/s, %s",
			print_time(ts.tv_sec, ts.tv_nsec),
			tm.elapsed() / 1000, total_size / (1024 * 1024.0),
			ps.documents + rewind_lines, ps.lines + rewind_lines,
			real_size * 1000.0 / (realtm.elapsed() * 1024 * 1024.0),
			ps.lines * 1000.0 / (float)realtm.elapsed(), dps.lines * 1000.0 / (float)last_print.elapsed(),
			ps.print_stats().c_str());
		prev_ps = ps;
		last_print.restart();
		return tmp;
	};

	std::string line;
	while (std::getline(in, line) && !global_need_exit) {
		total_size += line.size();

		if (rewind > 0) {
			--rewind;

			if (last_print.elapsed() > print_interval) {
				printf("%s, rewind: %ld\n", print_stats(), rewind);
			}

			if (rewind == 0) {
				realtm.restart();
				printf("\n");
			}
			continue;
		}

		real_size += line.size();

		parser.queue_work(std::move(line));

		if (last_print.elapsed() > print_interval) {
			printf("%s\n", print_stats());
		}

	}
	parser.sync_wait();
	parser.write_indexes();

	printf("\n%s\n", print_stats());

	if (vm.count("compact")) {
		tm.restart();
		printf("Starting compaction\n");
		parser.compact();
		printf("Compaction1 completed, time: %ld seconds\n", tm.restart() / 1000);
		parser.compact();
		printf("Compaction2 completed, time: %ld seconds\n", tm.restart() / 1000);
	}


	return 0;
}
