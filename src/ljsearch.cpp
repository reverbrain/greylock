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
static ribosome::alphabet drop_alphabet;
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

//#define SERIALIZER_DEBUG

class serializer {
public:
	serializer(greylock::database &db, int workers): m_db(db), m_sns(workers, 0) {
	}

	~serializer() {
	}

	ribosome::error_info submit_work(int worker, size_t sn, bool flush) {
		(void) worker;
		(void) sn;
		(void) flush;
		return ribosome::error_info();
	}

	ribosome::error_info sync() {
		return submit_work(0, 0xffffffffffffffff, true);
	}

private:
	greylock::database &m_db;

	std::mutex m_lock;
	std::vector<size_t> m_sns;
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
	long indexes = 0;
	long shards = 0;
	long empty_authors = 0;
	long skipped_documents = 0;
	long processed_text_size = 0;
	long skipped_text_size = 0;
	long written_data_size = 0;

	struct word_cache_stats cstats;

	worker_stats &operator+(const worker_stats &other) {
		documents += other.documents;
		lines += other.lines;
		indexes += other.indexes;
		shards += other.shards;
		empty_authors += other.empty_authors;
		skipped_documents += other.skipped_documents;
		processed_text_size += other.documents;
		processed_text_size += other.processed_text_size;
		written_data_size += other.written_data_size;

		return *this;
	}
};

class index_cache {
public:
	typedef std::vector<greylock::document_for_index> index_container_t;
	typedef std::vector<size_t> shard_container_t;

	typedef std::map<std::string, greylock::disk_index> token_indexes_t;
	typedef std::map<std::string, greylock::disk_token> token_shards_t;

	index_cache(greylock::database &db, serializer &ser) : m_db(db), m_serializer(ser) {}

	long cached_documents() const {
		return m_docs.size();
	}
	long cached_empty_authors() const {
		return m_empty_authors;
	}

	std::vector<std::string> mboxes(const greylock::document &doc) {
		std::vector<std::string> ret;

		if (doc.is_comment) {
			ret.emplace_back("comment");

			size_t pos = doc.id.rfind('?');
			pos = doc.id.rfind('/', pos);
			if (pos != 0) {
				std::string journal = doc.id.substr(0, pos);
				ret.emplace_back("journal." + journal + ".comment");
			}

			if (doc.author.size()) {
				ret.emplace_back("author." + doc.author + ".comment");
			}
		} else {
			ret.emplace_back("post");

			if (doc.author.size()) {
				ret.emplace_back("journal." + doc.author + ".post");
			} else {
				m_empty_authors++;
			}
		}

		return ret;
	}

	void index(const greylock::document &doc) {
		m_docs.push_back(doc);
	}

	ribosome::error_info write_indexes(int worker_id, worker_stats *wstats) {
		if (m_docs.empty())
			return ribosome::error_info();

		rocksdb::WriteBatch indexes_batch, shards_batch;

		long indexes_data_size = 0;
		long indexes = 0;
		long shards_data_size = 0;
		long shards = 0;

		token_indexes_t token_indexes;
		token_shards_t token_shards;

		greylock::error_info indexes_write_error, shards_write_error;

		size_t max_sn = 0;
		for (auto &doc: m_docs) {
			size_t shard_number = greylock::document::generate_shard_number(greylock::options(), doc.indexed_id);
			if (shard_number > max_sn)
				max_sn = shard_number;

			generate_indexes(doc, token_indexes, token_shards);
		}

		for (auto &p: token_shards) {
			std::string sdt = serialize(p.second);
			shards_batch.Merge(m_db.cfhandle(greylock::options::token_shards_column),
					rocksdb::Slice(p.first), rocksdb::Slice(sdt));
			shards_data_size += sdt.size();
		}

		shards = shards_batch.Count();
		shards_write_error = m_db.write(&shards_batch);


		for (auto &p: token_indexes) {
			std::string sdi = serialize(p.second);
			indexes_batch.Merge(m_db.cfhandle(greylock::options::indexes_column),
					rocksdb::Slice(p.first), rocksdb::Slice(sdi));
			indexes_data_size += sdi.size();
		}

		indexes = indexes_batch.Count();
		indexes_write_error = m_db.write(&indexes_batch);


		if (indexes_write_error) {
			return ribosome::create_error(indexes_write_error.code(), "could not write indexes: %s",
					indexes_write_error.message().c_str());
		}
		if (shards_write_error) {
			return ribosome::create_error(shards_write_error.code(), "could not write shards: %s",
					shards_write_error.message().c_str());
		}

		auto err = m_serializer.submit_work(worker_id, max_sn, false);
		if (err)
			return err;

		wstats->documents += m_docs.size();
		wstats->indexes += indexes;
		wstats->shards += shards;
		wstats->empty_authors += m_empty_authors;
		wstats->written_data_size += indexes_data_size + shards_data_size;

		clear();
		return ribosome::error_info();
	}

	void clear() {
		m_docs.clear();

		m_empty_authors = 0;
	}

	void swap(index_cache &other) {
		std::swap(m_docs, other.m_docs);

		long tmp;
		tmp = m_empty_authors;
		m_empty_authors = other.m_empty_authors;
		other.m_empty_authors = tmp;
	}

private:
	greylock::database &m_db;
	serializer &m_serializer;

	std::list<greylock::document> m_docs;

	long m_empty_authors = 0;

	void generate_indexes(greylock::document &doc, token_indexes_t &ti, token_shards_t &ts) {
		greylock::document_for_index did;
		did.indexed_id = doc.indexed_id;

		auto mboxes = this->mboxes(doc);
		for (auto &mbox: mboxes) {
			doc.mbox = mbox;
			doc.generate_token_keys(m_db.options());

			for (auto &attr: doc.idx.attributes) {
				for (auto &t: attr.tokens) {
					auto it = ti.find(t.key);
					if (it == ti.end()) {
						greylock::disk_index tmp;
						tmp.ids.push_back(did);
						ti.insert(std::pair<std::string, greylock::disk_index>(t.key, std::move(tmp)));
					} else {
						it->second.ids.push_back(did);
					}

					auto sh = ts.find(t.shard_key);
					if (sh == ts.end()) {
						greylock::disk_token tmp;
						tmp.shards.insert(tmp.shards.begin(), t.shards.begin(), t.shards.end());
						ts.insert(std::pair<std::string, greylock::disk_token>(t.shard_key, std::move(tmp)));
					} else {
						sh->second.shards.insert(sh->second.shards.end(), t.shards.begin(), t.shards.end());
					}
				}
			}
		}
	}
};

class lj_worker {
public:
	lj_worker(int worker_id, greylock::database &db, warp::language_checker &lch, const cache_control &cc, serializer &ser):
		m_worker_id(worker_id),
		m_db(db), m_lch(lch), m_cc(cc),
		m_word_cache(cc.word_cache_size),
		m_serializer(ser),
		m_index_cache(db, ser)
	{
	}

	~lj_worker() {
		write_cached_indexes();
		m_serializer.sync();
	}

	void swap(index_cache &dst) {
		std::lock_guard<std::mutex> guard(m_lock);
		m_index_cache.swap(dst);
		m_index_cache.clear();
	}

	ribosome::error_info write_cached_indexes() {
		std::lock_guard<std::mutex> guard(m_lock);
		return m_index_cache.write_indexes(m_worker_id, &m_wstats);
	}

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

	ribosome::error_info process(std::list<greylock::document> &docs) {
		ribosome::error_info err;

		while (docs.size()) {
			auto &doc = docs.front();

			err = process_one_document(doc);
			if (err)
				return err;

			docs.pop_front();
		}

#if 0
		std::lock_guard<std::mutex> guard(m_lock);
		m_index_cache.clear();
		return ribosome::error_info();
#endif

		if (cached_documents() > m_cc.index_cached_documents) {
			err = write_cached_indexes();
		}

		return err;
	}

	ribosome::error_info process_one_document(greylock::document &doc) {
		ribosome::split spl;

		auto fix_username = [] (const std::string &s) -> std::string {
			static const std::string users_prefix = "users.livejournal.com/";
			size_t apos = s.find(users_prefix);
			if (apos != std::string::npos) {
				std::string user;
				char *start = (char *)s.c_str() + users_prefix.size() + apos;
				char *end = strchr(start, '/');
				if (end) {
					user.assign(start, end - start);
				} else {
					user.assign(start);
				}

				return user + ".livejournal.com";
			}

			return s;
		};

		doc.author = fix_username(doc.author);


		auto split_content = [&] (const std::string &content, greylock::attribute *a) {
			std::set<std::string> stems;

			auto get_stem = [&] (const std::string &word, const ribosome::lstring &idx) -> std::string {
				std::string *stem_ptr = m_word_cache.get(word);
				if (!stem_ptr) {
#if 1
					std::string lang = m_lch.language(word, idx);
					std::string stem = m_stemmer.stem(word, lang, "");
#else
					std::string stem = word;
#endif
					m_word_cache.insert(word, stem);

					stems.insert(stem);
					return stem;
				} else {
					stems.insert(*stem_ptr);
					return *stem_ptr;
				}
			};

			ribosome::lstring lt = ribosome::lconvert::from_utf8(content);
			auto lower_request = ribosome::lconvert::to_lower(lt);

			auto all_words = spl.convert_split_words_drop_alphabet(lower_request, drop_alphabet);
			for (size_t pos = 0; pos < all_words.size(); ++pos) {
				auto &idx = all_words[pos];
				std::string word = ribosome::lconvert::to_string(idx);

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

		split_content(doc.ctx.content, &fc);
		split_content(doc.ctx.title, &ft);

		if (ft.tokens.empty() && fc.tokens.empty()) {
			m_wstats.skipped_documents++;
		}

		for (auto &url: doc.ctx.links) {
			auto all_urls = spl.convert_split_words_drop_alphabet(ribosome::lconvert::from_utf8(url), drop_alphabet);
			for (auto &u: all_urls) {
				urls.insert(ribosome::lconvert::to_string(u), 0);
			}
		}
		for (auto &url: doc.ctx.images) {
			auto all_urls = spl.convert_split_words_drop_alphabet(ribosome::lconvert::from_utf8(url), drop_alphabet);
			for (auto &u: all_urls) {
				urls.insert(ribosome::lconvert::to_string(u), 0);
			}
		}


		doc.idx.attributes.emplace_back(ft);
		doc.idx.attributes.emplace_back(fc);
		doc.idx.attributes.emplace_back(urls);

		m_index_cache.index(doc);

		std::unique_lock<std::mutex> guard(m_lock);
		m_wstats.lines++;
		return ribosome::error_info();
	}

	const worker_stats &wstats() {
		std::unique_lock<std::mutex> guard(m_lock);
		m_wstats.cstats = m_word_cache.cstats();
		return m_wstats;
	}

private:
	int m_worker_id = 0;

	greylock::database &m_db;
	warp::language_checker &m_lch;

	cache_control m_cc;

	word_cache<std::string, std::string> m_word_cache;

	serializer &m_serializer;

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

		std::string id = cut_scheme(p.id);
		replace_minus(id);

		doc.assign_id("", std::hash<std::string>{}(id), p.issued, 0);
		//doc.assign_id("", m_db.metadata().get_sequence(), p.issued, 0);
		doc.id = std::move(id);

		if (p.author.size()) {
			doc.author = cut_scheme(p.author);
			replace_minus(doc.author);
		} else {
			if (!p.is_comment) {
				size_t pos = doc.id.find('/');
				doc.author = doc.id.substr(0, pos);
			}
		}

		doc.is_comment = p.is_comment;

		auto parse = [&] (const std::string &content) {
			m_html.feed_text(content);

			doc.ctx.links.insert(doc.ctx.links.end(), m_html.links().begin(), m_html.links().end());
			doc.ctx.images.insert(doc.ctx.images.end(), m_html.images().begin(), m_html.images().end());

			return m_html.text(" ");
		};

		doc.ctx.content = parse(p.content);
		doc.ctx.title = parse(p.title);

		m_wstats.processed_text_size += p.content.size() + p.title.size();
		m_wstats.lines++;
		return write_document(doc);
	}

	ribosome::error_info write_document(const greylock::document &doc) {
		rocksdb::WriteBatch batch;

		std::string doc_serialized = serialize(doc);
		// may not be rvalue, otherwise batch will cache stall pointer
		std::string dkey = doc.indexed_id.to_string();
		batch.Put(m_db.cfhandle(greylock::options::documents_column),
				rocksdb::Slice(dkey), rocksdb::Slice(doc_serialized));

		std::string doc_indexed_id_serialized = serialize(doc.indexed_id);
		batch.Put(m_db.cfhandle(greylock::options::document_ids_column),
				rocksdb::Slice(doc.id), rocksdb::Slice(doc_indexed_id_serialized));

		auto err = m_db.write(&batch);
		if (err) {
			return ribosome::create_error(err.code(), "could not write batch: %s", err.message().c_str());
		}

		std::unique_lock<std::mutex> guard(m_lock);
		m_wstats.written_data_size += doc_serialized.size() + doc_indexed_id_serialized.size();
		m_wstats.documents++;

		return ribosome::error_info();
	}
};

struct parse_stats {
	long documents = 0;
	long lines = 0;
	long indexes = 0;
	long shards = 0;
	long skipped_documents = 0;
	long skipped_text_size = 0;
	long processed_text_size = 0;
	long written_data_size = 0;

	word_cache_stats wcstats;

	void merge(const worker_stats &ws) {
		documents += ws.documents;
		lines += ws.lines;
		indexes += ws.indexes;
		shards += ws.shards;
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
		indexes -= other.indexes;
		shards -= other.shards;
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

	std::string print_stats() const {
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

template <typename ET = std::string>
class lj_parser {
public:
	lj_parser(int n, const struct cache_control &cc) : m_cc(cc), m_serializer(m_db, n) {
		m_pool.reserve(n);
		m_workers.reserve(n);

		for (int i = 0; i < n; ++i) {
			m_pool.emplace_back(std::bind(&lj_parser::callback, this, i));
			m_workers.emplace_back(std::unique_ptr<lj_worker>(new lj_worker(i, m_db, m_lch, cc, m_serializer)));
		}
	}

	~lj_parser() {
		m_need_exit = true;
		m_pool_wait.notify_all();
		for (auto &t: m_pool) {
			t.join();
		}

		write_all_indexes();
		process_jobs(0);
	}

	void compact() {
		m_db.compact();
	}

	ribosome::error_info open(const std::string &dbpath) {
		auto err = m_db.open(dbpath, false, true);
		if (err)
			return ribosome::create_error(err.code(), "%s", err.message().c_str());

		return ribosome::error_info();
	}

	ribosome::error_info open_read_only(const std::string &dbpath) {
		auto err = m_db.open_read_only(dbpath);
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

	void queue_work(ET &&str) {
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
	void iterate(std::vector<iter<T>> &its, int column, rocksdb::WriteBatch *batch, std::function<std::string (const C&)> sfunc) {
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
			batch->Merge(m_db.cfhandle(column), rocksdb::Slice(name), rocksdb::Slice(sdata));
		}
	}

	greylock::error_info write_all_indexes() {
		// protect against write_all_indexes() running in parallel
		std::unique_lock<std::mutex> guard(m_sync_lock);
		for (auto &w: m_workers) {
			auto err = w->write_cached_indexes();
			if (err) {
				return greylock::create_error(err.code(), "could not write indexes: %s", err.message().c_str());
			}
		}

		return greylock::error_info();
	}

	void sync() {
		while (m_lines.size()) {
			std::unique_lock<std::mutex> guard(m_lock);
			m_pool_wait.notify_one();

			m_parser_wait.wait(guard);
		}

		m_serializer.sync();
	}

private:
	greylock::database m_db;
	warp::language_checker m_lch;
	cache_control m_cc;
	serializer m_serializer;

	std::vector<std::unique_ptr<lj_worker>> m_workers;

	bool m_need_exit = false;
	std::mutex m_lock;
	std::list<ET> m_lines;
	std::condition_variable m_pool_wait, m_parser_wait;
	std::vector<std::thread> m_pool;

	worker_stats m_icache_wstats;

	std::mutex m_sync_lock;

	void process_jobs(int idx) {
		if (idx >= (int)m_workers.size())
			return;

		auto &worker = m_workers[idx];

		while (!m_lines.empty()) {
			std::unique_lock<std::mutex> guard(m_lock);

			if (m_lines.empty())
				return;

			ET line = std::move(m_lines.front());
			m_lines.pop_front();
			guard.unlock();

			m_parser_wait.notify_all();

			auto err = worker->process(line);
			if (err) {
				std::cerr << "could not process line: " << err.message() << std::endl;
				exit(err.code());
			}
		}
	}

	void callback(int idx) {
		while (!m_need_exit) {
			std::unique_lock<std::mutex> guard(m_lock);
			m_pool_wait.wait_for(guard, std::chrono::milliseconds(100), [&] {return !m_lines.empty();});

			guard.unlock();
			process_jobs(idx);
		}
	}
};


int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Parser options");

	std::vector<std::string> als;
	std::vector<std::string> lang_models;
	std::string output, indexdb, lang_path;
	std::vector<std::string> inputs;
	size_t rewind = 0;
	int thread_num;
	std::string rewind_doc;
	cache_control cc;
	long print_interval;
	size_t shard_chunk_size = 10000;
	int shard_start = 0;
	size_t index_documents;
	generic.add_options()
		("help", "This help message")
		("input", bpo::value<std::vector<std::string>>(&inputs)->composing(), "Livejournal dump files packed with bzip2")
		("indexdb", bpo::value<std::string>(&indexdb), "Rocksdb database where livejournal posts are stored")
		("output", bpo::value<std::string>(&output)->required(), "Output rocksdb database (when indexing, '.NNN suffix will be added)")
		("index-documents", bpo::value<size_t>(&index_documents)->default_value(100000000),
			"Number of documents to be indexed and put into single output database")
		("rewind", bpo::value<size_t>(&rewind), "Rewind input to this line number")
		("rewind-doc", bpo::value<std::string>(&rewind_doc), "Rewind document iterator to this document id")
		("threads", bpo::value<int>(&thread_num)->default_value(8), "Number of parser threads")
		("alphabet", bpo::value<std::vector<std::string>>(&als)->composing(), "Allowed alphabet")
		("compact", "Compact database on exit")
		("print-interval", bpo::value<long>(&print_interval)->default_value(1000), "Statistics print interval in milliseconds")
		("word-cache", bpo::value<long>(&cc.word_cache_size)->default_value(100000), "Word->stem per thread cLRU cache size")
		("index-cache-interval", bpo::value<long>(&cc.index_cache_sync_interval)->default_value(60),
			"Per-thread index cache flush interval in seconds")
		("index-cached-documents", bpo::value<long>(&cc.index_cached_documents)->default_value(1000),
			"Maximum number of documents parsed and cached per thread")
		("index-shard-chunk-size", bpo::value<size_t>(&shard_chunk_size)->default_value(10000),
			"Maximum number of documents in the shard chunk")
		("index-shard-start", bpo::value<int>(&shard_start)->default_value(0), "Index shard start id")
		("lang_stats", bpo::value<std::string>(&lang_path), "Language stats file")
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

	ribosome::timer tm, realtm, last_print;

	size_t rewind_lines = rewind;
	size_t prev_docs_lines = 0;
	size_t total_size = 0;
	size_t real_size = 0;

	parse_stats prev_ps;

	auto print_stats_docs = [&] (const parse_stats &ps) -> char * {
		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);

		static char tmp[1024];

		parse_stats dps = ps - prev_ps;

		snprintf(tmp, sizeof(tmp),
			"%s: %ld seconds: loaded: %.2f MBs, documents: %ld [%ld], speed: %.2f MB/s %.2f [%.2f] lines/s, "
				"processed_text: %.2f MBs, written data: %.2f MBs",
			print_time(ts.tv_sec, ts.tv_nsec),
			tm.elapsed() / 1000, total_size / (1024 * 1024.0),
			ps.documents + rewind_lines,
			ps.documents + rewind_lines - prev_docs_lines,
			real_size * 1000.0 / (realtm.elapsed() * 1024 * 1024.0),
			ps.lines * 1000.0 / (float)realtm.elapsed(), dps.lines * 1000.0 / (float)last_print.elapsed(),
			ps.processed_text_size / 1024 / 1024.0, ps.written_data_size / 1024 / 1024.0);

		prev_ps = ps;
		last_print.restart();
		return tmp;
	};
	auto print_stats = [&] (const parse_stats &ps) -> char * {
		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);

		static char tmp[1024];

		parse_stats dps = ps - prev_ps;

		long indexes_per_document = ps.indexes;
		if (ps.documents)
			indexes_per_document /= ps.documents;
		long shards_per_document = ps.shards;
		if (ps.documents)
			shards_per_document /= ps.documents;

		auto lps = ps.lines * 1000.0 / (float)realtm.elapsed();
		auto dlps = dps.lines * 1000.0 / (float)last_print.elapsed();
		auto lines_per_second = lps * 0.8 + dlps * 0.2;

		snprintf(tmp, sizeof(tmp),
			"%s: %ld seconds: loaded: %.2f MBs, documents: %ld, indexes: %ld [%ld], shards: %ld [%ld], "
			"speed: %.2f lines/s, %s",
			print_time(ts.tv_sec, ts.tv_nsec),
			tm.elapsed() / 1000, total_size / (1024 * 1024.0),
			ps.documents + rewind_lines,
			ps.indexes, indexes_per_document, ps.shards, shards_per_document,
			lines_per_second,
			ps.print_stats().c_str());
		prev_ps = ps;
		last_print.restart();
		return tmp;
	};


	if (inputs.size()) {
		lj_parser<std::string> parser(thread_num, cc);
		auto err = parser.open(output);
		if (err) {
			std::cerr << "could not open output rocksdb database: " << err.message() << std::endl;
			return err.code();
		}


		for (auto &input: inputs) {
			namespace bio = boost::iostreams;

			printf("opening new input file: %s, documents processed so far: %ld\n", input.c_str(), prev_docs_lines);

			std::ifstream file(input, std::ios::in | std::ios::binary);
			bio::filtering_streambuf<bio::input> bin;
			bin.push(bio::gzip_decompressor());
			bin.push(file);

			std::istream in(&bin);
			std::string line;

			while (std::getline(in, line) && !global_need_exit) {
				total_size += line.size();

				if (rewind > 0) {
					--rewind;

					if (last_print.elapsed() > print_interval) {
						printf("%s: rewind: %ld\n", input.c_str(), rewind);
						last_print.restart();
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
					printf("%s: %s\n", input.c_str(), print_stats_docs(parser.pstats()));
				}
			}

			prev_docs_lines = parser.pstats().documents + rewind_lines;
		}

		parser.sync();
		// it is possible that worker thread has grabbed the line, but hasn't yet process it
		// give it some time to complete
		sleep(1);
		printf("\n%s\n", print_stats_docs(parser.pstats()));

		if (vm.count("compact")) {
			tm.restart();
			printf("Starting compaction\n");
			parser.compact();
			printf("Compaction1 completed, time: %ld seconds\n", tm.restart() / 1000);
			parser.compact();
			printf("Compaction2 completed, time: %ld seconds\n", tm.restart() / 1000);
		}
	} else if (indexdb.size()) {
		if (lang_path.empty()) {
			std::cerr << "indexing requires language detector" << std::endl;
			return -ENOENT;
		}

		lj_parser<std::list<greylock::document>> parser(thread_num, cc);

		greylock::database idb;
		auto gerr = idb.open_read_only(indexdb);
		if (gerr) {
			std::cerr << "could not open in read-only mode input rocksdb database: " << gerr.message() << std::endl;
			return gerr.code();
		}

		drop_alphabet.merge(drop_characters);
		for (auto &a: als) {
			supported_alphabet.merge(a);
		}
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

		auto it = idb.iterator(greylock::options::documents_column, rocksdb::ReadOptions());
		if (rewind_doc.size()) {
			it->Seek(rewind_doc);
		} else {
			it->SeekToFirst();
		}

		if (!it->Valid()) {
			auto s = it->status();
			fprintf(stderr, "iterator from database %s is not valid: %s [%d]", indexdb.c_str(), s.ToString().c_str(), s.code());
			return -s.code();
		}

		realtm.restart();

		while (it->Valid()) {
			std::string output_name = output + "." + std::to_string(shard_start);
			auto err = parser.open(output_name);
			if (err) {
				std::cerr << "could not open output rocksdb database: " << err.message() << std::endl;
				return err.code();
			}

			shard_start++;

			size_t current_documents = 0;
			size_t prev_shard_number = 0;
			std::list<greylock::document> docs;
			for (; it->Valid(); it->Next()) {
				if (rewind) {
					rewind--;
					if (last_print.elapsed() > print_interval) {
						printf("rewind: %ld\n", rewind);
						last_print.restart();
					}

					if (rewind == 0) {
						realtm.restart();
						printf("\n");
					}
					continue;
				}

				auto sl = it->value();

				greylock::document doc;
				gerr = deserialize(doc, sl.data(), sl.size());
				if (gerr) {
					fprintf(stderr, "could not deserialize document, key: %s, size: %ld, error: %s [%d]\n",
							it->key().ToString().c_str(), sl.size(), gerr.message().c_str(), gerr.code());
					return gerr.code();
				}

				total_size += doc.ctx.content.size() + doc.ctx.title.size();
				real_size += doc.ctx.content.size() + doc.ctx.title.size();

				size_t shard_number = greylock::document::generate_shard_number(greylock::options(), doc.indexed_id);
				if (shard_number > 0xffffffff) {
					printf("shard_number: %ld [%lx], id: %s, doc: %s\n",
							shard_number, shard_number,
							doc.indexed_id.to_string().c_str(), doc.id.c_str());
				}

				if ((docs.size() && (shard_number != prev_shard_number)) || docs.size() == shard_chunk_size) {
					if (shard_number != prev_shard_number) {
						//printf("%s, shard: %ld, docs: %ld\n", print_stats(parser.pstats()), prev_shard_number, docs.size());
					}

					parser.queue_work(std::move(docs));
					docs.clear();
				}

				// reopen shard, current document will be deserialized again and put into new db
				if (++current_documents > index_documents && docs.empty()) {
					parser.write_all_indexes();
					parser.sync();
					printf("%s: going to compact database %s\n", print_stats(parser.pstats()), output_name.c_str());
					parser.compact();
					printf("Compaction of %s has been completed\n", output_name.c_str());
					break;
				}

				prev_shard_number = shard_number;
				docs.emplace_back(doc);

				if (last_print.elapsed() > print_interval) {
					printf("%s\n", print_stats(parser.pstats()));
				}


			}
		}

		//parser.sync_wait_completed();
		parser.write_all_indexes();
		parser.sync();
		// it is possible that worker thread has grabbed the line, but hasn't yet process it
		// give it some time to complete
		sleep(1);
		parser.write_all_indexes();
		printf("\n%s\n", print_stats(parser.pstats()));

		parser.write_all_indexes();
		printf("%s\n", print_stats(parser.pstats()));

		if (vm.count("compact")) {
			tm.restart();
			printf("Starting compaction\n");
			parser.compact();
			printf("Compaction1 completed, time: %ld seconds\n", tm.restart() / 1000);
			parser.compact();
			printf("Compaction2 completed, time: %ld seconds\n", tm.restart() / 1000);
		}


	}

	return 0;
}

