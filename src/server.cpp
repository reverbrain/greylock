#include "greylock/database.hpp"
#include "greylock/error.hpp"
#include "greylock/json.hpp"
#include "greylock/jsonvalue.hpp"
#include "greylock/intersection.hpp"
#include "greylock/types.hpp"

#include <unistd.h>
#include <signal.h>

#include <thevoid/server.hpp>
#include <thevoid/stream.hpp>

#include <ribosome/expiration.hpp>
#include <ribosome/split.hpp>
#include <ribosome/timer.hpp>

#include <swarm/logger.hpp>

#include <msgpack.hpp>

#include <functional>
#include <string>
#include <thread>

#define ILOG(level, a...) BH_LOG(logger(), level, ##a)
#define ILOG_ERROR(a...) ILOG(SWARM_LOG_ERROR, ##a)
#define ILOG_WARNING(a...) ILOG(SWARM_LOG_WARNING, ##a)
#define ILOG_INFO(a...) ILOG(SWARM_LOG_INFO, ##a)
#define ILOG_NOTICE(a...) ILOG(SWARM_LOG_NOTICE, ##a)
#define ILOG_DEBUG(a...) ILOG(SWARM_LOG_DEBUG, ##a)

using namespace ioremap;

template <typename Server>
struct simple_request_stream_error : public thevoid::simple_request_stream<Server> {
	void send_error(int status, int error, const char *fmt, ...) {
		va_list args;
		va_start(args, fmt);

		char buffer[1024];
		int sz = vsnprintf(buffer, sizeof(buffer), fmt, args);

		BH_LOG(this->server()->logger(), SWARM_LOG_ERROR, "%s: %d", buffer, error);

		greylock::JsonValue val;
		rapidjson::Value ev(rapidjson::kObjectType);


		rapidjson::Value esv(buffer, sz, val.GetAllocator());
		ev.AddMember("message", esv, val.GetAllocator());
		ev.AddMember("code", error, val.GetAllocator());
		val.AddMember("error", ev, val.GetAllocator());

		va_end(args);

		std::string data = val.ToString();

		thevoid::http_response http_reply;
		http_reply.set_code(status);
		http_reply.headers().set_content_length(data.size());
		http_reply.headers().set_content_type("text/json");

		this->send_reply(std::move(http_reply), std::move(data));
	}
};


class http_server : public thevoid::server<http_server>
{
public:
	virtual ~http_server() {
		m_expiration_timer.stop();

		if (db().db) {
			db().sync_metadata(NULL);
			ILOG_INFO("Synced metadata, key: %s", db().opts.metadata_key.c_str());
		}
	}
	virtual bool initialize(const rapidjson::Value &config) {
		if (!rocksdb_init(config))
			return false;

		on<on_ping>(
			options::exact_match("/ping"),
			options::methods("GET")
		);

		on<on_compact>(
			options::exact_match("/compact"),
			options::methods("POST", "PUT")
		);

		on<on_index>(
			options::exact_match("/index"),
			options::methods("POST", "PUT")
		);
		on<on_index>(
			options::exact_match("/index_tokens"),
			options::methods("POST", "PUT")
		);

		on<on_search>(
			options::exact_match("/search"),
			options::methods("POST", "PUT")
		);

		return true;
	}

	const greylock::options &options() const {
		return m_db.opts;
	}

	greylock::error_info meta_insert(greylock::document &doc) {
		std::lock_guard<std::mutex> guard(m_lock);
		db().meta.insert(options(), doc);
		return greylock::error_info();
	}

	struct on_ping : public simple_request_stream_error<http_server> {
		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			(void) buffer;
			(void) req;

			this->send_reply(thevoid::http_response::ok);
		}
	};

	struct on_compact : public simple_request_stream_error<http_server> {
		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			(void) req;
			(void) buffer;

			server()->db().compact();
			this->send_reply(thevoid::http_response::ok);
		}
	};

	struct on_search : public simple_request_stream_error<http_server> {
		bool check_negation(const std::vector<greylock::token> &tokens, const std::vector<std::string> &content) {
			for (const auto &t: tokens) {
				for (const auto &word: content) {
					if (t.name == word) {
						return true;
					}
				}
			}

			return false;
		}

		bool check_exact(const std::vector<greylock::token> &tokens, const std::vector<std::string> &content) {
			auto check_token_positions = [] (const greylock::token &token,
					const std::vector<std::string> &content, size_t content_offset) -> bool {
				for (size_t pos: token.positions) {
					size_t offset = content_offset + pos;
					if (offset >= content.size()) {
						return false;
					}

					if (token.name != content[offset]) {
						return false;
					}
				}

				return true;
			};

			for (size_t content_offset = 0; content_offset < content.size(); ++content_offset) {
				bool match = true;

				for (const auto &token: tokens) {
					match = check_token_positions(token, content, content_offset);
					if (!match)
						break;
				}

				if (match)
					return true;
			}

			return false;
		}

		// returns true if record has to be accepted, false - if record must be dropped
		bool check_result(const greylock::indexes &idx, greylock::single_doc_result &sd) {
			const greylock::document &doc = sd.doc;

			for (const auto &attr: idx.negation) {
				bool should_drop;

				if (attr.name.find("title") != std::string::npos) {
					should_drop = check_negation(attr.tokens, doc.ctx.stemmed_title);
				} else {
					should_drop = check_negation(attr.tokens, doc.ctx.stemmed_content);
				}

				if (should_drop)
					return false;
			}

			for (const auto &attr: idx.exact) {
				bool match;

				if (attr.name.find("title") != std::string::npos) {
					match = check_exact(attr.tokens, doc.ctx.title);
				} else {
					match = check_exact(attr.tokens, doc.ctx.content);
				}

				if (!match)
					return false;
			}

			return true;
		}

		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			(void) req;

			ribosome::timer search_tm;

			// this is needed to put ending zero-byte, otherwise rapidjson parser will explode
			std::string data(const_cast<char *>(boost::asio::buffer_cast<const char*>(buffer)),
					boost::asio::buffer_size(buffer));

			rapidjson::Document doc;
			doc.Parse<0>(data.c_str());

			if (doc.HasParseError()) {
				send_error(swarm::http_response::bad_request, -EINVAL,
						"search: could not parse document: %s, error offset: %d",
						doc.GetParseError(), doc.GetErrorOffset());
				return;
			}


			if (!doc.IsObject()) {
				send_error(swarm::http_response::bad_request, -EINVAL, "search: document must be object");
				return;
			}

			const char *mbox = greylock::get_string(doc, "mailbox");
			if (!mbox) {
				send_error(swarm::http_response::bad_request, -ENOENT, "search: 'mailbox' must be a string");
				return;
			}

			greylock::document::id_t next_document_id = 0;
			size_t max_number = ~0UL;

			const auto &paging = greylock::get_object(doc, "paging");
			if (paging.IsObject()) {
				next_document_id = greylock::get_int64(paging, "next_document_id", 0);
				max_number = greylock::get_int64(paging, "max_number", ~0UL);
			}

			greylock::indexes idx;

			const rapidjson::Value &query_and = greylock::get_object(doc, "query");
			if (query_and.IsObject()) {
				auto ireq = server()->get_indexes(query_and);
				idx.merge_query(ireq);
			}

			const rapidjson::Value &query_exact = greylock::get_object(doc, "exact");
			if (query_exact.IsObject()) {
				auto ireq = server()->get_indexes(query_exact);

				// merge these indexes into intersection set,
				// since exact phrase match implies document contains all tokens
				idx.merge_exact(ireq);
			}

			const rapidjson::Value &query_negation = greylock::get_object(doc, "negation");
			if (query_negation.IsObject()) {
				auto ireq = server()->get_indexes(query_negation);
				// do not merge these indexes into intersection set, put them into own container
				idx.merge_negation(ireq);
			}

			if (idx.attributes.empty()) {
				send_error(swarm::http_response::bad_request, -ENOENT,
						"search: mailbox: %s, there are no queries suitable for search", mbox);
				return;
			}

			greylock::search_result result;
			greylock::intersector<greylock::database> inter(server()->db());
			result = inter.intersect(mbox, idx, next_document_id, max_number,
					std::bind(&on_search::check_result, this, std::ref(idx), std::placeholders::_1));

			send_search_result(mbox, result);

			ILOG_INFO("search: attributes: and: %ld, among them exact: %ld, negation: %ld, "
					"next_document_id: %ld -> %ld, max_number: %ld, completed: %d, "
					"indexes: %ld, duration: %d ms",
					idx.attributes.size(), idx.exact.size(), idx.negation.size(),
					next_document_id, result.next_document_id,
					max_number, result.completed, result.docs.size(),
					search_tm.elapsed());
		}

		void pack_string_array(rapidjson::Value &parent, rapidjson::Document::AllocatorType &allocator,
				const char *name, const std::vector<std::string> &data) {
			rapidjson::Value arr(rapidjson::kArrayType);
			for (const auto &s: data) {
				rapidjson::Value v(s.c_str(), s.size(), allocator);
				arr.PushBack(v, allocator);
			}

			parent.AddMember(name, arr, allocator);
		}

		template <typename T>
		void pack_simple_array(rapidjson::Value &parent, rapidjson::Document::AllocatorType &allocator,
				const char *name, const std::vector<T> &data) {
			rapidjson::Value arr(rapidjson::kArrayType);
			for (const auto &s: data) {
				arr.PushBack(s, allocator);
			}

			parent.AddMember(name, arr, allocator);
		}

		void send_search_result(const std::string &mbox, const greylock::search_result &result) {
			greylock::JsonValue ret;
			auto &allocator = ret.GetAllocator();

			rapidjson::Value ids(rapidjson::kArrayType);
			for (auto it = result.docs.begin(), end = result.docs.end(); it != end; ++it) {
				rapidjson::Value key(rapidjson::kObjectType);

				const greylock::document &doc = it->doc;

				rapidjson::Value idv(doc.id.c_str(), doc.id.size(), allocator);
				key.AddMember("id", idv, allocator);
				key.AddMember("indexed_id", doc.indexed_id, allocator);

				rapidjson::Value dv(doc.data.c_str(), doc.data.size(), allocator);
				key.AddMember("data", dv, allocator);

				rapidjson::Value av(doc.author.c_str(), doc.author.size(), allocator);
				key.AddMember("author", av, allocator);

				rapidjson::Value cv(rapidjson::kObjectType);
				pack_string_array(cv, allocator, "content", doc.ctx.content);
				pack_string_array(cv, allocator, "title", doc.ctx.title);
				pack_string_array(cv, allocator, "links", doc.ctx.links);
				pack_string_array(cv, allocator, "images", doc.ctx.images);
				key.AddMember("content", cv, allocator);

				key.AddMember("relevance", it->relevance, allocator);

				rapidjson::Value ts(rapidjson::kObjectType);
				ts.AddMember("tsec", doc.ts.tv_sec, allocator);
				ts.AddMember("tnsec", doc.ts.tv_nsec, allocator);
				key.AddMember("timestamp", ts, allocator);

				ids.PushBack(key, allocator);
			}

			ret.AddMember("ids", ids, allocator);
			ret.AddMember("completed", result.completed, allocator);

			ret.AddMember("next_document_id", result.next_document_id, allocator);

			rapidjson::Value mbval(mbox.c_str(), mbox.size(), allocator);
			ret.AddMember("mailbox", mbval, allocator);

			std::string data = ret.ToString();

			thevoid::http_response reply;
			reply.set_code(swarm::http_response::ok);
			reply.headers().set_content_type("text/json; charset=utf-8");
			reply.headers().set_content_length(data.size());

			this->send_reply(std::move(reply), std::move(data));
		}
	};

	struct on_index : public simple_request_stream_error<http_server> {
		greylock::error_info process_one_document(greylock::document &doc) {
			auto err = server()->meta_insert(doc);
			if (err)
				return err;

			auto wo = rocksdb::WriteOptions();
			rocksdb::WriteBatch batch;
			rocksdb::Status s;

			std::string doc_serialized = serialize(doc);
			rocksdb::Slice doc_value(doc_serialized);

			greylock::document_for_index did;
			did.indexed_id = doc.indexed_id;
			std::string sdid = serialize(did);

			size_t indexes = 0;
			for (const auto &attr: doc.idx.attributes) {
				for (const auto &t: attr.tokens) {
					batch.Merge(rocksdb::Slice(t.key), rocksdb::Slice(sdid));

					greylock::token_disk td(t.shards);
					std::string tds = serialize(td);

					batch.Merge(rocksdb::Slice(t.shard_key), rocksdb::Slice(tds));

					indexes++;
				}
			}

			std::string dkey = server()->options().document_prefix + std::to_string(doc.indexed_id);
			batch.Put(rocksdb::Slice(dkey), doc_value);

			if (server()->options().sync_metadata_timeout == 0) {
				err = server()->db().sync_metadata(&batch);
				if (err) {
					return err;
				}
			}

			s = server()->db().db->Write(wo, &batch);
			if (!s.ok()) {
				return greylock::create_error(-s.code(), "could not write index batch, mbox: %s, id: %s, error: %s",
						doc.mbox.c_str(), doc.id.c_str(), s.ToString().c_str());
			}

			ILOG_INFO("index: successfully indexed document: mbox: %s, id: %s, "
					"indexed_id: %ld, indexes: %ld, serialized_doc_size: %ld",
					doc.mbox.c_str(), doc.id.c_str(),
					doc.indexed_id, indexes, doc_value.size());
			return greylock::error_info();
		}

		template <typename T>
		std::vector<T> get_numeric_vector(const rapidjson::Value &data, const char *name) {
			std::vector<T> ret;
			const auto &arr = greylock::get_array(data, name);
			if (!arr.IsArray())
				return ret;

			for (auto it = arr.Begin(), end = arr.End(); it != end; it++) {
				if (it->IsNumber())
					ret.push_back((T)it->GetDouble());
			}

			return ret;
		}

		std::vector<std::string> get_string_vector(const rapidjson::Value &ctx, const char *name) {
			std::vector<std::string> ret;

			const auto &a = greylock::get_array(ctx, name);
			if (!a.IsArray())
				return ret;

			for (auto it = a.Begin(), end = a.End(); it != end; ++it) {
				if (it->IsString())
					ret.push_back(std::string(it->GetString(), it->GetStringLength()));
			}

			return ret;
		}
		greylock::error_info parse_content(const rapidjson::Value &ctx, greylock::document &doc) {
			doc.ctx.content = get_string_vector(ctx, "content");
			doc.ctx.stemmed_content = get_string_vector(ctx, "stemmed_content");
			doc.ctx.title = get_string_vector(ctx, "title");
			doc.ctx.stemmed_title = get_string_vector(ctx, "stemmed_title");
			doc.ctx.links = get_string_vector(ctx, "links");
			doc.ctx.images = get_string_vector(ctx, "images");

			return greylock::error_info();
		}

		greylock::error_info parse_doc_tokens(greylock::document &doc, const rapidjson::Value &idxs) {
			for (rapidjson::Value::ConstMemberIterator it = idxs.MemberBegin(),
					idxs_end = idxs.MemberEnd(); it != idxs_end; ++it) {
				const char *aname = it->name.GetString();
				const rapidjson::Value &avalue = it->value;

				if (!avalue.IsString())
					continue;

				std::string aname_str(aname);

				greylock::attribute orig(std::string("orig_") + aname_str);
				greylock::attribute stemmed(std::string("fixed_") + aname_str);

				const auto &tokens = greylock::get_array(avalue, "tokens");
				if (!tokens.IsArray()) {
					return greylock::create_error(-ENOENT,
							"index: %s, invalid 'tokens' element, must be an array",
							aname);
				}

				for (auto tok = tokens.Begin(), tok_end = tokens.End(); tok != tok_end; ++tok) {
					if (!tok->IsObject()) {
						return greylock::create_error(-EINVAL,
								"index: %s, 'tokens' array must contain objects",
								aname);
					}

					const char *word = greylock::get_string(*tok, "word");
					if (!word) {
						return greylock::create_error(-EINVAL,
								"index: %s, there is no 'word' field in objects in 'tokens' array",
								aname);
					}
					std::vector<greylock::pos_t> positions =
						get_numeric_vector<greylock::pos_t>(*tok, "positions");

					orig.insert(word, positions);

					const char *stem = greylock::get_string(*tok, "stem");
					if (stem) {
						stemmed.insert(stem, positions);
					}
				}

				doc.idx.attributes.emplace_back(orig);
				doc.idx.attributes.emplace_back(stemmed);
			}

			return greylock::error_info();
		}

		greylock::error_info parse_docs(const std::string &mbox, const rapidjson::Value &docs) {
			greylock::error_info err = greylock::create_error(-ENOENT,
					"parse_docs: mbox: %s: could not parse document, there are no valid index entries", mbox.c_str());

			bool want_tokens = false;
			if (this->request().url().path().find("/index_tokens") == 0) {
				want_tokens = true;
			}

			for (auto it = docs.Begin(), id_end = docs.End(); it != id_end; ++it) {
				if (!it->IsObject()) {
					return greylock::create_error(-EINVAL, "docs entries must be objects");
				}

				const char *id = greylock::get_string(*it, "id");
				const char *data = greylock::get_string(*it, "data");
				const char *author = greylock::get_string(*it, "author");
				if (!id) {
					return greylock::create_error(-EINVAL, "id must be string");
				}

				struct timespec ts;

				const rapidjson::Value &timestamp = greylock::get_object(*it, "timestamp");
				if (timestamp.IsObject()) {
					long tsec, tnsec;

					tsec = greylock::get_int64(timestamp, "tsec", ts.tv_sec);
					tnsec = greylock::get_int64(timestamp, "tnsec", ts.tv_nsec);

					ts.tv_sec = tsec;
					ts.tv_nsec = tnsec;
				} else {
					clock_gettime(CLOCK_REALTIME, &ts);
				}


				greylock::document doc;
				doc.mbox = mbox;
				doc.ts = ts;

				doc.id.assign(id);
				if (data) {
					doc.data.assign(data);
				}
				if (author) {
					doc.author.assign(author);
				}

				const rapidjson::Value &ctx = greylock::get_object(*it, "content");
				if (ctx.IsObject()) {
					err = parse_content(ctx, doc);
					if (err)
						return err;
				}

				const rapidjson::Value &idxs = greylock::get_object(*it, "index");
				if (!idxs.IsObject()) {
					return greylock::create_error(-EINVAL, "docs/index must be array");
				}

				if (want_tokens) {
					err = parse_doc_tokens(doc, idxs);
					if (err)
						return err;
				} else {
					doc.idx = server()->get_indexes(idxs);
				}

				err = process_one_document(doc);
				if (err)
					return err;
			}

			return err;
		}

		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			(void) req;
			ribosome::timer index_tm;

			// this is needed to put ending zero-byte, otherwise rapidjson parser will explode
			std::string data(const_cast<char *>(boost::asio::buffer_cast<const char*>(buffer)),
					boost::asio::buffer_size(buffer));

			rapidjson::Document doc;
			doc.Parse<0>(data.c_str());

			if (doc.HasParseError()) {
				send_error(swarm::http_response::bad_request, -EINVAL,
						"index: could not parse document: %s, error offset: %d",
						doc.GetParseError(), doc.GetErrorOffset());
				return;
			}

			if (!doc.IsObject()) {
				send_error(swarm::http_response::bad_request, -EINVAL, "index: document must be object, its type: %d",
						doc.GetType());
				return;
			}

			const char *mbox = greylock::get_string(doc, "mailbox");
			if (!mbox) {
				send_error(swarm::http_response::bad_request, -ENOENT, "index: 'mailbox' must be a string");
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			const rapidjson::Value &docs = greylock::get_array(doc, "docs");
			if (!docs.IsArray()) {
				send_error(swarm::http_response::bad_request, -ENOENT, "index: mailbox: %s, 'docs' must be array", mbox);
				return;
			}

			greylock::error_info err = parse_docs(mbox, docs);
			if (err) {
				send_error(swarm::http_response::bad_request, err.code(),
						"index: mailbox: %s, keys: %d: insertion error: %s",
					mbox, docs.Size(), err.message());
				return;
			}

			ILOG_INFO("index: mailbox: %s, keys: %d: insertion completed, index duration: %d ms",
					mbox, docs.Size(), index_tm.elapsed());
			this->send_reply(thevoid::http_response::ok);
		}
	};

	greylock::indexes get_indexes(const rapidjson::Value &idxs) {
		greylock::indexes ireq;

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
				if (idx.size() >= options().ngram_index_size) {
					a.insert(ribosome::lconvert::to_string(idx), pos);
				} else {
					if (pos > 0) {
						auto &prev = indexes[pos - 1];
						a.insert(ribosome::lconvert::to_string(prev + idx), pos);
					}

					if (pos < avalue.Size() - 1) {
						auto &next = indexes[pos - 1];
						a.insert(ribosome::lconvert::to_string(idx + next), pos);
					}
				}
			}

			ireq.attributes.emplace_back(a);
		}

		return ireq;
	}

	greylock::database &db() {
		return m_db;
	}

private:
	std::mutex m_lock;
	greylock::database m_db;

	ribosome::expiration m_expiration_timer;

	void sync_metadata_callback() {
		if (db().meta.dirty) {
			db().sync_metadata(NULL);
			ILOG_INFO("Synced metadata, key: %s", db().opts.metadata_key.c_str());
		}

		auto expires_at = std::chrono::system_clock::now() +
			std::chrono::milliseconds(this->options().sync_metadata_timeout);
		m_expiration_timer.insert(expires_at, std::bind(&http_server::sync_metadata_callback, this));
	}

	bool rocksdb_init(const rapidjson::Value &config) {
		const auto &rconf = greylock::get_object(config, "rocksdb");
		if (!rconf.IsObject()) {
			ILOG_ERROR("there is no 'rocksdb' object in config");
			return false;
		}

		const char *path = greylock::get_string(rconf, "path");
		if (!path) {
			ILOG_ERROR("there is no 'path' string in rocksdb config");
			return false;
		}

		auto err = m_db.open(path);
		if (err) {
			ILOG_ERROR("could not open database: %s [%d]", err.message().c_str(), err.code());
			return false;
		}

		if (this->options().sync_metadata_timeout > 0) {
			sync_metadata_callback();
		}

		return true;
	}
};

int main(int argc, char **argv)
{
	ioremap::ribosome::set_locale("en_US.UTF8");

	ioremap::thevoid::register_signal_handler(SIGINT, ioremap::thevoid::handle_stop_signal);
	ioremap::thevoid::register_signal_handler(SIGTERM, ioremap::thevoid::handle_stop_signal);
	ioremap::thevoid::register_signal_handler(SIGHUP, ioremap::thevoid::handle_reload_signal);
	ioremap::thevoid::register_signal_handler(SIGUSR1, ioremap::thevoid::handle_ignore_signal);
	ioremap::thevoid::register_signal_handler(SIGUSR2, ioremap::thevoid::handle_ignore_signal);

	ioremap::thevoid::run_signal_thread();

	auto server = ioremap::thevoid::create_server<http_server>();
	int err = server->run(argc, argv);

	ioremap::thevoid::stop_signal_thread();

	return err;
}

