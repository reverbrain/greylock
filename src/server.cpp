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

	struct on_ping : public thevoid::simple_request_stream<http_server> {
		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			(void) buffer;
			(void) req;

			this->send_reply(thevoid::http_response::ok);
		}
	};

	struct on_compact : public thevoid::simple_request_stream<http_server> {
		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			(void) req;
			(void) buffer;

			server()->db().compact();
			this->send_reply(thevoid::http_response::ok);
		}
	};

	struct on_search : public thevoid::simple_request_stream<http_server> {
		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			ribosome::timer search_tm;

			// this is needed to put ending zero-byte, otherwise rapidjson parser will explode
			std::string data(const_cast<char *>(boost::asio::buffer_cast<const char*>(buffer)),
					boost::asio::buffer_size(buffer));

			rapidjson::Document doc;
			doc.Parse<0>(data.c_str());

			if (doc.HasParseError()) {
				ILOG_ERROR("on_request: url: %s, error: %d: could not parse document: %s, error offset: %d",
						req.url().to_human_readable().c_str(), -EINVAL, doc.GetParseError(), doc.GetErrorOffset());
				this->send_reply(swarm::http_response::bad_request);
				return;
			}


			if (!doc.IsObject()) {
				ILOG_ERROR("on_request: url: %s, error: %d: document must be object",
						req.url().to_human_readable().c_str(), -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			const char *mbox = greylock::get_string(doc, "mailbox");
			if (!mbox) {
				ILOG_ERROR("on_request: url: %s, error: %d: 'mailbox' must be a string",
						req.url().to_human_readable().c_str(), -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}


			const rapidjson::Value &query = greylock::get_object(doc, "query");
			if (!query.IsObject()) {
				ILOG_ERROR("on_request: url: %s, mailbox: %s, error: %d: 'query' must be object",
						req.url().to_human_readable().c_str(), mbox, -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			size_t shard_offset = 0;
			size_t max_number = ~0UL;

			if (doc.HasMember("paging")) {
				const auto &pages = doc["paging"];
				shard_offset = greylock::get_int64(pages, "shard_offset", 0);
				max_number = greylock::get_int64(pages, "max_number", ~0UL);
			}

			const rapidjson::Value &match = greylock::get_object(doc, "match");
			if (match.IsObject()) {
				const char *match_type = greylock::get_string(match, "type");
				if (match_type) {
					if (strcmp(match_type, "and")) {
					} else if (strcmp(match_type, "phrase")) {
					}
				}
			}

			auto ireq = server()->get_indexes(query);

			greylock::search_result result;
			greylock::intersector<greylock::database> inter(server()->db());
			result = inter.intersect(mbox, ireq, shard_offset, max_number);
			send_search_result(mbox, result);

			ILOG_INFO("search: attributes: %ld, shard_offset: %ld, max_number: %ld, "
					"indexes: %ld, duration: %d ms",
					ireq.attributes.size(),
					shard_offset, max_number, result.docs.size(),
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

	struct on_index : public thevoid::simple_request_stream<http_server> {
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

		std::vector<std::string> get_string_vector(const rapidjson::Value &data, const char *name) {
			std::vector<std::string> ret;
			const auto &arr = greylock::get_array(data, name);
			if (!arr.IsArray())
				return ret;

			for (auto it = arr.Begin(), end = arr.End(); it != end; it++) {
				if (it->IsString())
					ret.push_back(it->GetString());
			}

			return ret;
		}
		greylock::error_info parse_content(const rapidjson::Value &ctx, greylock::document &doc) {
			doc.ctx.content = get_string_vector(ctx, "content");
			doc.ctx.title = get_string_vector(ctx, "title");
			doc.ctx.links = get_string_vector(ctx, "links");
			doc.ctx.images = get_string_vector(ctx, "images");

			return greylock::error_info();
		}

		greylock::error_info parse_docs(const std::string &mbox, const rapidjson::Value &docs) {
			greylock::error_info err = greylock::create_error(-ENOENT,
					"parse_docs: mbox: %s: could not parse document, there are no valid index entries", mbox.c_str());

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

				doc.idx = server()->get_indexes(idxs);

				err = process_one_document(doc);
				if (err)
					return err;
			}

			return err;
		}

		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			ribosome::timer index_tm;
			ILOG_INFO("url: %s: start", req.url().to_human_readable().c_str());

			// this is needed to put ending zero-byte, otherwise rapidjson parser will explode
			std::string data(const_cast<char *>(boost::asio::buffer_cast<const char*>(buffer)),
					boost::asio::buffer_size(buffer));

			rapidjson::Document doc;
			doc.Parse<0>(data.c_str());

			if (doc.HasParseError()) {
				ILOG_ERROR("on_request: error: %d: could not parse document: %s, error offset: %d",
						-EINVAL, doc.GetParseError(), doc.GetErrorOffset());
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			if (!doc.IsObject()) {
				ILOG_ERROR("on_request: error: %d: document must be object, its type: %d", -EINVAL, doc.GetType());
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			const char *mbox = greylock::get_string(doc, "mailbox");
			if (!mbox) {
				ILOG_ERROR("on_request: error: %d: 'mailbox' must be a string", -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			const rapidjson::Value &docs = greylock::get_array(doc, "docs");
			if (!docs.IsArray()) {
				ILOG_ERROR("on_request: mailbox: %s, error: %d: 'docs' must be array", mbox, -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			greylock::error_info err = parse_docs(mbox, docs);
			if (err) {
				ILOG_ERROR("on_request: mailbox: %s, keys: %d: insertion error: %s [%d]",
					mbox, docs.Size(), err.message(), err.code());
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			ILOG_INFO("on_request: mailbox: %s, keys: %d: insertion completed, index duration: %d ms",
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

