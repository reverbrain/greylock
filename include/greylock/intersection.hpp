#ifndef __INDEXES_INTERSECTION_HPP
#define __INDEXES_INTERSECTION_HPP

#include "greylock/iterator.hpp"
#include "greylock/types.hpp"

namespace ioremap { namespace greylock {

struct single_doc_result {
	document doc;

	float relevance = 0;
};

struct search_result {
	bool completed = true;

	// This will contain a cookie which must be used for the next intersection request,
	// if current request is not complete. This may happen when client has requested limited
	// maximum number of keys in reply and there are more keys.
	id_t next_document_id;
	long max_number_of_documents = ~0UL;

	// array of documents which contain all requested indexes
	std::vector<single_doc_result> docs;
};

// check whether given result matches query, may also set or change some result parameters like relevance field
typedef std::function<bool (single_doc_result &)> check_result_function_t;

struct mailbox_query {
	std::string mbox;
	greylock::indexes idx;

	greylock::error_info parse_error;

	mailbox_query(const greylock::options &options, const rapidjson::Value &doc) {
		const rapidjson::Value &query_and = greylock::get_object(doc, "query");
		if (query_and.IsObject()) {
			auto ireq = indexes::get_indexes(options, query_and);
			idx.merge_query(ireq);
		}

		const rapidjson::Value &query_exact = greylock::get_object(doc, "exact");
		if (query_exact.IsObject()) {
			auto ireq = indexes::get_indexes(options, query_exact);

			// merge these indexes into intersection set,
			// since exact phrase match implies document contains all tokens
			idx.merge_exact(ireq);
		}

		const rapidjson::Value &query_negation = greylock::get_object(doc, "negation");
		if (query_negation.IsObject()) {
			auto ireq = indexes::get_indexes(options, query_negation);
			// do not merge these indexes into intersection set, put them into own container
			idx.merge_negation(ireq);
		}

		if (idx.attributes.empty()) {
			parse_error = greylock::create_error(-ENOENT,
					"search: mailbox: %s, there are no queries suitable for search", mbox.c_str());
			return;
		}
	}
};

struct intersection_query {
	id_t range_start, range_end;

	std::vector<mailbox_query> se;

	id_t next_document_id;
	size_t max_number = LONG_MAX;

	std::string to_string() const {
		std::ostringstream ss;

		ss << "[ ";
		for (const auto &ent: se) {
			ss << "mailbox: " << ent.mbox << ", indexes: " << ent.idx.to_string() << "| ";
		}
		ss << "]";

		return ss.str();
	}
};

template <typename DBT>
class intersector {
public:
	intersector(DBT &db_docs, DBT &db_indexes) : m_db_docs(db_docs), m_db_indexes(db_indexes) {}

	search_result intersect(const intersection_query &iq) const {
		return intersect(iq, [&] (single_doc_result &) -> bool {
					return true;
				});
	}

	// search for intersections between all @indexes
	// starting with the key @start, returning at most @num entries
	//
	// after @intersect() completes, it sets @start to the next key to start searching from
	// user should not change that token, otherwise @intersect() may skip some entries or
	// return duplicates.
	//
	// if number of returned entries is less than requested number @num or if @start has been set to empty string
	// after call to this function returns, then intersection is completed.
	//
	// @search_result.completed will be set to true in this case.
	search_result intersect(const intersection_query &iq, check_result_function_t check) const {
		search_result res;
#ifdef STDOUT_DEBUG
				auto dump_vector = [] (const std::vector<size_t> &sh) -> std::string {
					std::ostringstream ss;
					for (size_t i = 0; i < sh.size(); ++i) {
						ss << sh[i];
						if (i != sh.size() - 1)
							ss << " ";
					}

					return ss.str();
				};

#endif


		std::vector<size_t> common_shards;
		bool init = true;
		for (const auto &ent: iq.se) {
			for (const auto &attr: ent.idx.attributes) {
				for (const auto &t: attr.tokens) {
					std::string shard_key = document::generate_shard_key(m_db_indexes.options(), ent.mbox, attr.name, t.name);
					auto shards = m_db_indexes.get_shards(shard_key);
#ifdef STDOUT_DEBUG
					printf("common_shards: %s, key: %s, shards: %s\n",
							dump_vector(common_shards).c_str(), shard_key.c_str(),
							dump_vector(shards).c_str());
#endif
					// one index is empty, intersection will be empty, return early
					if (shards.size() == 0) {
						return res;
					}

					if (init) {
						common_shards = shards;
						init = false;
					} else {
						std::vector<size_t> intersection;
						std::set_intersection(common_shards.begin(), common_shards.end(),
								shards.begin(), shards.end(),
								std::back_inserter(intersection));
						common_shards = intersection;
					}

					// intersection is empty, return early
					if (common_shards.size() == 0) {
						return res;
					}
				}
			}
		}

		struct iter {
			greylock::index_iterator<DBT> begin, end;

			iter(DBT &db, const std::string &mbox, const std::string &attr, const std::string &token,
					const std::vector<size_t> &shards) :
				begin(greylock::index_iterator<DBT>::begin(db, mbox, attr, token, shards)),
				end(greylock::index_iterator<DBT>::end(db, mbox, attr, token))
			{
			}
		};

		// contains vector of iterators pointing to the requested indexes
		// iterator always points to the smallest document ID not yet pushed into resulting structure (or to client)
		// or discarded (if other index iterators point to larger document IDs)
		std::vector<iter> idata;
		std::vector<iter> inegation;

		for (const auto &ent: iq.se) {
			for (const auto &attr: ent.idx.attributes) {
				for (const auto &t: attr.tokens) {
					iter itr(m_db_indexes, ent.mbox, attr.name, t.name, common_shards);

					if (iq.next_document_id != 0) {
						itr.begin.rewind_to_index(iq.next_document_id);
					} else {
						itr.begin.rewind_to_index(iq.range_start);
					}

					idata.emplace_back(itr);
				}
			}

			for (const auto &attr: ent.idx.negation) {
				for (const auto &t: attr.tokens) {
					std::string shard_key = document::generate_shard_key(m_db_indexes.options(), ent.mbox, attr.name, t.name);
					auto shards = m_db_indexes.get_shards(shard_key);
#ifdef STDOUT_DEBUG
					printf("negation: key: %s, shards: %s\n",
							shard_key.c_str(),
							dump_vector(shards).c_str());
#endif

					iter itr(m_db_indexes, ent.mbox, attr.name, t.name, shards);
					inegation.emplace_back(itr);
				}
			}
		}

		while (true) {
			// contains indexes within @idata array of iterators,
			// each iterator contains the same and smallest to the known moment reference to the document (i.e. document ID)
			//
			// if checking @idata array yelds smaller document ID than that in iterators referenced in @pos,
			// then we clear @pos and starts pushing the new smallest iterator indexes
			//
			// we could break out of the @idata processing, increase the smallest pointing iterator and start over,
			// but we optimize @idata processing - if there are other iterators in @idata which equal to the smallest
			// iterator value (document ID), we put them into @pos
			// Since @pos doesn't contain all indexes (its size doesn't equal to the size of @idata), we will increase
			// all iterators where we have found the smallest document ID, hopefully they will point to the new document ID,
			// which might be the same for all iterator among @idata and thus we will push this document ID to the result
			// structure returned to the client
			//
			// Here is an example:
			//
			// 1. @idata iterators	0	1	2	3
			//                      -------------------------
			// document ids		d0	d2	d3	d3
			// 			d2	d3	d4	d4
			// 			d3	d4	d5	d5
			// 			d4	-	-	-
			// 			d5	-	-	-
			//
			// We start from the top of this table, i.e. row after 'document ids' string
			// @pos will contain following values during iteration over @idata iterators
			// 	0 - select the first value
			// 	0 - skip iterator 1 (d2 document id) since its value is greater than that 0'th iterator value (d0)
			// 	0 - skip iterator 2
			// 	0 - skip iterator 3
			//
			// @pos contains only 0 index, it is not equal to the size of @idata (4), thus we have to increase 0'th iterator
			// discarding its first value
			//
			// 2. @idata iterators	0	1	2	3
			//                      -------------------------
			// document ids		d2	d2	d3	d3
			// 			d3	d3	d4	d4
			// 			d4	d4	d5	d5
			// 			d5	-	-	-
			// @pos:
			// 	0 - select the first iterator
			// 	0 1 - 1'th iterator value equals to the value of the 0'th iterator, append it to the array
			// 	0 1 - 2'th iterator value (d3) is greater than that of the 0'th iterator (d2)
			// 	0 1 - the same as above
			// since size of the @pos is not equal to the size of @idata we increment all iterators which are indexed in @pos
			//
			// 3. @idata iterators	0	1	2	3
			//                      -------------------------
			// document ids		d3	d3	d3	d3
			// 			d4	d4	d4	d4
			// 			d5	-	d5	d5
			// @pos will contain all 4 indexes, since all iterator's value are the same (d3)
			// We will increment all iterators and push d3 into resulting array which will be returned to the client,
			// since size of the @pos array equals to the @idata size
			//
			// 4. @idata iterators	0	1	2	3
			//                      -------------------------
			// document ids		d4	d4	d4	d4
			// 			d5	-	d5	d5
			// We put d4 into resulting array and increment all iterators as above
			//
			// 5. @idata iterators	0	1	2	3
			//                      -------------------------
			// document ids		d5	-	d5	d5
			//
			// @pos:
			// 	0 - select the first iterator
			// 	Stop processing, since 1'th iterator is empty.
			// 	This means no further iteration checks can contain all 4 the same value,
			// 	thus it is not possible to find any other document with higher ID
			// 	which will contain all 4 requested indexes.
			//
			// 6. Return [d3, d4] values to the client
			std::vector<pos_t> pos;

			id_t next_id;

			int current = -1;
			for (auto &itr: idata) {
				auto &it = itr.begin;
				auto &e = itr.end;
				++current;

				if (it == e) {
					res.completed = true;
					break;
				}

				if (it->indexed_id > iq.range_end) {
					res.completed = true;
					break;
				}

				res.completed = false;
				res.next_document_id.set_next_id(it->indexed_id);

				if (pos.size() == 0) {
					pos.push_back(current);
					continue;
				}

				auto &min_it = idata[pos[0]].begin;
#if 0
				BH_LOG(m_bp.logger(), INDEXES_LOG_INFO, "intersection: min-index: %s, id: %s, it-index: %s, id: %s",
						idata[pos[0]].idx.start_key().str(), min_it->str(),
						idata_it->idx.start_key().str(), it->str());
#endif
				if (it->indexed_id == min_it->indexed_id) {
					pos.push_back(current);
					continue;
				}

				next_id = std::max(it->indexed_id, min_it->indexed_id);
				res.next_document_id.set_next_id(next_id);

				pos.clear();
				break;
			}

			// this can only happen if one of the iterators has been finished,
			// which means number of found positions will not be equal to the number
			// of indexes to intersect, and thus there is no more data to push into result.
			// Just break out of the processing loop - nothing can be added anymore.
			if (res.completed) {
				break;
			}

			// number of entries with the same document ID doesn't match number of indexes,
			// this means some index doesn't have this docuement and thus it has to be skipped
			// and iteration check process has to be started over
			if (pos.size() != idata.size()) {
				for (auto &it: idata) {
					auto &min_it = it.begin;

					min_it.rewind_to_index(next_id);
				}

				continue;
			}

			auto &min_it = idata[pos.front()].begin;
			id_t indexed_id = min_it->indexed_id;

			bool negation_match = false;
			for (auto &neg: inegation) {
				auto &it = neg.begin;
				it.rewind_to_index(indexed_id);
				if (it != neg.end) {
					if (it->indexed_id == indexed_id) {
						negation_match = true;
						break;
					}
				}
			}

			auto increment_all_iterators = [&] () {
				for (auto it = pos.begin(); it != pos.end(); ++it) {
					auto &idata_iter = idata[*it].begin;
					++idata_iter;
				}
			};

			if (negation_match) {
				increment_all_iterators();
				continue;
			}

			single_doc_result rs;
			auto err = min_it.document(m_db_docs, &rs.doc);
			if (err) {
#if 0
				printf("could not read document id: %ld, err: %s [%d]\n",
						min_it->indexed_id, err.message().c_str(), err.code());
#endif
				increment_all_iterators();
				continue;
			}
			rs.doc.indexed_id = indexed_id;

			// increment all iterators
			increment_all_iterators();

			if (!check(rs)) {
				continue;
			}

			res.docs.emplace_back(rs);
			if (res.docs.size() == iq.max_number)
				break;
		}

		return res;
	}
private:
	DBT &m_db_docs;
	DBT &m_db_indexes;
};

}} // namespace ioremap::greylock

#endif // __INDEXES_INTERSECTION_HPP
