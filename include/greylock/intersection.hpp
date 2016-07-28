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
	std::string cookie;
	long max_number_of_documents = ~0UL;

	// array of documents which contain all requested indexes
	std::vector<single_doc_result> docs;
};

template <typename DBT>
class intersector {
public:
	intersector(DBT &db) : m_db(db) {}

	search_result intersect(const std::string &mbox, const greylock::indexes &indexes, size_t shard_offset, size_t max_num) const {
		return intersect(mbox, indexes, shard_offset, max_num,
				[&] (const greylock::indexes &, search_result &) {return true;});
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
	search_result intersect(const std::string &mbox, const greylock::indexes &indexes, size_t shard_offset, size_t max_num,
			const std::function<bool (const greylock::indexes &, search_result &)> &finish) const {
		search_result res;

		std::vector<size_t> common_shards;
		bool init = true;
		for (const auto &attr: indexes.attributes) {
			for (const auto &t: attr.tokens) {
				std::string shard_key = metadata::generate_shard_key(m_db.opts, mbox, attr.name, t.name);
				auto shards = m_db.get_shards(shard_key);

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

		for (const auto &attr: indexes.attributes) {
			for (const auto &t: attr.tokens) {
				iter itr(m_db, mbox, attr.name, t.name, common_shards);
				idata.emplace_back(itr);
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

			document::id_t next_id;

			int current = -1;
			for (auto &itr: idata) {
				auto &it = itr.begin;
				auto &e = itr.end;
				++current;

				if (it == e) {
					res.completed = true;
					break;
				}

				res.completed = false;

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

				pos.clear();
				break;
			}

			if (res.completed) {
				if (!finish(indexes, res))
					continue;
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

			if (res.docs.size() == max_num) {
				if (!finish(indexes, res))
					continue;
				break;
			}

			if (shard_offset > 0) {
				shard_offset--;
				continue;
			}

			single_doc_result rs;
			auto &min_it = idata[pos.front()].begin;
			auto err = min_it.document(&rs.doc);
			if (err) {
#if 0
				printf("could not read document id: %ld, err: %s [%d]\n",
						min_it->indexed_id, err.message().c_str(), err.code());
#endif
				continue;
			}
			rs.doc.indexed_id = min_it->indexed_id;

			// increment all iterators
			for (auto it = pos.begin(); it != pos.end(); ++it) {
				auto &idata_iter = idata[*it].begin;

				++idata_iter;
			}

			res.docs.emplace_back(rs);
		}

		return res;
	}
private:
	DBT &m_db;
};

}} // namespace ioremap::greylock

#endif // __INDEXES_INTERSECTION_HPP
