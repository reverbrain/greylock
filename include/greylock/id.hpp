#pragma once

#include <string>

#include <stdint.h>
#include <string.h>

#include <msgpack.hpp>

namespace ioremap { namespace greylock {

struct id_t {
	uint64_t	timestamp = 0;
	uint64_t	seq = 0;

	MSGPACK_DEFINE(timestamp);

	void set_timestamp(long tsec, long nsec) {
		timestamp = tsec;
		timestamp <<= 30;
		timestamp |= nsec & ((1<<30) - 1);
	}

	void get_timestamp(long *tsec, long *nsec) const {
		*tsec = timestamp >> 30;
		*nsec = timestamp & ((1<<30) - 1);
	}

	bool operator<(const id_t &other) const {
		if (timestamp < other.timestamp)
			return true;
		if (timestamp > other.timestamp)
			return false;

		return seq < other.seq;
	}
	bool operator>(const id_t &other) const {
		if (timestamp > other.timestamp)
			return true;
		if (timestamp < other.timestamp)
			return false;

		return seq > other.seq;
	}

	bool operator==(const id_t &other) const {
		return ((timestamp == other.timestamp) && (seq == other.seq));
	}
	bool operator!=(const id_t &other) const {
		return !operator==(other);
	}

	std::string to_string() const {
		char buf[64];
		size_t sz = snprintf(buf, sizeof(buf), "%lx.%lx", timestamp, seq);
		return std::string(buf, sz);
	}

	id_t(): timestamp(0), seq(0) {
	}

	id_t(const id_t &other) {
		seq = other.seq;
		timestamp = other.timestamp;
	}

	id_t(const char *str) {
		if (!str) {
			id_t();
			return;
		}

		auto ptr = strchr(str, '.');
		if (ptr != NULL) {
			char tmp[64];
			snprintf(tmp, sizeof(tmp), "%*s", (int)(ptr - str), str);
			timestamp = strtoull(tmp, NULL, 16);
			seq = strtoull(ptr+1, NULL, 16);
		}
	}

	void set_next_id(const id_t &other) {
		long tsec, tnsec;
		other.get_timestamp(&tsec, &tnsec);
		tnsec += 1;

		set_timestamp(tsec, tnsec);
		seq = other.seq;
	}

};

}} // namespace ioremap::greylock
