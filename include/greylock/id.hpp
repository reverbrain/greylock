#pragma once

#include <string>

#include <stdint.h>
#include <string.h>

#include <msgpack.hpp>

namespace ioremap { namespace greylock {

namespace {
	static const uint32_t start_date = 946674000; // 2000-01-01
	static const uint32_t date_div = 3600 * 24;
}

struct id_t {
	uint64_t	timestamp = 0;

	MSGPACK_DEFINE(timestamp);

	void set_timestamp(long tsec, long aux) {
		tsec = (tsec - start_date) / date_div;

		timestamp = tsec << 32;
		timestamp |= aux & ((1UL << 32) - 1);
	}

	void get_timestamp(long *tsec, long *aux) const {
		*tsec = (timestamp >> 32) * date_div + start_date;
		*aux = timestamp & ((1UL << 32) - 1);
	}

	bool operator<(const id_t &other) const {
		return timestamp < other.timestamp;
	}
	bool operator>(const id_t &other) const {
		return timestamp > other.timestamp;
	}

	bool operator==(const id_t &other) const {
		return (timestamp == other.timestamp);
	}
	bool operator!=(const id_t &other) const {
		return !operator==(other);
	}

	std::string to_string() const {
		char buf[64];
		size_t sz = snprintf(buf, sizeof(buf), "%lx", timestamp);
		return std::string(buf, sz);
	}

	id_t(): timestamp(0) {
	}

	id_t(const id_t &other) {
		timestamp = other.timestamp;
	}

	id_t(const char *str) {
		if (!str) {
			id_t();
			return;
		}

		timestamp = strtoull(str, NULL, 16);
	}

	void set_next_id(const id_t &other) {
		timestamp = other.timestamp + 1;
	}

};

}} // namespace ioremap::greylock
