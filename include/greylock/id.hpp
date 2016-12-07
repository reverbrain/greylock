#pragma once

#include <string>
#include <sstream>

#include <stdint.h>
#include <string.h>

#include <msgpack.hpp>

#include <time.h>

namespace ioremap { namespace greylock {

namespace {
	static const uint32_t start_date = 0;
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
		size_t sz = snprintf(buf, sizeof(buf), "%016lx", timestamp);
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

std::string print_id(const greylock::id_t &id) {
	long tsec, aux;
	id.get_timestamp(&tsec, &aux);

	std::ostringstream ss;
	ss << id.to_string() <<
		", raw_ts: " << id.timestamp <<
		", aux: " << aux <<
		", ts: " << print_time(tsec, 0);
	return ss.str();
}

}} // namespace ioremap::greylock
