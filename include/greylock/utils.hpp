#pragma once

#include "greylock/error.hpp"

#include <string>
#include <sstream>
#include <vector>

#include <msgpack.hpp>

namespace ioremap { namespace greylock {

template <typename T>
std::string dump_vector(const std::vector<T> &vec) {
	std::ostringstream ss;
	for (size_t i = 0; i < vec.size(); ++i) {
		ss << vec[i];
		if (i != vec.size() - 1)
			ss << " ";
	}

	return ss.str();
}

template <typename T>
std::string dump_vector(const std::vector<T> &vec, std::function<std::string (const T &)> convert) {
	std::ostringstream ss;
	for (size_t i = 0; i < vec.size(); ++i) {
		ss << convert(vec[i]);
		if (i != vec.size() - 1)
			ss << " ";
	}

	return ss.str();
}

template <typename T>
greylock::error_info deserialize(T &t, const char *data, size_t size) {
	msgpack::unpacked msg;
	try {
		msgpack::unpack(&msg, data, size);

		msg.get().convert(&t);
	} catch (const std::exception &e) {
		std::ostringstream ss;
		ss << msg.get();
		return greylock::create_error(-EINVAL, "could not unpack data, size: %ld, value: %s, error: %s",
				size, ss.str().c_str(), e.what());
	}

	return greylock::error_info();
}

template <typename T>
std::string serialize(const T &t) {
	std::stringstream buffer;
	msgpack::pack(buffer, t);
	buffer.seekg(0);
	return buffer.str();
}

}} // namesapce ioremap::greylock
