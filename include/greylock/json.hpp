#ifndef __INDEXES_JSON_HPP
#define __INDEXES_JSON_HPP

#include <thevoid/rapidjson/document.h>

#include <stdint.h>

namespace ioremap { namespace greylock {

static inline const char *get_string(const rapidjson::Value &entry, const char *name, const char *def = NULL) {
	if (entry.HasMember(name)) {
		const rapidjson::Value &v = entry[name];
		if (v.IsString()) {
			return v.GetString();
		}
	}

	return def;
}

static inline int64_t get_int64(const rapidjson::Value &entry, const char *name, int64_t def = -1) {
	if (entry.HasMember(name)) {
		const rapidjson::Value &v = entry[name];
		if (v.IsInt()) {
			return v.GetInt();
		}
		if (v.IsUint()) {
			return v.GetUint();
		}
		if (v.IsInt64()) {
			return v.GetInt64();
		}
		if (v.IsUint()) {
			return v.GetUint64();
		}
	}

	return def;
}

static inline const rapidjson::Value &get_object(const rapidjson::Value &entry, const char *name,
		const rapidjson::Value &def = rapidjson::Value()) {
	if (entry.HasMember(name)) {
		const rapidjson::Value &v = entry[name];

		if (v.IsObject())
			return v;
	}

	return def;
}

static inline const rapidjson::Value &get_array(const rapidjson::Value &entry, const char *name,
		const rapidjson::Value &def = rapidjson::Value()) {
	if (entry.HasMember(name)) {
		const rapidjson::Value &v = entry[name];

		if (v.IsArray())
			return v;
	}

	return def;
}

static inline bool get_bool(const rapidjson::Value &entry, const char *name, bool def = true) {
	if (entry.HasMember(name)) {
		const rapidjson::Value &v = entry[name];

		if (v.IsBool())
			return v.GetBool();
	}

	return def;
}

}} // namespace ioremap::greylock

#endif // __INDEXES_JSON_HPP
