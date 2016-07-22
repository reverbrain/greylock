#pragma once

#include <stdexcept>
#include <string>

namespace ioremap { namespace greylock {

class error : public std::exception
{
	public:
		// err must be negative value
		explicit error(int err, const std::string &message) throw();
		~error() throw() {}

		int error_code() const;

		virtual const char *what() const throw();

		std::string error_message() const throw();

	private:
		int m_errno;
		std::string m_message;
};

class not_found_error : public error
{
	public:
		explicit not_found_error(const std::string &message) throw();
};

class timeout_error : public error
{
	public:
		explicit timeout_error(const std::string &message) throw();
};

class no_such_address_error : public error
{
	public:
		explicit no_such_address_error(const std::string &message) throw();
};

class error_info
{
	public:
		inline error_info() : m_code(0) {}
		inline error_info(int code, const std::string &&message)
			: m_code(code), m_message(message) {}
		inline error_info(int code, const std::string &message)
			: m_code(code), m_message(message) {}
		inline ~error_info() {}

		inline int code() const { return m_code; }
		inline const std::string &message() const { return m_message; }
		inline operator bool() const { return m_code != 0; }
		inline bool operator !() const { return !operator bool(); }
		operator int() const = delete; // disable implicit cast to int

		void throw_error() const;
	private:
		int m_code;
		std::string m_message;
};

// err must be negative value
void throw_error(int err, const char *format, ...)
	__attribute__ ((format (printf, 2, 3)));

// err must be negative value
error_info create_error(int err, const char *format, ...)
	__attribute__ ((format (printf, 2, 3)));

}} /* namespace ioremap::greylock */
