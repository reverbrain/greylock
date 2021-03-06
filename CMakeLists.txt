cmake_minimum_required (VERSION 2.6)
project (greylock)

FILE (READ "${CMAKE_CURRENT_SOURCE_DIR}/debian/changelog" DEBCHANGELOG)

string(REGEX MATCH "([0-9]+\\.[0-9]+\\.[0-9]+)" DEBFULLVERSION "${DEBCHANGELOG}")
STRING (REGEX MATCH "([0-9]+\\.[0-9]+)" GREYLOCK_MAJOR_VERSION "${DEBFULLVERSION}")
SET(GREYLOCK_FULL_VERSION ${DEBFULLVERSION})

set(CMAKE_CXX_FLAGS "-g -std=c++0x -W -Wall -Wextra -fstack-protector-all")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")

find_package(Boost REQUIRED COMPONENTS system program_options filesystem thread)
find_package(Ribosome REQUIRED)

INCLUDE(cmake/locate_library.cmake)

LOCATE_LIBRARY(JEMALLOC "jemalloc/jemalloc.h" "jemalloc")
LOCATE_LIBRARY(MSGPACK "msgpack.hpp" "msgpack")
LOCATE_LIBRARY(THEVOID "thevoid/server.hpp" "thevoid")
LOCATE_LIBRARY(SWARM "swarm/http_request.hpp" "swarm")
LOCATE_LIBRARY(ROCKSDB "rocksdb/db.h" "rocksdb")

FILE(GLOB headers
	"${CMAKE_CURRENT_SOURCE_DIR}/include/greylock/*.hpp"
	"${CMAKE_CURRENT_SOURCE_DIR}/include/greylock/*.h"

)
install(FILES ${headers} DESTINATION include/greylock)

configure_file(cmake/GreylockConfig.cmake.in "${PROJECT_BINARY_DIR}/cmake/GreylockConfig.cmake" @ONLY)
install(FILES "${PROJECT_BINARY_DIR}/cmake/GreylockConfig.cmake" DESTINATION share/greylock/cmake)

include_directories(${PROJECT_SOURCE_DIR}/include
	${Boost_INCLUDE_DIRS}
	${MSGPACK_INCLUDE_DIRS}
	${RIBOSOME_INCLUDE_DIRS}
	${ROCKSDB_INCLUDE_DIRS}
	${SWARM_INCLUDE_DIRS}
	${THEVOID_INCLUDE_DIRS}
)

add_subdirectory(src)
