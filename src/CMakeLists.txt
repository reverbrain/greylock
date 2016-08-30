add_library(greylock SHARED exception.cpp)
set_target_properties(greylock PROPERTIES
	VERSION ${GREYLOCK_MAJOR_VERSION}
	SOVERSION ${GREYLOCK_FULL_VERSION}
	LINKER_LANGUAGE CXX
)
target_link_libraries(greylock
	${Boost_LIBRARIES}
	${MSGPACK_LIBRARIES}
	${RIBOSOME_LIBRARIES}
	${ROCKSDB_LIBRARIES}
	${SWARM_LIBRARIES}
	${THEVOID_LIBRARIES}
	pthread
)
add_executable(greylock_server server.cpp)
target_link_libraries(greylock_server
	greylock
)

add_executable(greylock_meta meta.cpp)
target_link_libraries(greylock_meta
	greylock
)

install(TARGETS	greylock
	LIBRARY DESTINATION lib${LIB_SUFFIX}
	ARCHIVE DESTINATION lib${LIB_SUFFIX}
	BUNDLE DESTINATION library
)
install(TARGETS	greylock_server greylock_meta
	RUNTIME DESTINATION bin COMPONENT runtime
)

add_executable(greylock_merge merge.cpp)
target_link_libraries(greylock_merge
	greylock
)

add_executable(greylock_ljsearch ljsearch.cpp)
target_link_libraries(greylock_ljsearch
	greylock
	${WARP_LIBRARIES}
)
