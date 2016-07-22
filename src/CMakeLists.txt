add_library(greylock STATIC exception.cpp)
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

install(TARGETS	greylock_server greylock_meta
	RUNTIME DESTINATION bin COMPONENT runtime)

