add_executable(greylock_server server.cpp exception.cpp)
target_link_libraries(greylock_server
	${Boost_LIBRARIES}
	${MSGPACK_LIBRARIES}
	${RIBOSOME_LIBRARIES}
	${ROCKSDB_LIBRARIES}
	${SWARM_LIBRARIES}
	${THEVOID_LIBRARIES}
	pthread
)

install(TARGETS	greylock_server
	RUNTIME DESTINATION bin COMPONENT runtime)

