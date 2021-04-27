add_executable(basic_test basic_test.cpp)

add_executable(persistence_test persistence_test.cpp)

add_executable(storage_test storage_test.cpp)

add_executable(query_test query_test.cpp)

target_link_libraries(basic_test PUBLIC templatedb gtest)
target_link_libraries(persistence_test PUBLIC templatedb gtest)
target_link_libraries(storage_test PUBLIC templatedb gtest)
target_link_libraries(query_test PUBLIC templatedb gtest)

add_test(NAME basic_test COMMAND ${CMAKE_BINARY_DIR}/tests/basic_test)
add_test(NAME persistence_test COMMAND ${CMAKE_BINARY_DIR}/tests/persistence_test)
add_test(NAME storage_test COMMAND ${CMAKE_BINARY_DIR}/tests/storage_test)
add_test(NAME query_test COMMAND ${CMAKE_BINARY_DIR}/tests/query_test)
