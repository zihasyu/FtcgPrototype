add_executable(Ftcg main.cc)
find_package(Threads REQUIRED)
target_link_libraries(Ftcg ${FINAL_OBJ} xdelta3 ${CMAKE_THREAD_LIBS_INIT})


