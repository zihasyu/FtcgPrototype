add_executable(Ftcg main.cc)
target_link_libraries(Ftcg ${FINAL_OBJ})

find_package(Threads REQUIRED)

target_link_libraries(Ftcg ${CMAKE_THREAD_LIBS_INIT})