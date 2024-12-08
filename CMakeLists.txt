cmake_minimum_required (VERSION 3.5)

project (SeastarExample)

find_package (Seastar REQUIRED)

add_executable (sort
    external_sort.cc)

target_link_libraries (sort
  PRIVATE Seastar::seastar)