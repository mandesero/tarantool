add_library(ann STATIC ann_numeric.c ann_flat.c ann_registry.c)
target_link_libraries(ann m)

add_library(ann_usearch STATIC ann_usearch.cc)
target_compile_features(ann_usearch PRIVATE cxx_std_17)
target_include_directories(ann_usearch SYSTEM PRIVATE
                           ${PROJECT_SOURCE_DIR}/third_party/USearch/include)
target_link_libraries(ann_usearch ann)
