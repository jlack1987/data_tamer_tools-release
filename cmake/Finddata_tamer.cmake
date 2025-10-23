# Finddata_tamer.cmake - Find the Data Tamer library and its dependencies

# Look for the Data Tamer configuration file
find_package(PkgConfig)

# Try to locate the config files installed by Data Tamer
find_path(data_tamer_INCLUDE_DIR
  NAMES data_tamer/data_tamer.hpp
  PATHS /usr/local/include
)

find_library(data_tamer_LIBRARY
  NAMES data_tamer
  PATHS /usr/local/lib
)

find_library(MCAP_LIBRARY
  NAMES mcap_lib
  PATHS /usr/local/lib
)

# Load the CMake targets if the config files exist
find_package(data_tamer CONFIG)

# Check if all the components were found
if (data_tamer_INCLUDE_DIR AND data_tamer_LIBRARY AND MCAP_LIBRARY)
  set(data_tamer_FOUND TRUE)
  set(data_tamer_LIBRARIES ${data_tamer_LIBRARY} ${MCAP_LIBRARY})
  set(data_tamer_INCLUDE_DIRS ${data_tamer_INCLUDE_DIR})

  message(STATUS "Found Data Tamer: ${data_tamer_INCLUDE_DIR}")

else()
  set(data_tamer_FOUND FALSE)
  message(WARNING "Could not find Data Tamer")
endif()

# Mark variables as advanced
mark_as_advanced(data_tamer_INCLUDE_DIR data_tamer_LIBRARY MCAP_LIBRARY)

# Provide target if found
if (data_tamer_FOUND)
  add_library(data_tamer::data_tamer UNKNOWN IMPORTED)
  set_target_properties(data_tamer::data_tamer PROPERTIES
    IMPORTED_LOCATION "${data_tamer_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${data_tamer_INCLUDE_DIR}"
  )
endif()
