# FindFoxgloveWebSocket.cmake
# This module locates the Foxglove WebSocket library and headers.

find_path(FoxgloveWebSocket_INCLUDE_DIR
  NAMES foxglove/websocket/base64.hpp
  PATHS /usr/local/include
)

find_library(FoxgloveWebSocket_LIBRARY
  NAMES foxglove_websocket
  PATHS /usr/local/lib
)

if(FoxgloveWebSocket_INCLUDE_DIR AND FoxgloveWebSocket_LIBRARY)
  set(FoxgloveWebSocket_FOUND TRUE)
else()
  set(FoxgloveWebSocket_FOUND FALSE)
endif()

if(FoxgloveWebSocket_FOUND)
  set(FoxgloveWebSocket_INCLUDE_DIRS ${FoxgloveWebSocket_INCLUDE_DIR})
  set(FoxgloveWebSocket_LIBRARIES ${FoxgloveWebSocket_LIBRARY})

  message(STATUS "Found Foxglove WebSocket: ${FoxgloveWebSocket_LIBRARY}")
else()
  message(WARNING "Could not find Foxglove WebSocket")
endif()

mark_as_advanced(FoxgloveWebSocket_INCLUDE_DIR FoxgloveWebSocket_LIBRARY)
