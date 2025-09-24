cmake_minimum_required(VERSION 3.10)
project(RiskAnalysisServer)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找依赖库
find_package(PkgConfig REQUIRED)
pkg_check_modules(MYSQL REQUIRED mysqlclient)

# 包含目录
include_directories(${MYSQL_INCLUDE_DIRS})

# 查找httplib
find_path(HTTPLIB_INCLUDE_DIR httplib.h)
if(NOT HTTPLIB_INCLUDE_DIR)
    message(FATAL_ERROR "httplib not found. Please install httplib or provide the include path.")
endif()

# 查找jsoncpp
find_package(PkgConfig REQUIRED)
pkg_check_modules(JSONCPP REQUIRED jsoncpp)

# 包含目录
include_directories(${HTTPLIB_INCLUDE_DIR})
include_directories(${JSONCPP_INCLUDE_DIRS})

# 源文件
set(SOURCES
    risk_analysis_server.cpp
)

# 创建可执行文件
add_executable(risk_analysis_server ${SOURCES})

# 链接库
target_link_libraries(risk_analysis_server 
    ${MYSQL_LIBRARIES}
    ${JSONCPP_LIBRARIES}
    pthread
)

# 编译选项
target_compile_options(risk_analysis_server PRIVATE ${MYSQL_CFLAGS_OTHER})

# 安装规则
install(TARGETS risk_analysis_server DESTINATION bin)











