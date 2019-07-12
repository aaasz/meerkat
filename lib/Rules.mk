d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	lookup3.cc message.cc memory.cc transport.cc \
	fasttransport.cc latency.cc configuration.cc)

LIB-hash := $(o)lookup3.o

LIB-message := $(o)message.o $(LIB-hash)

LIB-hashtable := $(LIB-hash) $(LIB-message)

LIB-memory := $(o)memory.o

LIB-configuration := $(o)configuration.o $(LIB-message)

LIB-transport := $(o)transport.o $(LIB-message) $(LIB-configuration)

LIB-fasttransport := $(o)fasttransport.o $(LIB-transport)