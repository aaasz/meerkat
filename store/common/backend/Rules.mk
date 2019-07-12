d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
				kvstore.cc lockserver.cc txnstore.cc versionstore.cc \
				pthread_kvs.cc atomic_kvs.cc)

LIB-store-backend := $(o)kvstore.o $(o)lockserver.o $(o)txnstore.o \
					 $(o)versionstore.o $(o)pthread_kvs.o $(o)atomic_kvs.o

include $(d)tests/Rules.mk
