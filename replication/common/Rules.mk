d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	record.cc)

OBJS-replication-common := $(o)record.o
