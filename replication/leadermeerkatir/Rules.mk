d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	    replica.cc client.cc)

OBJS-leadermeerkatir-client := $(o)client.o  \
                   $(OBJS-replication-common) $(LIB-message) \
                   $(LIB-configuration)

OBJS-leadermeerkatir-replica := $(o)replica.o \
                   $(OBJS-replication-common) $(LIB-message) \
                   $(LIB-configuration)

