d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
		client.cc replica.cc)

OBJS-meerkatir-client :=  $(o)client.o                  \
                   $(OBJS-replication-common) $(LIB-message) \
                   $(LIB-configuration)

OBJS-meerkatir-replica :=  $(o)replica.o     \
                   $(OBJS-replication-common) $(LIB-message) \
                   $(LIB-configuration)

