d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), record.cc client.cc replica.cc)

OBJS-tapirir-client :=  \
	$(o)client.o \
    $(OBJS-client) $(LIB-message) \
    $(LIB-configuration)

OBJS-tapirir-replica := \
	$(o)record.o $(o)replica.o \
    $(OBJS-replica) $(LIB-message) \
    $(LIB-configuration)
