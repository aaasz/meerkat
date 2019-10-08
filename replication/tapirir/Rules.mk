d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), record.cc client.cc replica.cc)
SRCS += $(addprefix $(d), client.cc)

OBJS-tapirir-client :=  \
	$(o)ir-proto.o $(o)client.o \
    $(OBJS-client) $(LIB-message) \
    $(LIB-configuration)

OBJS-tapirir-replica := \
	$(o)record.o $(o)replica.o $(o)ir-proto.o \
    $(OBJS-replica) $(LIB-message) \
    $(LIB-configuration) $(LIB-persistent_register)
