d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc \
	server.cc store.cc server_main.cc)

PROTOS += $(addprefix $(d), multitapir-proto.proto)

OBJS-multitapir-store := $(LIB-message) $(LIB-store-common) $(LIB-store-backend) \
	$(o)multitapir-proto.o $(o)store.o 

OBJS-multitapir-client := $(OBJS-ir-client)             \
        $(LIB-transport)                                \
		$(LIB-fasttransport)                            \
        $(LIB-store-frontend)                           \
        $(LIB-store-common) $(o)multitapir-proto.o      \
		$(o)shardclient.o $(o)client.o

OBJS-multitapir-server := $(LIB-transport)              \
		$(LIB-fasttransport) $(OBJS-ir-replica)         \
		$(OBJS-multitapir-store) $(o)server.o

$(d)meerkat_server: $(OBJS-multitapir-server) $(o)server_main.o

BINS += $(d)meerkat_server
