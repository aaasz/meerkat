d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc server.cc store.cc server_main.cc)

OBJS-silostore := $(LIB-message) $(LIB-store-common) $(LIB-store-backend) \
	$(o)store.o

OBJS-silostore-client := $(OBJS-leadermeerkatir-client) \
		$(LIB-transport)                        \
		$(LIB-fasttransport)                    \
		$(LIB-store-frontend)                   \
		$(LIB-store-common)                     \
		$(o)shardclient.o $(o)client.o

OBJS-silostore-server := $(LIB-transport)               \
		$(LIB-fasttransport)                    \
		$(OBJS-leadermeerkatir-replica)         \
		$(OBJS-silostore) $(o)server.o

$(d)silo_server:  $(OBJS-silostore-server)  $(o)server_main.o

BINS += $(d)silo_server
