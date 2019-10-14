d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc server.cc server_main.cc)

OBJS-meerkatstore-client := $(OBJS-meerkatir-client)             \
        $(LIB-transport)                                \
		$(LIB-fasttransport)                            \
        $(LIB-store-frontend)                           \
        $(LIB-store-common)                             \
		$(o)shardclient.o $(o)client.o

OBJS-meerkatstore-server := $(LIB-transport)                  \
		$(LIB-fasttransport) $(OBJS-meerkatir-replica)     \
		$(OBJS-meerkatstore) $(o)server.o

$(d)meerkat_server: $(OBJS-meerkatstore-server) $(o)server_main.o

BINS += $(d)meerkat_server