d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc server.cc server_main.cc)

OBJS-meerkatstore-leader-client := $(OBJS-leadermeerkatir-client)             \
        $(LIB-transport)                                \
		$(LIB-fasttransport)                            \
        $(LIB-store-frontend)                           \
        $(LIB-store-common)                             \
		$(o)shardclient.o $(o)client.o

OBJS-meerkatstore-leader-server := $(LIB-transport)                  \
		$(LIB-fasttransport) $(OBJS-leadermeerkatir-replica)     \
		$(OBJS-meerkatstore) $(o)server.o

$(d)meerkat_server: $(OBJS-meerkatstore-leader-server) $(o)server_main.o

BINS += $(d)meerkat_server