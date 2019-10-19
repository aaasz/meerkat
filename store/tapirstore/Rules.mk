d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc \
						  server.cc store.cc server_main.cc)

OBJS-tapir-store := $(LIB-message) \
	                $(LIB-store-common) \
					$(LIB-store-backend) \
				    $(o)store.o

OBJS-tapir-client := $(OBJS-tapirir-client) \
	                 $(LIB-transport) \
					 $(LIB-fasttransport) \
					 $(LIB-store-frontend) \
					 $(LIB-store-common) \
					 $(o)shardclient.o $(o)client.o

OBJS-tapir-server := $(LIB-transport) \
					 $(LIB-fasttransport) \
					 $(OBJS-tapirir-replica) \
		             $(OBJS-tapir-store) \
					 $(o)server.o

$(d)tapir_server: $(OBJS-tapir-server) $(o)server_main.o

BINS += $(d)tapir_server
