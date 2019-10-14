d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), store.cc)

OBJS-meerkatstore := $(LIB-message) $(LIB-store-common) $(LIB-store-backend) \
	$(o)store.o