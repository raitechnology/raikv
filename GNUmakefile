# defines a directory for build, for example, RH6_x86_64
lsb_dist     := $(shell if [ -x /usr/bin/lsb_release ] ; then lsb_release -is ; else echo Linux ; fi)
lsb_dist_ver := $(shell if [ -x /usr/bin/lsb_release ] ; then lsb_release -rs | sed 's/[.].*//' ; fi)
uname_m      := $(shell uname -m)

short_dist := $(patsubst CentOS,RH,$(patsubst RedHat,RH,\
                $(patsubst Fedora,FC,$(patsubst Ubuntu,UB,\
		  $(patsubst Debian,DEB,$(patsubst SUSE,SS,$(lsb_dist)))))))

# this is where the targets are compiled
port    := $(short_dist)$(lsb_dist_ver)_$(uname_m)$(port_extra)
bind    := $(port)/bin
libd    := $(port)/lib
objd    := $(port)/obj
dependd := $(port)/depend

# use 'make port_extra=-g' for debug build
ifeq (-g,$(findstring -g,$(port_extra)))
  DEBUG = true
endif

# the compiler and linker
cc         := gcc
cpp        := g++ -fno-rtti -fno-exceptions -fno-omit-frame-pointer
cpplink    := gcc
gcc_wflags := -Wall -Werror -pedantic

ifdef DEBUG
cflags   := $(gcc_wflags) -ggdb
else
cflags   := $(gcc_wflags) -O3
endif

includes    := -Isrc
defines     :=
#cpp_lnk     := -lsupc++
sock_lib    :=
math_lib    := -lm
thread_lib  := -pthread -lrt
malloc_lib  :=
#dynlink_lib := -ldl

# targets filled in below
all_exes    :=
all_libs    :=
all_dlls    :=
all_depends :=

# first target everything, target all: is at the end, after all_* are defined
.PHONY: everything
everything: all

libraikv_objs := $(objd)/key_ctx.o $(objd)/ht_linear.o \
                 $(objd)/ht_cuckoo.o $(objd)/key_hash.o \
		 $(objd)/msg_ctx.o $(objd)/ht_stats.o \
		 $(objd)/ht_init.o $(objd)/util.o \
		 $(objd)/rela_ts.o $(objd)/radix_sort.o
libraikv_deps := $(dependd)/key_ctx.d $(dependd)/ht_linear.d \
                 $(dependd)/ht_cuckoo.d $(dependd)/key_hash.d \
		 $(dependd)/msg_ctx.d $(dependd)/ht_stats.d \
		 $(dependd)/ht_init.d $(dependd)/util.d \
		 $(dependd)/rela_ts.d $(dependd)/radix_sort.d

$(libd)/libraikv.a: $(libraikv_objs) $(libraikv_libs)

all_libs    += $(libd)/libraikv.a
all_depends += $(libraikv_deps)

test_objs = $(objd)/test.o $(objd)/common.o
test_deps = $(dependd)/test.d $(dependd)/common.d
test_libs = $(libd)/libraikv.a
test_lnk  = -lraikv

$(bind)/test: $(test_objs) $(test_libs)

hash_test_objs = $(objd)/hash_test.o
hash_test_deps = $(dependd)/hash_test.d
hash_test_libs = $(libd)/libraikv.a
hash_test_lnk  = -lraikv

$(bind)/hash_test: $(hash_test_objs) $(hash_test_libs)

ping_objs = $(objd)/ping.o $(objd)/common.o
ping_deps = $(dependd)/ping.d $(dependd)/common.d
ping_libs = $(libd)/libraikv.a
ping_lnk  = -lraikv

$(bind)/ping: $(ping_objs) $(ping_libs)

cli_objs = $(objd)/cli.o $(objd)/common.o
cli_deps = $(dependd)/cli.d $(dependd)/common.d
cli_libs = $(libd)/libraikv.a
cli_lnk  = -lraikv

$(bind)/cli: $(cli_objs) $(cli_libs)

mcs_test_objs = $(objd)/mcs_test.o
mcs_test_deps = $(dependd)/mcs_test.d
mcs_test_libs = $(libd)/libraikv.a
mcs_test_lnk  = -lraikv

$(bind)/mcs_test: $(mcs_test_objs) $(mcs_test_libs)

server_objs = $(objd)/server.o $(objd)/common.o
server_deps = $(dependd)/server.d $(dependd)/common.d
server_libs = $(libd)/libraikv.a
server_lnk  = -lraikv

$(bind)/server: $(server_objs) $(server_libs)

load_objs = $(objd)/load.o $(objd)/common.o
load_deps = $(dependd)/load.d $(dependd)/common.d
load_libs = $(libd)/libraikv.a
load_lnk  = -lraikv

$(bind)/load: $(load_objs) $(load_libs)

ctest_objs = $(objd)/ctest.o $(objd)/common.o
ctest_deps = $(dependd)/ctest.d $(dependd)/common.d
ctest_libs = $(libd)/libraikv.a
ctest_lnk  = -lraikv

$(bind)/ctest: $(ctest_objs) $(ctest_libs)

rela_test_objs = $(objd)/rela_test.o
rela_test_deps = $(dependd)/rela_test.d
rela_test_libs = $(libd)/libraikv.a
rela_test_lnk  = -lraikv

$(bind)/rela_test: $(rela_test_objs) $(rela_test_libs)

#cuckoo_test_objs = $(objd)/cuckoo_test.o
#cuckoo_test_deps = $(dependd)/cuckoo_test.d

#$(bind)/cuckoo_test: $(cuckoo_test_objs) $(cuckoo_test_libs)

all_exes    += $(bind)/test $(bind)/hash_test $(bind)/ping \
               $(bind)/cli $(bind)/mcs_test $(bind)/server \
	       $(bind)/load $(bind)/rela_test $(bind)/ctest
all_depends += $(test_deps) $(hash_test_deps) $(ping_deps) \
               $(cli_deps) $(mcs_test_deps) $(server_deps) \
	       $(load_deps) $(rela_test_deps) $(ctest_deps)

all_dirs := $(bind) $(libd) $(objd) $(dependd)

# the default targets
.PHONY: all
all: $(all_libs) $(all_dlls) $(all_exes)

# create directories
$(dependd):
	@mkdir -p $(all_dirs)

# remove target bins, objs, depends
.PHONY: clean
clean:
	rm -r -f $(port)

# force a remake of depend using 'make -B depend'
.PHONY: depend
depend: $(dependd)/depend.make

$(dependd)/depend.make: $(dependd) $(all_depends)
	@echo "# depend file" > $(dependd)/depend.make
	@cat $(all_depends) >> $(dependd)/depend.make

# dependencies made by 'make depend'
-include $(dependd)/depend.make

$(objd)/%.o: src/%.cpp
	$(cpp) $(cflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.o: src/%.c
	$(cc) $(cflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.o: test/%.cpp
	$(cpp) $(cflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.o: test/%.c
	$(cc) $(cflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(libd)/%.a:
	ar rc $@ $($(*)_objs)

$(bind)/%:
	$(cpplink) $(cflags) -o $@ $($(*)_objs) -L$(libd) $($(*)_lnk) $(cpp_lnk) $(sock_lib) $(math_lib) $(thread_lib) $(malloc_lib) $(dynlink_lib)

$(dependd)/%.d: src/%.cpp
	gcc -x c++ $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.d: src/%.c
	gcc $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.d: test/%.cpp
	gcc -x c++ $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.d: test/%.c
	gcc $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

