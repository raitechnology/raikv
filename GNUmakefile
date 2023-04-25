# raikv makefile
lsb_dist     := $(shell if [ -f /etc/os-release ] ; then \
                  grep '^NAME=' /etc/os-release | sed 's/.*=[\"]*//' | sed 's/[ \"].*//' ; \
                  elif [ -x /usr/bin/lsb_release ] ; then \
                  lsb_release -is ; else echo Linux ; fi)
lsb_dist_ver := $(shell if [ -f /etc/os-release ] ; then \
		  grep '^VERSION=' /etc/os-release | sed 's/.*=[\"]*//' | sed 's/[ \"].*//' ; \
                  elif [ -x /usr/bin/lsb_release ] ; then \
                  lsb_release -rs | sed 's/[.].*//' ; else uname -r | sed 's/[-].*//' ; fi)
#lsb_dist     := $(shell if [ -x /usr/bin/lsb_release ] ; then lsb_release -is ; else echo Linux ; fi)
#lsb_dist_ver := $(shell if [ -x /usr/bin/lsb_release ] ; then lsb_release -rs | sed 's/[.].*//' ; else uname -r | sed 's/[-].*//' ; fi)
uname_m      := $(shell uname -m)

short_dist_lc := $(patsubst CentOS,rh,$(patsubst RedHatEnterprise,rh,\
                   $(patsubst RedHat,rh,\
                     $(patsubst Fedora,fc,$(patsubst Ubuntu,ub,\
		       $(patsubst Debian,deb,$(patsubst SUSE,ss,$(lsb_dist))))))))
short_dist    := $(shell echo $(short_dist_lc) | tr a-z A-Z)
pwd           := $(shell pwd)
rpm_os        := $(short_dist_lc)$(lsb_dist_ver).$(uname_m)

# this is where the targets are compiled
build_dir ?= $(short_dist)$(lsb_dist_ver)_$(uname_m)$(port_extra)
bind      := $(build_dir)/bin
libd      := $(build_dir)/lib64
objd      := $(build_dir)/obj
dependd   := $(build_dir)/dep

default_cflags := -ggdb -O3
# use 'make port_extra=-g' for debug build
ifeq (-g,$(findstring -g,$(port_extra)))
  default_cflags := -ggdb
endif
ifeq (-a,$(findstring -a,$(port_extra)))
  default_cflags := -fsanitize=address -ggdb -O3
endif
ifeq (-mingw,$(findstring -mingw,$(port_extra)))
  CC    := /usr/bin/x86_64-w64-mingw32-gcc
  CXX   := /usr/bin/x86_64-w64-mingw32-g++
  mingw := true
endif
ifeq (,$(port_extra))
  build_cflags := $(shell if [ -x /bin/rpm ]; then /bin/rpm --eval '%{optflags}' ; \
                          elif [ -x /bin/dpkg-buildflags ] ; then /bin/dpkg-buildflags --get CFLAGS ; fi)
endif
# msys2 using ucrt64
ifeq (MSYS2,$(lsb_dist))
  mingw := true
endif
CC          ?= gcc
CXX         ?= g++
cc          := $(CC) -std=c11
cpp         := $(CXX)
arch_cflags := -mavx -maes -fno-omit-frame-pointer
gcc_wflags  := -Wall -Wextra -Werror

# if windows cross compile
ifeq (true,$(mingw))
dll       := dll
exe       := .exe
soflag    := -shared -Wl,--subsystem,windows
fpicflags := -fPIC -DKV_SHARED
sock_lib  := -lcares -lws2_32
NO_STL    := 1
else
dll       := so
exe       :=
soflag    := -shared
fpicflags := -fPIC
thread_lib := -pthread -lrt
sock_lib  := -lcares
endif
# make apple shared lib
ifeq (Darwin,$(lsb_dist)) 
dll       := dylib
endif
# rpmbuild uses RPM_OPT_FLAGS
#ifeq ($(RPM_OPT_FLAGS),)
CFLAGS ?= $(build_cflags) $(default_cflags)
#else
#CFLAGS ?= $(RPM_OPT_FLAGS)
#endif
cflags := $(gcc_wflags) $(CFLAGS) $(arch_cflags)
lflags := -Wno-stringop-overflow

INCLUDES  ?= -Iinclude
DEFINES   ?=
includes  := $(INCLUDES)
defines   := $(DEFINES)

# if not linking libstdc++
ifdef NO_STL
cppflags  := -std=c++11 -fno-rtti -fno-exceptions
cpplink   := $(CC)
else
cppflags  := -std=c++11
cpplink   := $(CXX)
endif

rpath     := -Wl,-rpath,$(pwd)/$(libd)
math_lib  := -lm

# first target everything, target all: is at the end, after all_* are defined
.PHONY: everything
everything: all

# copr/fedora build (with version env vars)
# copr uses this to generate a source rpm with the srpm target
-include .copr/Makefile

# debian build (debuild)
# target for building installable deb: dist_dpkg
-include deb/Makefile

# targets filled in below
all_exes    :=
all_libs    :=
all_dlls    :=
all_depends :=
all_doc     :=
print_defines  = -DKV_VER=$(ver_build)
server_defines = -DKV_VER=$(ver_build)
lnk_lib     := $(libd)/libraikv.a
dlnk_lib    := -lraikv

$(objd)/print.o : .copr/Makefile
$(objd)/print.fpic.o : .copr/Makefile
$(objd)/server.o : .copr/Makefile
$(objd)/server.fpic.o : .copr/Makefile

libraikv_files := key_ctx ht_linear ht_cuckoo key_hash msg_ctx ht_stats \
                  ht_init scratch_mem util rela_ts radix_sort print \
		  ev_net route_db timer_queue stream_buf array_out \
		  bloom monitor ev_tcp ev_udp ev_unix ev_cares logger kv_pubsub
ifeq (true,$(mingw))
libraikv_files += win
endif
libraikv_cfile := \
          $(foreach file, $(libraikv_files), $(wildcard src/$(file).cpp)) \
          $(foreach file, $(libraikv_files), $(wildcard src/$(file).c))
libraikv_wfile := src/win.c
libraikv_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(libraikv_files)))
libraikv_dbjs  := $(addprefix $(objd)/, $(addsuffix .fpic.o, $(libraikv_files)))
libraikv_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(libraikv_files))) \
                  $(addprefix $(dependd)/, $(addsuffix .fpic.d, $(libraikv_files)))
libraikv_spec  := $(version)-$(build_num)_$(git_hash)
libraikv_ver   := $(major_num).$(minor_num)

$(libd)/libraikv.a: $(libraikv_objs)
$(libd)/libraikv.$(dll): $(libraikv_dbjs)

all_libs    += $(libd)/libraikv.a $(libd)/libraikv.$(dll)
all_depends += $(libraikv_deps)

kv_test_files := test
kv_test_cfile := $(addprefix test/, $(addsuffix .cpp, $(kv_test_files)))
kv_test_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(kv_test_files)))
kv_test_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(kv_test_files)))
kv_test_libs  := $(libd)/libraikv.a
#kv_test_lnk  = -lraikv
kv_test_lnk   := $(lnk_lib)

$(bind)/kv_test$(exe): $(kv_test_objs) $(kv_test_libs)
all_exes      += $(bind)/kv_test$(exe)
all_depends   += $(kv_test_deps)

hash_test_defines = -DKV_VER=$(ver_build)
$(objd)/hash_test.o : .copr/Makefile
$(objd)/hash_test.fpic.o : .copr/Makefile
hash_test_files := hash_test
hash_test_cfile := $(addprefix test/, $(addsuffix .cpp, $(hash_test_files)))
hash_test_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(hash_test_files)))
hash_test_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(hash_test_files)))
hash_test_libs  := $(libd)/libraikv.a
hash_test_lnk   := $(lnk_lib)

$(bind)/hash_test$(exe): $(hash_test_objs) $(hash_test_libs)
all_exes        += $(bind)/hash_test$(exe)
all_depends     += $(hash_test_deps)

ping_files := ping
ping_cfile := $(addprefix test/, $(addsuffix .cpp, $(ping_files)))
ping_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(ping_files)))
ping_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(ping_files)))
ping_libs  := $(libd)/libraikv.a
ping_lnk   := $(dlnk_lib)

$(bind)/ping$(exe): $(ping_objs) $(ping_libs)
all_exes        += $(bind)/ping$(exe)
all_depends     += $(ping_deps)

kv_cli_files := cli
kv_cli_cfile := $(addprefix test/, $(addsuffix .cpp, $(kv_cli_files)))
kv_cli_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(kv_cli_files)))
kv_cli_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(kv_cli_files)))
kv_cli_libs  := $(libd)/libraikv.a
kv_cli_lnk   := $(dlnk_lib)

$(bind)/kv_cli$(exe): $(kv_cli_objs) $(kv_cli_libs)
all_exes        += $(bind)/kv_cli$(exe)
all_depends     += $(kv_cli_deps)

mcs_test_files := mcs_test
mcs_test_cfile := $(addprefix test/, $(addsuffix .cpp, $(mcs_test_files)))
mcs_test_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(mcs_test_files)))
mcs_test_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(mcs_test_files)))
mcs_test_libs  := $(libd)/libraikv.a
mcs_test_lnk   := $(dlnk_lib)

$(bind)/mcs_test$(exe): $(mcs_test_objs) $(mcs_test_libs)
all_exes        += $(bind)/mcs_test$(exe)
all_depends     += $(mcs_test_deps)

kv_server_files := server
kv_server_cfile := $(addprefix test/, $(addsuffix .cpp, $(kv_server_files)))
kv_server_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(kv_server_files)))
kv_server_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(kv_server_files)))
kv_server_libs  := $(libd)/libraikv.a
kv_server_lnk   := $(dlnk_lib)

$(bind)/kv_server$(exe): $(kv_server_objs) $(kv_server_libs)
all_exes        += $(bind)/kv_server$(exe)
all_depends     += $(kv_server_deps)

load_files := load
load_cfile := $(addprefix test/, $(addsuffix .cpp, $(load_files)))
load_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(load_files)))
load_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(load_files)))
load_libs  := $(libd)/libraikv.a
load_lnk   := $(dlnk_lib)

$(bind)/load$(exe): $(load_objs) $(load_libs)
all_exes        += $(bind)/load$(exe)
all_depends     += $(load_deps)

ctest_files := ctest
ctest_cfile := $(addprefix test/, $(addsuffix .c, $(ctest_files)))
ctest_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(ctest_files)))
ctest_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(ctest_files)))
ctest_libs  := $(libd)/libraikv.a
ctest_lnk   := $(dlnk_lib)

$(bind)/ctest$(exe): $(ctest_objs) $(ctest_libs)
all_exes    += $(bind)/ctest$(exe)
all_depends += $(ctest_deps)

rela_test_files := rela_test
rela_test_cfile := $(addprefix test/, $(addsuffix .cpp, $(rela_test_files)))
rela_test_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(rela_test_files)))
rela_test_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(rela_test_files)))
rela_test_libs  := $(libd)/libraikv.a
rela_test_lnk   := $(dlnk_lib)

$(bind)/rela_test$(exe): $(rela_test_objs) $(rela_test_libs)
all_exes        += $(bind)/rela_test$(exe)
all_depends     += $(rela_test_deps)

pq_test_files := pq_test
pq_test_cfile := $(addprefix test/, $(addsuffix .cpp, $(pq_test_files)))
pq_test_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(pq_test_files)))
pq_test_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(pq_test_files)))

$(bind)/pq_test$(exe): $(pq_test_objs) $(pq_test_libs)
all_exes       += $(bind)/pq_test$(exe)
all_depends    += $(pq_test_deps)

pubsub_files := pubsub
pubsub_cfile := $(addprefix test/, $(addsuffix .cpp, $(pubsub_files)))
pubsub_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(pubsub_files)))
pubsub_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(pubsub_files)))
pubsub_libs  := $(libd)/libraikv.a
pubsub_lnk   := $(dlnk_lib)

$(bind)/pubsub$(exe): $(pubsub_objs) $(pubsub_libs)
all_exes     += $(bind)/pubsub$(exe)
all_depends  += $(pubsub_deps)

pipe_test_files := pipe_test
pipe_test_cfile := $(addprefix test/, $(addsuffix .cpp, $(pipe_test_files)))
pipe_test_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(pipe_test_files)))
pipe_test_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(pipe_test_files)))
pipe_test_libs  := $(libd)/libraikv.a
pipe_test_lnk   := $(dlnk_lib)

$(bind)/pipe_test$(exe): $(pipe_test_objs) $(pipe_test_libs)
#all_exes        += $(bind)/pipe_test$(exe)
#all_depends     += $(pipe_test_deps)

zipf_test_files := zipf_test
zipf_test_cfile := $(addprefix test/, $(addsuffix .cpp, $(zipf_test_files)))
zipf_test_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(zipf_test_files)))
zipf_test_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(zipf_test_files)))
zipf_test_libs  := $(libd)/libraikv.a
zipf_test_lnk   := $(dlnk_lib)

$(bind)/zipf_test$(exe): $(zipf_test_objs) $(zipf_test_libs)
all_exes        += $(bind)/zipf_test$(exe)
all_depends     += $(zipf_test_deps)

test_rtht_files := test_rtht
test_rtht_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_rtht_files)))
test_rtht_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_rtht_files)))
test_rtht_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_rtht_files)))
test_rtht_libs  := $(libd)/libraikv.a
test_rtht_lnk   := $(dlnk_lib)

$(bind)/test_rtht$(exe): $(test_rtht_objs) $(test_rtht_libs)
all_exes        += $(bind)/test_rtht$(exe)
all_depends     += $(test_rtht_deps)

test_delta_files := test_delta
test_delta_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_delta_files)))
test_delta_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_delta_files)))
test_delta_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_delta_files)))
test_delta_libs  := $(libd)/libraikv.a
test_delta_lnk   := $(dlnk_lib)

$(bind)/test_delta$(exe): $(test_delta_objs) $(test_delta_libs)
all_exes         += $(bind)/test_delta$(exe)
all_depends      += $(test_delta_deps)

test_routes_files := test_routes
test_routes_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_routes_files)))
test_routes_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_routes_files)))
test_routes_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_routes_files)))
test_routes_libs  := $(libd)/libraikv.a
test_routes_lnk   := $(dlnk_lib)

$(bind)/test_routes$(exe): $(test_routes_objs) $(test_routes_libs)
all_exes          += $(bind)/test_routes$(exe)
all_depends       += $(test_routes_deps)

test_wild_files := test_wild
test_wild_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_wild_files)))
test_wild_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_wild_files)))
test_wild_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_wild_files)))
test_wild_libs  := $(libd)/libraikv.a
test_wild_lnk   := $(dlnk_lib) -lpcre2-8

$(bind)/test_wild$(exe): $(test_wild_objs) $(test_wild_libs)
all_exes        += $(bind)/test_wild$(exe)
all_depends     += $(test_wild_deps)

test_uintht_files := test_uintht
test_uintht_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_uintht_files)))
test_uintht_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_uintht_files)))
test_uintht_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_uintht_files)))
test_uintht_libs  := $(libd)/libraikv.a
test_uintht_lnk   := $(dlnk_lib)

$(bind)/test_uintht$(exe): $(test_uintht_objs) $(test_uintht_libs)
all_exes          += $(bind)/test_uintht$(exe)
all_depends       += $(test_uintht_deps)

test_bitset_files := test_bitset
test_bitset_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_bitset_files)))
test_bitset_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_bitset_files)))
test_bitset_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_bitset_files)))
test_bitset_libs  := $(libd)/libraikv.a
test_bitset_lnk   := $(dlnk_lib)

$(bind)/test_bitset$(exe): $(test_bitset_objs) $(test_bitset_libs)
all_exes          += $(bind)/test_bitset$(exe)
all_depends       += $(test_bitset_deps)

test_dlist_files := test_list
test_dlist_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_dlist_files)))
test_dlist_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_dlist_files)))
test_dlist_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_dlist_files)))
test_dlist_libs  := $(libd)/libraikv.a
test_dlist_lnk   := $(dlnk_lib)

$(bind)/test_dlist$(exe): $(test_dlist_objs) $(test_dlist_libs)
all_exes        += $(bind)/test_dlist$(exe)
all_depends     += $(test_dlist_deps)

test_bloom_files := test_bloom
test_bloom_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_bloom_files)))
test_bloom_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_bloom_files)))
test_bloom_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_bloom_files)))
test_bloom_libs  := $(libd)/libraikv.a
test_bloom_lnk   := $(dlnk_lib)

$(bind)/test_bloom$(exe): $(test_bloom_objs) $(test_bloom_libs)
all_exes         += $(bind)/test_bloom$(exe)
all_depends      += $(test_bloom_deps)

test_coll_files := test_coll
test_coll_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_coll_files)))
test_coll_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_coll_files)))
test_coll_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_coll_files)))
test_coll_libs  := $(libd)/libraikv.a
test_coll_lnk   := $(dlnk_lib)

$(bind)/test_coll$(exe): $(test_coll_objs) $(test_coll_libs)
all_exes        += $(bind)/test_coll$(exe)
all_depends     += $(test_coll_deps)

test_min_files := test_min
test_min_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_min_files)))
test_min_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_min_files)))
test_min_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_min_files)))
test_min_libs  := $(libd)/libraikv.a
test_min_lnk   := $(dlnk_lib)

$(bind)/test_min$(exe): $(test_min_objs) $(test_min_libs)
all_exes       += $(bind)/test_min$(exe)
all_depends    += $(test_min_deps)

test_timer_files := test_timer
test_timer_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_timer_files)))
test_timer_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_timer_files)))
test_timer_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_timer_files)))
test_timer_libs  := $(libd)/libraikv.a
test_timer_lnk   := $(dlnk_lib)

$(bind)/test_timer$(exe): $(test_timer_objs) $(test_timer_libs)
all_exes         += $(bind)/test_timer$(exe)
all_depends      += $(test_timer_deps)

all_dirs := $(bind) $(libd) $(objd) $(dependd)

test_tcp_files := test_tcp
test_tcp_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_tcp_files)))
test_tcp_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_tcp_files)))
test_tcp_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_tcp_files)))
test_tcp_libs  := $(libd)/libraikv.a
test_tcp_lnk   := $(dlnk_lib)

$(bind)/test_tcp$(exe): $(test_tcp_objs) $(test_tcp_libs)
all_exes       += $(bind)/test_tcp$(exe)
all_depends    += $(test_tcp_deps)

test_udp_files := test_udp
test_udp_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_udp_files)))
test_udp_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_udp_files)))
test_udp_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_udp_files)))
test_udp_libs  := $(libd)/libraikv.a
test_udp_lnk   := $(dlnk_lib)

$(bind)/test_udp$(exe): $(test_udp_objs) $(test_udp_libs)
all_exes       += $(bind)/test_udp$(exe)
all_depends    += $(test_udp_deps)

ifneq (true,$(mingw))
test_unix_files := test_unix
test_unix_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_unix_files)))
test_unix_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_unix_files)))
test_unix_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_unix_files)))
test_unix_libs  := $(libd)/libraikv.a
test_unix_lnk   := $(dlnk_lib)

$(bind)/test_unix$(exe): $(test_unix_objs) $(test_unix_libs)
all_exes        += $(bind)/test_unix$(exe)
all_depends     += $(test_unix_deps)

test_dgram_files := test_dgram
test_dgram_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_dgram_files)))
test_dgram_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_dgram_files)))
test_dgram_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_dgram_files)))
test_dgram_libs  := $(libd)/libraikv.a
test_dgram_lnk   := $(dlnk_lib)

$(bind)/test_dgram$(exe): $(test_dgram_objs) $(test_dgram_libs)
all_exes        += $(bind)/test_dgram$(exe)
all_depends     += $(test_dgram_deps)
endif

test_log_files := test_log
test_log_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_log_files)))
test_log_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_log_files)))
test_log_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_log_files)))
test_log_libs  := $(libd)/libraikv.a
test_log_lnk   := $(dlnk_lib)

$(bind)/test_log$(exe): $(test_log_objs) $(test_log_libs)
all_exes       += $(bind)/test_log$(exe)
all_depends    += $(test_log_deps)

test_dns_files := test_dns
test_dns_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_dns_files)))
test_dns_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_dns_files)))
test_dns_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_dns_files)))
test_dns_libs  := $(libd)/libraikv.a
test_dns_lnk   := $(dlnk_lib)

$(bind)/test_dns$(exe): $(test_dns_objs) $(test_dns_libs)
all_exes       += $(bind)/test_dns$(exe)
all_depends    += $(test_dns_deps)

test_balloc_files := test_balloc
test_balloc_cfile := $(addprefix test/, $(addsuffix .cpp, $(test_balloc_files)))
test_balloc_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_balloc_files)))
test_balloc_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_balloc_files)))
test_balloc_libs  := $(libd)/libraikv.a
test_balloc_lnk   := $(dlnk_lib)

$(bind)/test_balloc$(exe): $(test_balloc_objs) $(test_balloc_libs)
all_exes       += $(bind)/test_balloc$(exe)
all_depends    += $(test_balloc_deps)

all_dirs := $(bind) $(libd) $(objd) $(dependd)

have_pandoc := $(shell if [ -x /usr/bin/pandoc ]; then echo true; fi)
ifeq ($(have_pandoc),true)
doc/kv_server.1: doc/kv_server.1.md
	pandoc -s -t man $< -o $@

doc/kv_test.1: doc/kv_test.1.md
	pandoc -s -t man $< -o $@

doc/kv_cli.1: doc/kv_cli.1.md
	pandoc -s -t man $< -o $@

all_doc += doc/kv_server.1 doc/kv_test.1 doc/kv_cli.1
endif

# the default targets
.PHONY: all
all: $(all_libs) $(all_dlls) $(all_exes) $(all_doc) cmake

.PHONY: cmake
cmake: CMakeLists.txt

.ONESHELL: CMakeLists.txt
CMakeLists.txt: .copr/Makefile
	@cat <<'EOF' > $@
	cmake_minimum_required (VERSION 3.9.0)
	if (POLICY CMP0111)
	  cmake_policy(SET CMP0111 OLD)
	endif ()
	project (raikv)
	include_directories (include)
	if (CMAKE_SYSTEM_NAME STREQUAL "Windows")
	  add_definitions(/DPCRE2_STATIC)
	  if ($$<CONFIG:Release>)
	    add_compile_options (/arch:AVX2 /GL /std:c11 /wd5105)
	  else ()
	    add_compile_options (/arch:AVX2 /std:c11 /wd5105)
	  endif ()
	  set (kv_sources $(libraikv_cfile) $(libraikv_wfile))
	else ()
	  set (kv_sources $(libraikv_cfile))
	  add_compile_options ($(cflags))
	endif ()
	add_library (raikv STATIC $${kv_sources})
	if (CMAKE_SYSTEM_NAME STREQUAL "Windows")
	  link_libraries (raikv ws2_32)
	else ()
	  link_libraries (raikv -lcares -lpthread -lrt)
	endif ()
	add_definitions (-DKV_VER=$(ver_build))
	add_executable (kv_test $(kv_test_cfile))
	add_executable (hash_test $(hash_test_cfile))
	add_executable (ping $(ping_cfile))
	add_executable (kv_cli $(kv_cli_cfile))
	add_executable (mcs_test $(mcs_test_cfile))
	add_executable (kv_server $(kv_server_cfile))
	add_executable (load $(load_cfile))
	add_executable (ctest $(ctest_cfile))
	add_executable (rela_test $(rela_test_cfile))
	add_executable (pq_test $(pq_test_cfile))
	add_executable (pubsub $(pubsub_cfile))
	add_executable (zipf_test $(zipf_test_cfile))
	add_executable (test_rtht $(test_rtht_cfile))
	add_executable (test_delta $(test_delta_cfile))
	add_executable (test_routes $(test_routes_cfile))
	add_executable (test_wild $(test_wild_cfile))
	if (CMAKE_SYSTEM_NAME STREQUAL "Windows")
	  if (NOT TARGET pcre2-8-static)
	    add_library (pcre2-8-static STATIC IMPORTED)
	    set_property (TARGET pcre2-8-static PROPERTY IMPORTED_LOCATION_DEBUG ../pcre2/build/Debug/pcre2-8-staticd.lib)
	    set_property (TARGET pcre2-8-static PROPERTY IMPORTED_LOCATION_RELEASE ../pcre2/build/Release/pcre2-8-static.lib)
	    target_include_directories (test_wild PUBLIC ../pcre2/build)
	  else ()
	    target_include_directories (test_wild PUBLIC $${CMAKE_BINARY_DIR}/pcre2)
	  endif ()
	  target_link_libraries (test_wild pcre2-8-static)
	else ()
	  if (TARGET pcre2-8-static)
	    target_include_directories (test_wild PUBLIC $${CMAKE_BINARY_DIR}/pcre2)
	    target_link_libraries (test_wild pcre2-8-static)
	  else ()
	    target_link_libraries (test_wild -lpcre2-8)
	  endif ()
	endif ()
	add_executable (test_uintht $(test_uintht_cfile))
	add_executable (test_bitset $(test_bitset_cfile))
	add_executable (test_dlist $(test_dlist_cfile))
	add_executable (test_bloom $(test_bloom_cfile))
	add_executable (test_coll $(test_coll_cfile))
	add_executable (test_min $(test_min_cfile))
	add_executable (test_timer $(test_timer_cfile))
	add_executable (test_tcp $(test_tcp_cfile))
	add_executable (test_udp $(test_udp_cfile))
	add_executable (test_log $(test_log_cfile))
	EOF

# create directories
$(dependd):
	@mkdir -p $(all_dirs)

# remove target bins, objs, depends
.PHONY: clean
clean:
	rm -rf $(bind) $(libd) $(objd) $(dependd)
	if [ "$(build_dir)" != "." ] ; then rmdir $(build_dir) ; fi

.PHONY: clean_dist
clean_dist:
	rm -rf dpkgbuild rpmbuild

.PHONY: clean_all
clean_all: clean clean_dist

# force a remake of depend using 'make -B depend'
.PHONY: depend
depend: $(dependd)/depend.make

$(dependd)/depend.make: $(dependd) $(all_depends)
	@echo "# depend file" > $(dependd)/depend.make
	@cat $(all_depends) >> $(dependd)/depend.make

.PHONY: dist_bins
dist_bins: $(all_libs) $(all_dlls) $(bind)/kv_cli $(bind)/kv_server $(bind)/kv_test
	chrpath -d $(libd)/libraikv.$(dll)
	chrpath -d $(bind)/kv_cli
	chrpath -d $(bind)/kv_server
	chrpath -d $(bind)/kv_test

.PHONY: dist_rpm
dist_rpm: srpm
	( cd rpmbuild && rpmbuild --define "-topdir `pwd`" -ba SPECS/raikv.spec )

.PHONY: local_repo_update
local_repo_update: dist_rpm
	createrepo --update `pwd`/rpmbuild/RPMS/${uname_m}
	sudo dnf -y update raikv
	sudo dnf -y debuginfo-install raikv-debuginfo
	sudo dnf -y update raikv-debugsource

.PHONY: local_repo_create
local_repo_create: dist_rpm
	createrepo -v `pwd`/rpmbuild/RPMS/${uname_m}
	@echo "# Create this file: /etc/yum.repos.d/raikv.repo"
	@echo "[raikv]"
	@echo "name=My Local Repository"
	@echo "baseurl=file://`pwd`/rpmbuild/RPMS/${uname_m}"
	@echo "metadata_expire=1"
	@echo "gpgcheck=0"
	@echo "enabled=1"
	@echo "# Use this to install:"
	@echo "sudo dnf -y install raikv"
	@echo "sudo dnf -y debuginfo-install raikv-debuginfo"
	@echo "sudo dnf -y install raikv-debugsource"

# dependencies made by 'make depend'
-include $(dependd)/depend.make

.PHONY: dnf_depend
dnf_depend:
	sudo dnf -y install make gcc-c++ git redhat-lsb pcre2-devel chrpath c-ares-devel

.PHONY: yum_depend
yum_depend:
	sudo yum -y install make gcc-c++ git redhat-lsb pcre2-devel chrpath c-ares-devel

.PHONY: deb_depend
deb_depend:
	sudo apt-get install -y install make g++ gcc devscripts libpcre2-dev chrpath git lsb-release c-ares-dev

ifeq ($(DESTDIR),)
# 'sudo make install' puts things in /usr/local/lib, /usr/local/include
install_prefix = /usr/local
else
# debuild uses DESTDIR to put things into debian/libdecnumber/usr
install_prefix = $(DESTDIR)/usr
endif

install: dist_bins
	install -d $(install_prefix)/lib $(install_prefix)/bin
	install -d $(install_prefix)/include/raikv
	for f in $(libd)/libraikv.* ; do \
	if [ -h $$f ] ; then \
	cp -a $$f $(install_prefix)/lib ; \
	else \
	install $$f $(install_prefix)/lib ; \
	fi ; \
	done
	install -m 755 $(bind)/kv_cli $(install_prefix)/bin
	install -m 755 $(bind)/kv_server $(install_prefix)/bin
	install -m 755 $(bind)/kv_test $(install_prefix)/bin
	install -m 644 include/raikv/*.h $(install_prefix)/include/raikv

$(objd)/%.o: src/%.cpp
	$(cpp) $(cflags) $(cppflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.o: src/%.c
	$(cc) $(cflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.fpic.o: src/%.cpp
	$(cpp) $(cflags) $(fpicflags) $(cppflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.fpic.o: src/%.c
	$(cc) $(cflags) $(fpicflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.o: test/%.cpp
	$(cpp) $(cflags) $(cppflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.o: test/%.c
	$(cc) $(cflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(libd)/%.a:
	ar rc $@ $($(*)_objs)

ifeq (Darwin,$(lsb_dist))
$(libd)/%.dylib:
	$(cpplink) -dynamiclib $(cflags) $(lflags) -o $@.$($(*)_dylib).dylib -current_version $($(*)_dylib) -compatibility_version $($(*)_ver) $($(*)_dbjs) $($(*)_dlnk) $(sock_lib) $(math_lib) $(thread_lib) $(malloc_lib) $(dynlink_lib) && \
	cd $(libd) && ln -f -s $(@F).$($(*)_dylib).dylib $(@F).$($(*)_ver).dylib && ln -f -s $(@F).$($(*)_ver).dylib $(@F)
else
$(libd)/%.$(dll):
	$(cpplink) $(soflag) $(rpath) $(cflags) $(lflags) -o $@.$($(*)_spec) -Wl,-soname=$(@F).$($(*)_ver) $($(*)_dbjs) $($(*)_dlnk) $(sock_lib) $(math_lib) $(thread_lib) $(malloc_lib) $(dynlink_lib) && \
	cd $(libd) && ln -f -s $(@F).$($(*)_spec) $(@F).$($(*)_ver) && ln -f -s $(@F).$($(*)_ver) $(@F)
endif

$(bind)/%$(exe):
	$(cpplink) $(cflags) $(lflags) $(rpath) -o $@ $($(*)_objs) -L$(libd) $($(*)_lnk) $(cpp_lnk) $(sock_lib) $(math_lib) $(thread_lib) $(malloc_lib) $(dynlink_lib)

$(dependd)/%.d: src/%.cpp
	$(cpp) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.d: src/%.c
	$(cc) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.fpic.d: src/%.cpp
	$(cpp) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).fpic.o -MF $@

$(dependd)/%.fpic.d: src/%.c
	$(cc) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).fpic.o -MF $@

$(dependd)/%.d: test/%.cpp
	$(cpp) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.d: test/%.c
	$(cc) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

