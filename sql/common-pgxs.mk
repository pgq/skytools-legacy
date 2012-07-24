
# PGXS does not support modules that are supposed
# to run on different Postgres versions very well.
# Here are some workarounds for them.

# Variables that are used when extensions are available
Extension_data ?=
Extension_data_built ?=
Extension_regress ?=

# Variables that are used when extensions are not available
Contrib_data ?=
Contrib_data_built ?=
Contrib_regress ?=

# Should the Contrib* files installed (under ../contrib/)
# even when extensions are available?
Contrib_install_always ?= no

#
# switch variables
#

IfExt = $(if $(filter 8.% 9.0%,$(MAJORVERSION)8.3),$(2),$(1))

DATA = $(call IfExt,$(Extension_data),$(Contrib_data))
DATA_built = $(call IfExt,$(Extension_data_built),$(Contrib_data_built))
REGRESS = $(call IfExt,$(Extension_regress),$(Contrib_regress))

EXTRA_CLEAN += $(call IfExt,$(Contrib_data_built),$(Extension_data_built)) test.dump

# have deterministic dbname for regtest database
override CONTRIB_TESTDB = regression
REGRESS_OPTS = --load-language=plpgsql --dbname=$(CONTRIB_TESTDB)

#
# load PGXS
#

PG_CONFIG ?= pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

#
# build rules, in case Contrib data must be always installed
#

ifeq ($(call IfExt,$(Contrib_install_always),no),yes)

all: $(Contrib_data) $(Contrib_data_built)
installdirs: installdirs-old-contrib
install: install-old-contrib

installdirs-old-contrib:
	$(MKDIR_P) '$(DESTDIR)$(datadir)/contrib'

install-old-contrib: $(Contrib_data) $(Contrib_data_built) installdirs-old-contrib
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(Contrib_data)) $(Contrib_data_built) '$(DESTDIR)$(datadir)/contrib/'

endif

#
# regtest shortcuts
#

test: install
	$(MAKE) installcheck || { filterdiff --format=unified regression.diffs | less; exit 1; }
	pg_dump regression > test.dump

ack:
	cp results/*.out expected/

.PHONY: test ack installdirs-old-contrib install-old-contrib
