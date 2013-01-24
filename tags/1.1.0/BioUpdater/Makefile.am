SUBDIRS = loadPops

noinst_LTLIBRARIES=liblokiupdate.la

liblokiupdate_la_SOURCES=dbsettings.py\
bio_loader.py \
__init__.py \
loaders/ncbi_loader.py \
loaders/load_reactome.py \
loaders/load_pharmgkb.py \
loaders/load_pfam.py \
loaders/load_netpath.py \
loaders/load_mint.py \
loaders/load_kegg.py \
loaders/load_go.py \
loaders/load_ensembl.py \
loaders/load_dip.py \
loaders/load_chainfiles.py \
loaders/load_biogrid.py \
loaders/__init__.py \
loaders/util/settings.py \
loaders/util/region_manager.py \
loaders/util/biosettings.py \
loaders/util/bioloader.py \
loaders/util/__init__.py 

if LOKI_INSTALL
LOKI_CMDS=$(srcdir)/bio_loader.py LOAD knowledge.bio ALL
VAR_FILES=knowledge.bio variations variations.txt
	
else
LOKI_CMDS=$(srcdir)/bio_loader.py LOAD test.bio TEST
VAR_FILES=test.bio variations-test variations-test.txt
	
endif

variations: knowledge.bio

variations.txt : knowledge.bio

knowledge.bio: liblokiupdate.la .creation
	rm -rf knowledge.bio
	$(LOKI_CMDS)

variations-test: test.bio

variations-test.txt: test.bio

test.bio: liblokiupdate.la .creation
	rm -rf test.bio
	$(LOKI_CMDS)
	
.creation:
	date +"%Y.%m.%d" > .creation

DATE_PREFIX="`cat .creation`."

install-data-hook:
	for f in $(VAR_FILES); do \
		mv -f $(DESTDIR)$(datadir)/$$f $(DESTDIR)$(datadir)/$(DATE_PREFIX)$$f; \
		chmod a-wx $(DESTDIR)$(datadir)/$(DATE_PREFIX)$$f; \
		ln -s $(DESTDIR)$(datadir)/$(DATE_PREFIX)$$f $(DESTDIR)$(datadir)/$$f; \
	done

uninstall-hook:
	rm -f $(DESTDIR)$(datadir)/*test.bio $(DESTDIR)$(datadir)/*variations* $(DESTDIR)$(datadir)/*knowledge.bio
	


nodist_data_DATA=$(VAR_FILES)

CLEANFILES=test.bio knowledge.bio variations* observed_relationships.csv .creation








