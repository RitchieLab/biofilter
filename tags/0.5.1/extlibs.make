
# -- Some libraries and applications require freetype. They will definie this variable
                                                                                                            
ifeq ($(FREETYPE), 1)
ifeq ($(WIN32), 1)
    FT_CPPFLAGS:=-I/c/unx/3rdparty/include -I/c/unx/3rdparty/include/freetype2
    FT_LINK:=-L/c/unx/3rdparty/lib -lfreetype
else
    FT_CPPFLAGS:=$(shell freetype-config --cflags)
    FT_LINK:=$(shell freetype-config --libs)
endif
endif

# -- Some apps depend on wx-widgets
ifeq ($(WX_WIDGETS), 1)
    WX_CPPFLAGS:=$(shell wx-config --cppflags)
    WX_LINK:=$(shell wx-config --libs)
endif


ifeq ($(SOCI), 1)
ifeq ($(WIN32), 1)
        SOCI_CPPFLAGS:=-I/usr/include/soci -I/usr/local/include/soci -I/usr/local/include/soci/sqlite3/
        SOCI_LINKS:=-L /usr/local/lib -lsoci_sqlite3-gcc-3_0 -lsoci_core-gcc-3_0
else
        SOCI_CPPFLAGS:=-I/usr/include/soci -I/usr/local/include/soci -I/usr/include/soci/sqlite3 -I/usr/local/include/soci/sqlite3/
        #SOCI_LINKS:=-L/usr/local/lib -lsoci_sqlite3-gcc-3_0 -lsoci_core-gcc-3_0
	#To build on my mac, I have to uncomment this line. 
        #SOCI_LINKS:=-L/usr/local/lib -L/usr/local/lib64 /usr/local/lib/libsoci_sqlite3-gcc-3_0.a /usr/local/lib/libsoci_core-gcc-3_0.a -lsqlite3 -ldl -lpthread
	#To build on our cheeses, I have to uncomment this line....need to streamline this 
	SOCI_LINKS:=-L/usr/local/lib -L/usr/local/lib64 /usr/local/lib64/libsoci_sqlite3-gcc-3_0.a /usr/local/lib64/libsoci_core-gcc-3_0.a /usr/local/lib64/libsqlite3.a -ldl -lpthread
	#SOCI_LINES:=-L/usr/local/lib -L/usr/local/lib64 -lsoci_sqlite3-gcc-3_0 -lsoci_core-gcc-3_0 -lsqlite3 -ldl -lpthread
	
	# for bx.psu
	SOCI_CPPFLAGS:=-I/afs/bx.psu.edu/depot/data/ritchie_lab/usr/tools/include
	SOCI_LINKS:=-L/afs/bx.psu.edu/depot/data/ritchie_lab/usr/tools/lib -lsoci_sqlite3 -lsoci_core -lsqlite3 -ldl -lpthread
endif
endif

EXTERNAL_LINKS+=$(WX_LINK) $(FT_LINK) $(SOCI_LINKS)
EXT_INCLUDES+=$(WX_CPPFLAGS) $(FT_CPPFLAGS) $(SOCI_CPPFLAGS)
