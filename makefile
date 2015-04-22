CC = gcc
CFLAGS = -c -Wall -O3 `mysql_config --cflags`
LDFLAGS =
SOURCES = zonefile_parse.c
OBJECTS = $(SOURCES:.c=.o)
EXECUTABLE = zonefile_parse
LIBS = `mysql_config --libs`

all: $(SOURCES) $(EXECUTABLE) $(SRCPARSER) $(EXEPARSER)
	
$(EXECUTABLE): $(OBJECTS)
	$(CC) $(LDFLAGS) $(OBJECTS) $(LIBS) -o $@

.c.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -rf *o $(EXECUTABLE) $(EXEPARSER)


