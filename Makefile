.POSIX:
.SUFFIXES:

CC = cc
CFLAGS = -Os
LDFLAGS =

echo-server: echo-server.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) -pthread

echo-server-debug: echo-server.c
	$(CC) -std=c99 -O0 -g3 -Wall -Wextra -pedantic -o $@ $< -pthread

debug: echo-server-debug FORCE
	lldb $<

clean: FORCE
	rm -f echo-server echo-server-debug

FORCE: ;
