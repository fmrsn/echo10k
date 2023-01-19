.POSIX:
.SUFFIXES:

CC = cc
CFLAGS = -Os
LDFLAGS =

echo-server: echo-server.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) -pthread

echo-server-debug: echo-server.c
	$(CC) -O0 -g3 -Wall -Wextra -Werror -pedantic -o $@ $< -pthread

clean: FORCE
	rm -f echo-server echo-server-debug

FORCE: ;
