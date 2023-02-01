.POSIX:
.SUFFIXES:

CC = cc
CFLAGS = -Os
LDFLAGS =

echo10k: echo10k.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) -pthread

echo10k-debug: echo10k.c
	$(CC) -std=c99 -O0 -g3 -Wall -Wextra -pedantic -o $@ $< -pthread

debug: echo10k-debug FORCE
	lldb $<

clean: FORCE
	rm -f echo10k echo10k-debug *.dSYM

FORCE: ;
