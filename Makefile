CC     = gcc
CFLAGS = -Wall -Wextra -g -pthread

all: chatd

chatd: chatd.c
	$(CC) $(CFLAGS) -o chatd chatd.c

clean:
	rm -f chatd