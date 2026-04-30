CC     = gcc
CFLAGS = -Wall -Wextra -g -pthread

all: chat

chat: chat.c
	$(CC) $(CFLAGS) -o chat chat.c

clean:
	rm -f chat