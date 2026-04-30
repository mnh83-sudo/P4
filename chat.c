#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <pthread.h>
#include <ctype.h>

#define BACKLOG 5
#define BUFFER_SIZE 4096
#define MAX_NAME 32
#define MAX_STATUS 64
#define MAX_MSG 80
#define PROTO_VERSION "1"

/* ── linked list of connected users ── */
struct account_user {
    int  fd;
    char name[MAX_NAME + 1];
    char status[MAX_STATUS + 1];
    int  logged_in;
    struct account_user *next;
};

struct account_user *head = NULL;
pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

int setting_socket(int *port) {

    int hold_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (hold_sock < 0) {
        perror("socket() failed");
        exit(EXIT_FAILURE);
    }

    /* allow port reuse so we can restart quickly */
    int opt = 1;
    setsockopt(hold_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(*port);

    int binding = bind(hold_sock, (struct sockaddr *)&addr, sizeof(addr));
    if (binding < 0) {
        perror("bind() failed");
        exit(EXIT_FAILURE);
    }

    int listen_sock = listen(hold_sock, BACKLOG);
    if (listen_sock < 0) {
        perror("listen() failed");
        exit(EXIT_FAILURE);
    }

    return hold_sock;
}

/* add a new client to the front of the list */
void add_client(struct account_user *c) {
    pthread_mutex_lock(&clients_mutex);
    c->next = head;
    head    = c;
    pthread_mutex_unlock(&clients_mutex);
}

/* remove a client from the list by fd */
void remove_client(int fd) {
    pthread_mutex_lock(&clients_mutex);

    struct account_user *prev = NULL;
    struct account_user *cur  = head;

    while (cur != NULL) {
        if (cur->fd == fd) {
            if (prev == NULL) head      = cur->next;
            else              prev->next = cur->next;
            free(cur);
            break;
        }
        prev = cur;
        cur  = cur->next;
    }

    pthread_mutex_unlock(&clients_mutex);
}

/* find a client by name — call with mutex ALREADY held */
struct account_user *find_by_name_locked(const char *name) {
    struct account_user *cur = head;
    while (cur != NULL) {
        if (cur->logged_in && strcmp(cur->name, name) == 0)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

void send_err(int fd, int code, const char *explanation) {
    char body[256];
    /* body = "<code>|<explanation>|" */
    snprintf(body, sizeof(body), "%d|%s|", code, explanation);
    int body_len = (int)strlen(body);

    char frame[300];
    snprintf(frame, sizeof(frame), "1|ERR|%d|%s", body_len, body);
    write(fd, frame, strlen(frame));
}

void send_msg(int fd, const char *sender, const char *recipient, const char *text) {
    char body[8192];
    /* body = "<sender>|<recipient>|<text>|" */
    snprintf(body, sizeof(body), "%s|%s|%s|", sender, recipient, text);
    int body_len = (int)strlen(body);

    char frame[8300];
    snprintf(frame, sizeof(frame), "1|MSG|%d|%s", body_len, body);
    write(fd, frame, strlen(frame));
}

/* broadcast  1|MSG|...  to every logged-in user */
void broadcast_msg(const char *sender, const char *recipient, const char *text) {
    pthread_mutex_lock(&clients_mutex);
    struct account_user *cur = head;
    while (cur != NULL) {
        if (cur->logged_in)
            send_msg(cur->fd, sender, recipient, text);
        cur = cur->next;
    }
    pthread_mutex_unlock(&clients_mutex);
}

/* screen name: letters, digits, hyphen, underscore; 1-32 chars */
int valid_name(const char *s) {
    int len = (int)strlen(s);
    if (len < 1 || len > MAX_NAME) return 0;
    for (int i = 0; i < len; i++) {
        char c = s[i];
        if (!isalnum((unsigned char)c) && c != '-' && c != '_')
            return 0;
    }
    return 1;
}

/* status / message body: ASCII 32-126; status 0-64, message 1-80 */
int valid_text(const char *s, int max_len) {
    int len = (int)strlen(s);
    if (len > max_len) return 0;
    for (int i = 0; i < len; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c < 32 || c > 126) return 0;
    }
    return 1;
}

int read_exact(int fd, char *buf, int want) {
    int total = 0;
    while (total < want) {
        int n = read(fd, buf + total, want - total);
        if (n <= 0) return total;
        total += n;
    }
    return total;
}

int parse_message(int fd, char type_out[4], char *fields[], int max_fields, char **body_out) {
    *body_out = NULL;

    char header[32];
    int  hlen = 0;

    /* read version digit */
    char ch;
    if (read_exact(fd, &ch, 1) < 1) return -1;
    if (ch != '1') return -1;   
    header[hlen++] = ch;

    /* separator */
    if (read_exact(fd, &ch, 1) < 1) return -1;
    if (ch != '|') return -1;
    header[hlen++] = ch;

    char type[4];
    if (read_exact(fd, type, 3) < 3) return -1;
    type[3] = '\0';
    memcpy(type_out, type, 4);

    if (read_exact(fd, &ch, 1) < 1) return -1;
    if (ch != '|') return -1;

    /* body_len: read digits until '|' */
    char len_str[8];
    int  llen = 0;
    while (1) {
        if (read_exact(fd, &ch, 1) < 1) return -1;
        if (ch == '|') break;
        if (llen >= 5 || !isdigit((unsigned char)ch)) return -1;
        len_str[llen++] = ch;
    }
    if (llen == 0) return -1;
    len_str[llen] = '\0';
    int body_len = atoi(len_str);

    if (body_len < 1 || body_len > 99999) return -1;

    char *body = malloc(body_len + 1);
    if (!body) return -1;

    if (read_exact(fd, body, body_len) < body_len) {
        free(body);
        return -1;
    }
    body[body_len] = '\0';
    *body_out = body;

    /* last char must be '|' */
    if (body[body_len - 1] != '|') return -1;

    /* ── split body into fields on '|' ── */
    int  nfields = 0;
    char *p      = body;
    char *end    = body + body_len;   /* points just past last byte */

    while (p < end && nfields < max_fields) {
        fields[nfields++] = p;
        /* find next '|' */
        char *bar = memchr(p, '|', end - p);
        if (!bar) break;
        *bar = '\0';
        p    = bar + 1;
    }

    return nfields;
}



/* NAM: client wants to set their screen name */
void handle_nam(struct account_user *c, char *fields[], int nfields) {
    if (nfields < 1) {
        send_err(c->fd, 0, "NAM requires a name field");
        return;
    }

    char *requested_name = fields[0];

    /* validate characters */
    if (!valid_name(requested_name)) {
        /* check length separately for a better error code */
        if (strlen(requested_name) > MAX_NAME)
            send_err(c->fd, 4, "Name too long");
        else
            send_err(c->fd, 3, "Illegal character in name");
        return;
    }

    /* check uniqueness */
    pthread_mutex_lock(&clients_mutex);
    struct account_user *existing = find_by_name_locked(requested_name);
    if (existing != NULL) {
        pthread_mutex_unlock(&clients_mutex);
        send_err(c->fd, 1, "Name in use");
        return;
    }

    /* register the name */
    strncpy(c->name, requested_name, MAX_NAME);
    c->name[MAX_NAME] = '\0';
    c->logged_in = 1;
    pthread_mutex_unlock(&clients_mutex);

    /* welcome the user */
    send_msg(c->fd, "#all", c->name, "Welcome to the chat!");
}

/* SET: client wants to change their status */
void handle_set(struct account_user *c, char *fields[], int nfields) {
    if (!c->logged_in) {
        send_err(c->fd, 0, "Must set name first");
        return;
    }

    char *new_status = (nfields >= 1) ? fields[0] : "";

    if (!valid_text(new_status, MAX_STATUS)) {
        if (strlen(new_status) > MAX_STATUS)
            send_err(c->fd, 4, "Status too long");
        else
            send_err(c->fd, 3, "Illegal character in status");
        return;
    }

    pthread_mutex_lock(&clients_mutex);
    strncpy(c->status, new_status, MAX_STATUS);
    c->status[MAX_STATUS] = '\0';
    pthread_mutex_unlock(&clients_mutex);

    /* only broadcast if status is non-empty */
    if (strlen(new_status) > 0) {
        char announcement[256];
        snprintf(announcement, sizeof(announcement),
                 "%s is now \"%s\"", c->name, new_status);
        broadcast_msg("#all", "#all", announcement);
    }
}

/* MSG: client is sending a message to a user or to #all */
void handle_msg(struct account_user *c, char *fields[], int nfields) {
    if (!c->logged_in) {
        send_err(c->fd, 0, "Must set name first");
        return;
    }

    /* fields[0] = sender (ignored from client)
       fields[1] = recipient
       fields[2] = message body */
    if (nfields < 3) {
        send_err(c->fd, 0, "MSG requires sender, recipient, and body");
        return;
    }

    char *recipient = fields[1];
    char *body      = fields[2];

    /* validate body */
    if (strlen(body) < 1) {
        send_err(c->fd, 0, "Message cannot be empty");
        return;
    }
    if (!valid_text(body, MAX_MSG)) {
        if (strlen(body) > MAX_MSG)
            send_err(c->fd, 4, "Message too long");
        else
            send_err(c->fd, 3, "Illegal character in message");
        return;
    }

    if (strcmp(recipient, "#all") == 0) {
        /* broadcast to everyone */
        broadcast_msg(c->name, "#all", body);

    } else {
        /* private message: find the recipient */
        pthread_mutex_lock(&clients_mutex);
        struct account_user *dest = find_by_name_locked(recipient);
        if (dest == NULL) {
            pthread_mutex_unlock(&clients_mutex);
            send_err(c->fd, 2, "Unknown recipient");
            return;
        }
        int dest_fd = dest->fd;
        pthread_mutex_unlock(&clients_mutex);

        send_msg(dest_fd, c->name, recipient, body);
    }
}

/* WHO: client wants info about a user or about #all */
void handle_who(struct account_user *c, char *fields[], int nfields) {
    if (!c->logged_in) {
        send_err(c->fd, 0, "Must set name first");
        return;
    }

    if (nfields < 1) {
        send_err(c->fd, 0, "WHO requires a name or room");
        return;
    }

    char *target = fields[0];

    if (strcmp(target, "#all") == 0) {
        /* list every logged-in user with their status */
        char response[99999];
        int  pos = 0;
        response[0] = '\0';

        pthread_mutex_lock(&clients_mutex);
        struct account_user *cur = head;
        while (cur != NULL) {
            if (cur->logged_in) {
                if (pos > 0) {
                    /* newline separator between entries */
                    response[pos++] = '\n';
                    response[pos]   = '\0';
                }
                if (strlen(cur->status) > 0) {
                    pos += snprintf(response + pos, sizeof(response) - pos,
                                    "%s: %s", cur->name, cur->status);
                } else {
                    pos += snprintf(response + pos, sizeof(response) - pos,
                                    "%s", cur->name);
                }
            }
            cur = cur->next;
        }
        pthread_mutex_unlock(&clients_mutex);

        send_msg(c->fd, "#all", c->name, response);

    } else {
        /* single user lookup */
        pthread_mutex_lock(&clients_mutex);
        struct account_user *found = find_by_name_locked(target);

        if (found == NULL) {
            pthread_mutex_unlock(&clients_mutex);
            send_err(c->fd, 2, "Unknown user");
            return;
        }

        char response[256];
        if (strlen(found->status) > 0) {
            snprintf(response, sizeof(response),
                     "%s: %s", found->name, found->status);
        } else {
            snprintf(response, sizeof(response), "No status");
        }
        pthread_mutex_unlock(&clients_mutex);

        send_msg(c->fd, "#all", c->name, response);
    }
}


void *handle_client(void *arg) {
    struct account_user *c = (struct account_user *)arg;

    while (1) {
        char  type[4];
        char *fields[16];
        char *body = NULL;

        int nfields = parse_message(c->fd, type, fields, 16, &body);

        if (nfields < 0) {
            /* protocol error or connection closed */
            if (body == NULL) {
                /* clean disconnect — no ERR needed */
            } else {
                send_err(c->fd, 0, "Unreadable message");
            }
            free(body);
            break;
        }

        /* dispatch to the right handler */
        if (strcmp(type, "NAM") == 0) {
            handle_nam(c, fields, nfields);

        } else if (strcmp(type, "SET") == 0) {
            handle_set(c, fields, nfields);

        } else if (strcmp(type, "MSG") == 0) {
            handle_msg(c, fields, nfields);

        } else if (strcmp(type, "WHO") == 0) {
            handle_who(c, fields, nfields);

        } else {
            /* unknown message type — fatal error 0 */
            send_err(c->fd, 0, "Unknown message type");
            free(body);
            break;
        }

        free(body);
    }

    /* ── clean up on disconnect ── */
    pthread_mutex_lock(&clients_mutex);
    struct account_user *prev = NULL;
    struct account_user *cur  = head;
    while (cur != NULL) {
        if (cur->fd == c->fd) {
            if (prev == NULL) head       = cur->next;
            else              prev->next = cur->next;
            break;
        }
        prev = cur;
        cur  = cur->next;
    }
    pthread_mutex_unlock(&clients_mutex);

    close(c->fd);
    free(c);
    return NULL;
}

int main(int argc, char *argv[]) {

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int port          = atoi(argv[1]);
    int server_socket = setting_socket(&port);

    printf("chatd listening on port %d\n", port);

    while (1) {

        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int connect_sock = accept(server_socket,
                                  (struct sockaddr *)&client_addr,
                                  &client_len);

        if (connect_sock < 0) {
            perror("accept()");
            continue;
        }

        struct account_user *c = malloc(sizeof(struct account_user));
        if (!c) {
            close(connect_sock);
            continue;
        }
        c->fd        = connect_sock;   /* FIX: was  c->fd = fd  */
        c->name[0]   = '\0';
        c->status[0] = '\0';
        c->logged_in = 0;
        c->next      = NULL;

        /* add to linked list before spawning thread */
        pthread_mutex_lock(&clients_mutex);
        c->next = head;
        head    = c;
        pthread_mutex_unlock(&clients_mutex);

        pthread_t tid;
        pthread_create(&tid, NULL, handle_client, c);
        pthread_detach(tid);
    }

    return 0;
}