#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

#include "pgq.h"

int main(int argc, char* argv[]) {
  int ret = 0;
  event_id_t event_id;
  PGconn* conn = PQconnectdb("dbname=test user=postgres port=5433");
  if (!conn) {
    fprintf(stderr, "Could not open DB connection\n");
    return 1;
  }
  ret = create_queue(conn, "test_queue");
  switch (ret) {
    case 0: printf("Queue already exists\n"); break;
    case 1: printf("Queue has been created\n"); break;
    case -1: printf("Failed to create queue\n"); break;
  }
  while (1) {
    event_id = insert_event(conn, "test_queue", "type", "data");
    printf("event_id = %ld\n", event_id);
    event_id = insert_event_ex(conn, "test_queue", "2", "111", "1", "2", "3", "4");
    printf("event_id = %ld\n", event_id);
    sleep(3);
  }

  PQfinish(conn);
  return 0;
}
