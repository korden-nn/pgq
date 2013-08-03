#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

#include "pgq.h"

int main(int argc, char* argv[]) {
  int ret = 0, i;
  queue_info_t* info;
  consumer_info_t* c_info;
  batch_id_t bid;
  event_t* events;
  batch_info_t* batch_info;
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
  ret = register_consumer(conn, "test_queue", "test_consumer");
  printf("ret = %d\n", ret);
  if (ret == -1) {
    fprintf(stderr, "%s (%d)", get_error_text(), get_error_number());
  }
  /*event_id = insert_event(conn, "test_queue", "type", "data");
  printf("event_id = %ld\n", event_id);
  event_id = insert_event_ex(conn, "test_queue", "2", "111", "1", "2", "3", "4");
  printf("event_id = %ld\n", event_id);*/
  ret = get_queues_info(conn, &info);
  printf("queues info: ret=%d\n", ret);
  if (ret < 0)
    fprintf(stderr, "Error: %s\n", get_error_text());
  for (i = 0; i < ret; ++i) {
    printf("queue %d\n", i+1);
    print_queue_info(stdout, &info[i]);
    printf("\n\n");
  }
  ret = get_consumers_info(conn, "test_queue", &c_info);
  printf("consumer info: ret=%d\n", ret);
  if (ret < 0)
    fprintf(stderr, "Error: %s\n", get_error_text());
  for (i = 0; i < ret; ++i) {
    printf("consumer %d\n", i+1);
    print_consumer_info(stdout, &c_info[i]);
    printf("\n\n");
  }

  while (1) {
    bid = next_batch(conn, "test_queue", "test_consumer");
    if (bid == 0) {
      printf("No batches available\n");
      sleep(3);
    } else if (bid < 0) {
      printf("Failed: %s\n", get_error_text());
      break;
    } else {
      ret = get_batch_info(conn, bid, &batch_info);
      if (ret < 0) {
        printf("get_batch_info() failed: %s\n", get_error_text());
        break;
      }
      printf("Batch info:\n");
      print_batch_info(stdout, batch_info);
      printf("\n\n");
      ret = get_batch_events(conn, bid, &events);
      if (ret < 0) {
        printf("Failed retreiving events, %s\n", get_error_text());
        break;
      } else {
        printf("Available events: %d\n", ret);
        for (i = 0; i < ret; ++i) {
          printf("Event %d\n", i+1);
          print_event(stdout, &events[i]);
          printf("\n\n");
        }
        ret = finish_batch(conn, bid);
        if (ret < 0) {
          printf("Finish batch failed: %s\n", get_error_text());
          break;
        } else if (ret == 0) {
          printf("Batch has not been finished\n");
          continue;
        } else {
          printf("Batch %ld has been finished\n", bid);
        }
      }
    }
  }

  PQfinish(conn);
  return 0;
}
