#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "pgq.h"

#define MAX_QUERY_SIZE 1024
static char query[MAX_QUERY_SIZE];

static int error_number = 0;
#define MAX_ERROR_SIZE 1024
static char error_text[MAX_ERROR_SIZE];

static const char* INCORRECT_AMOUNT_OF_COLUMNS_ERR = "Incorrect amount of columns, awaited: %d, retreived: %d";
static const char* INCORRECT_AMOUNT_OF_RAWS_ERR = "Incorrect amount of raws, awaited: %d, retreived: %d";
static const char* MEMORY_ALLOC_ERR = "Could not allocate %d bytes";
static const char* TYPE_INTERVAL_ERROR = "Variable %s is not 'interval', value: %s";
static const char* TYPE_TIMESTAMP_ERROR = "Variable %s is not 'timestamp', value: %s";

#define ARRAY_SIZE(array) (sizeof(array)/sizeof(array[0]))

/* PgQ queries */
static const char* GET_VERSION_QUERY = "select pgq.version()";
static const char* CREATE_QUEUE_QUERY = "select pgq.create_queue('%s')";
static const char* DROP_QUEUE_QUERY = "select pgq.drop_queue('%s')";
static const char* DROP_QUEUE_FORCE_QUERY = "select pgq.drop_queue('%s', True)";

#define GET_QUEUE_INFO_COLUMNS 14
static const char* GET_QUEUE_INFO_QUERY = "select * from pgq.get_queue_info()";

static const char* INSERT_EVENT_QUERY = "select pgq.insert_event('%s', '%s', '%s')";
static const char* INSERT_EVENT_EX_QUERY = "select pgq.insert_event('%s', '%s', '%s', '%s', '%s', '%s', '%s')";
static const char* REGISTER_CONSUMER_QUERY = "select pgq.register_consumer('%s', '%s')";
static const char* UNREGISTER_CONSUMER_QUERY = "select pgq.unregister_consumer('%s', '%s')";

#define GET_CONSUMER_INFO_COLUMNS 8
static const char* GET_CONSUMER_INFO_QUERY = "select * from pgq.get_consumer_info('%s', '%s')";
static const char* GET_CONSUMERS_INFO_QUERY = "select * from pgq.get_consumer_info('%s')";

static const char* NEXT_BATCH_QUERY = "select pgq.next_batch('%s', '%s')";
static const char* BATCH_RETRY_QUERY = "select pgq.batch_retry(%ld, %d)";

#define GET_BATCH_EVENTS_COLUMNS 10
static const char* GET_BATCH_EVENTS_QUERY = "select * from pgq.get_batch_events(%ld)";

#define GET_BATCH_INFO_COLUMNS 9
static const char* GET_BATCH_INFO_QUERY = "select * from pgq.get_batch_info(%ld)";

static const char* EVENT_RETRY_QUERY = "select pgq.event_retry(%ld, %ld, %d)";
static const char* FINISH_BATCH_QUERY = "select pgq.finish_batch(%ld)";

#define SAVE_INTERVAL(dst, tmp, value, name) \
  tmp = PGTYPESinterval_from_asc(value, NULL); \
  if (!tmp) { \
    snprintf(error_text, ARRAY_SIZE(error_text), TYPE_INTERVAL_ERROR, name, value); \
    PQclear(result); \
    return (-4); \
  } \
  PGTYPESinterval_copy(tmp, dst); \
  PGTYPESinterval_free(tmp)

#define SAVE_TIMESTAMP(dst, value, name) \
  *dst = PGTYPEStimestamp_from_asc(value, NULL); \
  if (*dst == 0) { \
    snprintf(error_text, ARRAY_SIZE(error_text), TYPE_TIMESTAMP_ERROR, name, value); \
    PQclear(result); \
    return (-4); \
  }

int get_error_number() {
  return error_number;
}

const char* get_error_text() {
  return error_text;
}

void print_queue_info(FILE* f, queue_info_t* info) {
  char temp[256];
  fprintf(f, "queue name:           %s\n",  info->name);
  fprintf(f, "ntables:              %d\n",  info->ntables);
  fprintf(f, "cur_table:            %d\n",  info->cur_table);
  fprintf(f, "rotation_period:      %s\n",  PGTYPESinterval_to_asc(&info->rotation_period));
  PGTYPEStimestamp_fmt_asc(&info->switch_time, temp, ARRAY_SIZE(temp), "%Y-%m-%d %T");
  fprintf(f, "switch_time:          %s\n",  temp);
  fprintf(f, "external_ticker:      %d\n",  info->external_ticker);
  fprintf(f, "ticker_paused:        %d\n",  info->ticker_paused);
  fprintf(f, "ticker_max_count:     %d\n",  info->ticker_max_count);
  fprintf(f, "ticker_max_lag:       %s\n",  PGTYPESinterval_to_asc(&info->ticker_max_lag));
  fprintf(f, "ticker_idle_period:   %s\n",  PGTYPESinterval_to_asc(&info->ticker_idle_period));
  fprintf(f, "ticker_lag:           %s\n",  PGTYPESinterval_to_asc(&info->ticker_lag));
  fprintf(f, "ev_per_sec:           %f\n",  info->ev_per_sec);
  fprintf(f, "ev_new:               %ld\n", info->ev_new);
  fprintf(f, "last_tick_id:         %ld\n", info->last_tick_id);
}

void print_consumer_info(FILE* f, consumer_info_t* info) {
  fprintf(f, "queue name:           %s\n",  info->queue_name);
  fprintf(f, "consumer name:        %s\n",  info->consumer_name);
  fprintf(f, "lag:                  %s\n",  PGTYPESinterval_to_asc(&info->lag));
  fprintf(f, "last_seen:            %s\n",  PGTYPESinterval_to_asc(&info->last_seen));
  fprintf(f, "last_tick:            %ld\n", info->last_tick);
  fprintf(f, "current_batch:        %ld\n", info->current_batch);
  fprintf(f, "next_tick:            %ld\n", info->next_tick);
  fprintf(f, "pending_events:       %ld\n", info->pending_events);
}

void print_event(FILE* f, event_t* event) {
  char temp[256];
  fprintf(f, "id:                   %ld\n", event->id);
  PGTYPEStimestamp_fmt_asc(&event->time, temp, ARRAY_SIZE(temp), "%Y-%m-%d %T");
  fprintf(f, "time:                 %s\n",  temp);
  fprintf(f, "txid:                 %ld\n", event->txid);
  fprintf(f, "retry:                %d\n",  event->retry);
  fprintf(f, "type:                 %s\n",  event->type);
  fprintf(f, "data:                 %s\n",  event->data);
  fprintf(f, "extra1:               %s\n",  event->extra1);
  fprintf(f, "extra2:               %s\n",  event->extra2);
  fprintf(f, "extra3:               %s\n",  event->extra3);
  fprintf(f, "extra4:               %s\n",  event->extra4);
}

void print_batch_info(FILE* f, batch_info_t* info) {
  char temp[256];
  fprintf(f, "queue name:           %s\n",  info->queue_name);
  fprintf(f, "consumer name:        %s\n",  info->consumer_name);
  PGTYPEStimestamp_fmt_asc(&info->batch_start, temp, ARRAY_SIZE(temp), "%Y-%m-%d %T");
  fprintf(f, "batch_start:          %s\n",  temp);
  PGTYPEStimestamp_fmt_asc(&info->batch_end, temp, ARRAY_SIZE(temp), "%Y-%m-%d %T");
  fprintf(f, "batch_end:            %s\n",  temp);
  fprintf(f, "prev_tick_id:         %ld\n", info->prev_tick_id);
  fprintf(f, "tick_id:              %ld\n", info->tick_id);
  fprintf(f, "lag:                  %s\n",  PGTYPESinterval_to_asc(&info->lag));
  fprintf(f, "seq_start:            %ld\n", info->seq_start);
  fprintf(f, "seq_end:              %ld\n", info->seq_end);
}

static int execute_and_get_int_result(PGconn* conn, const char* cmd) {
  int ret = -1;
  PGresult* result = PQexec(conn, cmd);
  if (PQresultStatus(result) == PGRES_TUPLES_OK) {
    ret = atoi(PQgetvalue(result, 0, 0));
  } else {
    error_number = PQresultStatus(result);
    strncpy(error_text, PQresultErrorMessage(result), ARRAY_SIZE(error_text));
  }
  PQclear(result);
  return ret;
}

static long execute_and_get_long_result(PGconn* conn, const char* cmd) {
  long ret = -1;
  PGresult* result = PQexec(conn, cmd);
  if (PQresultStatus(result) == PGRES_TUPLES_OK) {
    ret = atol(PQgetvalue(result, 0, 0));
  } else {
    error_number = PQresultStatus(result);
    strncpy(error_text, PQresultErrorMessage(result), ARRAY_SIZE(error_text));
  }
  PQclear(result);
  return ret;
}

static char* execute_and_get_text_result(PGconn* conn, const char* cmd) {
  char* ret = NULL;
  PGresult* result = PQexec(conn, cmd);
  if (PQresultStatus(result) == PGRES_TUPLES_OK) {
    ret = (char*)PQgetvalue(result, 0, 0);
  } else {
    error_number = PQresultStatus(result);
    strncpy(error_text, PQresultErrorMessage(result), ARRAY_SIZE(error_text));
  }
  PQclear(result);
  return ret;
}

char* get_version(PGconn* conn) {
  return execute_and_get_text_result(conn, GET_VERSION_QUERY);
}

int create_queue(PGconn* conn, const char* queue_name) {
  snprintf(query, ARRAY_SIZE(query), CREATE_QUEUE_QUERY, queue_name);
  return execute_and_get_int_result(conn, query);
}

int drop_queue(PGconn* conn, const char* queue_name) {
  snprintf(query, ARRAY_SIZE(query), DROP_QUEUE_QUERY, queue_name);
  return execute_and_get_int_result(conn, query);
}

int drop_queue_force(PGconn* conn, const char* queue_name) {
  snprintf(query, ARRAY_SIZE(query), DROP_QUEUE_FORCE_QUERY, queue_name);
  return execute_and_get_int_result(conn, query);
}

int get_queues_info(PGconn* conn, queue_info_t** queues_info) {
  int size = -1, i, fields;
  interval* temp_interval;
  PGresult* result = PQexec(conn, GET_QUEUE_INFO_QUERY);
  if (PQresultStatus(result) == PGRES_TUPLES_OK) {
    size = PQntuples(result);
    fields = PQnfields(result);
    if (fields != GET_QUEUE_INFO_COLUMNS) {
      snprintf(error_text, ARRAY_SIZE(error_text), INCORRECT_AMOUNT_OF_COLUMNS_ERR,
          GET_QUEUE_INFO_COLUMNS, fields);
      PQclear(result);
      return -2;
    }
    *queues_info = (queue_info_t*)malloc(size*sizeof(queue_info_t));
    if (!*queues_info) {
      snprintf(error_text, ARRAY_SIZE(error_text), MEMORY_ALLOC_ERR, size*sizeof(queue_info_t));
      PQclear(result);
      return -3;
    }
    for (i = 0; i < size; ++i) {
      strncpy(queues_info[i]->name, (char*)PQgetvalue(result, i, 0), ARRAY_SIZE(queues_info[i]->name));
      queues_info[i]->ntables = atoi(PQgetvalue(result, i, 1));
      queues_info[i]->cur_table = atoi(PQgetvalue(result, i, 2));
      SAVE_INTERVAL(&queues_info[i]->rotation_period, temp_interval, PQgetvalue(result, i, 3), "rotation_period");
      SAVE_TIMESTAMP(&queues_info[i]->switch_time, PQgetvalue(result, i, 4), "switch_time");
      queues_info[i]->external_ticker = (int)atoi(PQgetvalue(result, i, 5));
      queues_info[i]->ticker_paused = (int)atoi(PQgetvalue(result, i, 6));
      queues_info[i]->ticker_max_count = atoi(PQgetvalue(result, i, 7));
      SAVE_INTERVAL(&queues_info[i]->ticker_max_lag, temp_interval, PQgetvalue(result, i, 8), "ticker_max_lag");
      SAVE_INTERVAL(&queues_info[i]->ticker_idle_period, temp_interval, PQgetvalue(result, i, 9), "ticker_idle_period");
      SAVE_INTERVAL(&queues_info[i]->ticker_lag, temp_interval, PQgetvalue(result, i, 10), "ticker_lag");
      queues_info[i]->ev_per_sec = atof(PQgetvalue(result, i, 11));
      queues_info[i]->ev_new = atol(PQgetvalue(result, i, 12));
      queues_info[i]->last_tick_id = (tick_id_t)atol(PQgetvalue(result, i, 13));
    }
  } else {
    error_number = PQresultStatus(result);
    strncpy(error_text, PQresultErrorMessage(result), ARRAY_SIZE(error_text));
  }
  PQclear(result);
  return size;
}

long insert_event(PGconn* conn, const char* queue_name, const char* ev_type, const char* ev_data) {
  snprintf(query, ARRAY_SIZE(query), INSERT_EVENT_QUERY, queue_name, queue_name, ev_type, ev_data);
  return execute_and_get_long_result(conn, query);
}

long insert_event_ex(PGconn* conn, const char* queue_name, const char* ev_type, const char* ev_data,
    const char* extra1, const char* extra2, const char* extra3, const char* extra4) {
  snprintf(query, ARRAY_SIZE(query), INSERT_EVENT_EX_QUERY, queue_name, queue_name, ev_type, ev_data,
      extra1, extra2, extra3, extra4);
  return execute_and_get_long_result(conn, query);
}

int register_consumer(PGconn* conn, const char* queue_name, const char* consumer_name) {
  snprintf(query, ARRAY_SIZE(query), REGISTER_CONSUMER_QUERY, queue_name, consumer_name);
  return execute_and_get_int_result(conn, query);
}

int unregister_consumer(PGconn* conn, const char* queue_name, const char* consumer_name) {
  snprintf(query, ARRAY_SIZE(query), UNREGISTER_CONSUMER_QUERY, queue_name, consumer_name);
  return execute_and_get_int_result(conn, query);
}

int get_consumer_info(PGconn* conn, const char* queue_name, const char* consumer_name, consumer_info_t** consumer_info) {
  int size = -1, i, fields;
  interval* temp_interval;
  PGresult* result;

  snprintf(query, ARRAY_SIZE(query), GET_CONSUMER_INFO_QUERY, queue_name, consumer_name);
  result = PQexec(conn, query);
  if (PQresultStatus(result) == PGRES_TUPLES_OK) {
    size = PQntuples(result);
    fields = PQnfields(result);
    if (fields != GET_CONSUMER_INFO_COLUMNS) {
      snprintf(error_text, ARRAY_SIZE(error_text), INCORRECT_AMOUNT_OF_COLUMNS_ERR,
          GET_CONSUMER_INFO_COLUMNS, fields);
      PQclear(result);
      return -2;
    }
    *consumer_info = (consumer_info_t*)malloc(size*sizeof(consumer_info_t));
    if (!*consumer_info) {
      snprintf(error_text, ARRAY_SIZE(error_text), MEMORY_ALLOC_ERR, size*sizeof(consumer_info_t));
      PQclear(result);
      return -3;
    }
    for (i = 0; i < size; ++i) {
      strncpy(consumer_info[i]->queue_name, (char*)PQgetvalue(result, i, 0), ARRAY_SIZE(consumer_info[i]->queue_name));
      strncpy(consumer_info[i]->consumer_name, (char*)PQgetvalue(result, i, 1), ARRAY_SIZE(consumer_info[i]->consumer_name));
      SAVE_INTERVAL(&consumer_info[i]->lag, temp_interval, PQgetvalue(result, i, 2), "lag");
      SAVE_INTERVAL(&consumer_info[i]->last_seen, temp_interval, PQgetvalue(result, i, 3), "last_seen");
      consumer_info[i]->last_tick = (tick_id_t)atol(PQgetvalue(result, i, 4));
      consumer_info[i]->current_batch = (batch_id_t)atol(PQgetvalue(result, i, 5));
      consumer_info[i]->next_tick = (tick_id_t)atol(PQgetvalue(result, i, 6));
      consumer_info[i]->pending_events = atol(PQgetvalue(result, i, 7));
    }
  } else {
    error_number = PQresultStatus(result);
    strncpy(error_text, PQresultErrorMessage(result), ARRAY_SIZE(error_text));
  }
  PQclear(result);
  return size;
}

int get_consumers_info(PGconn* conn, const char* queue_name, consumer_info_t** consumers_info) {
  int size = -1, i, fields;
  interval* temp_interval;
  PGresult* result;

  snprintf(query, ARRAY_SIZE(query), GET_CONSUMERS_INFO_QUERY, queue_name);
  result = PQexec(conn, query);
  if (PQresultStatus(result) == PGRES_TUPLES_OK) {
    size = PQntuples(result);
    fields = PQnfields(result);
    if (fields != GET_CONSUMER_INFO_COLUMNS) {
      snprintf(error_text, ARRAY_SIZE(error_text), INCORRECT_AMOUNT_OF_COLUMNS_ERR,
          GET_CONSUMER_INFO_COLUMNS, fields);
      PQclear(result);
      return -2;
    }
    *consumers_info = (consumer_info_t*)malloc(size*sizeof(consumer_info_t));
    if (!*consumers_info) {
      snprintf(error_text, ARRAY_SIZE(error_text), MEMORY_ALLOC_ERR, size*sizeof(consumer_info_t));
      PQclear(result);
      return -3;
    }
    for (i = 0; i < size; ++i) {
      strncpy(consumers_info[i]->queue_name, (char*)PQgetvalue(result, i, 0), ARRAY_SIZE(consumers_info[i]->queue_name));
      strncpy(consumers_info[i]->consumer_name, (char*)PQgetvalue(result, i, 1), ARRAY_SIZE(consumers_info[i]->consumer_name));
      SAVE_INTERVAL(&consumers_info[i]->lag, temp_interval, PQgetvalue(result, i, 2), "lag");
      SAVE_INTERVAL(&consumers_info[i]->last_seen, temp_interval, PQgetvalue(result, i, 3), "last_seen");
      consumers_info[i]->last_tick = (tick_id_t)atol(PQgetvalue(result, i, 4));
      consumers_info[i]->current_batch = (batch_id_t)atol(PQgetvalue(result, i, 5));
      consumers_info[i]->next_tick = (tick_id_t)atol(PQgetvalue(result, i, 6));
      consumers_info[i]->pending_events = atol(PQgetvalue(result, i, 7));
    }
  } else {
    error_number = PQresultStatus(result);
    strncpy(error_text, PQresultErrorMessage(result), ARRAY_SIZE(error_text));
  }
  PQclear(result);
  return size;
}

batch_id_t next_batch(PGconn* conn, const char* queue_name, const char* consumer_name) {
  snprintf(query, ARRAY_SIZE(query), NEXT_BATCH_QUERY, queue_name, consumer_name);
  return (batch_id_t)execute_and_get_long_result(conn, query);
}

int batch_retry(PGconn* conn, batch_id_t batch_id, int32_t retry_seconds) {
  snprintf(query, ARRAY_SIZE(query), BATCH_RETRY_QUERY, batch_id, retry_seconds);
  return execute_and_get_int_result(conn, query);
}

int get_batch_events(PGconn* conn, int64_t batch_id, event_t** events) {
  int size = -1, i, fields;
  PGresult* result;

  snprintf(query, ARRAY_SIZE(query), GET_BATCH_EVENTS_QUERY, batch_id);
  result = PQexec(conn, query);
  if (PQresultStatus(result) == PGRES_TUPLES_OK) {
    size = PQntuples(result);
    fields = PQnfields(result);
    if (fields != GET_BATCH_EVENTS_COLUMNS) {
      snprintf(error_text, ARRAY_SIZE(error_text), INCORRECT_AMOUNT_OF_COLUMNS_ERR,
          GET_BATCH_EVENTS_COLUMNS, fields);
      PQclear(result);
      return -2;
    }
    *events = (event_t*)malloc(size*sizeof(event_t));
    if (!*events) {
      snprintf(error_text, ARRAY_SIZE(error_text), MEMORY_ALLOC_ERR, size*sizeof(event_t));
      PQclear(result);
      return -3;
    }
    for (i = 0; i < size; ++i) {
      (*events)[i].id = (event_id_t)atol(PQgetvalue(result, i, 0));
      SAVE_TIMESTAMP(&(*events)[i].time, PQgetvalue(result, i, 1), "time");
      (*events)[i].txid = atol(PQgetvalue(result, i, 2));
      (*events)[i].retry = atoi(PQgetvalue(result, i, 3));
      strncpy((*events)[i].type, (char*)PQgetvalue(result, i, 4), ARRAY_SIZE((*events)[i].type));
      strncpy((*events)[i].data, (char*)PQgetvalue(result, i, 5), ARRAY_SIZE((*events)[i].data));
      strncpy((*events)[i].extra1, (char*)PQgetvalue(result, i, 6), ARRAY_SIZE((*events)[i].extra1));
      strncpy((*events)[i].extra2, (char*)PQgetvalue(result, i, 7), ARRAY_SIZE((*events)[i].extra2));
      strncpy((*events)[i].extra3, (char*)PQgetvalue(result, i, 8), ARRAY_SIZE((*events)[i].extra3));
      strncpy((*events)[i].extra4, (char*)PQgetvalue(result, i, 9), ARRAY_SIZE((*events)[i].extra4));
    }
  } else {
    error_number = PQresultStatus(result);
    strncpy(error_text, PQresultErrorMessage(result), ARRAY_SIZE(error_text));
  }
  PQclear(result);
  return size;
}

int get_batch_info(PGconn* conn, batch_id_t batch_id, batch_info_t** batch_info) {
  int size = -1, fields;
  interval* temp_interval;
  PGresult* result;

  snprintf(query, ARRAY_SIZE(query), GET_BATCH_INFO_QUERY, batch_id);
  result = PQexec(conn, query);
  if (PQresultStatus(result) == PGRES_TUPLES_OK) {
    size = PQntuples(result);
    fields = PQnfields(result);
    if (fields != GET_BATCH_INFO_COLUMNS) {
      snprintf(error_text, ARRAY_SIZE(error_text), INCORRECT_AMOUNT_OF_COLUMNS_ERR,
          GET_BATCH_INFO_COLUMNS, fields);
      PQclear(result);
      return -2;
    }
    if (size != 1) {
      snprintf(error_text, ARRAY_SIZE(error_text), INCORRECT_AMOUNT_OF_RAWS_ERR,
          1, size);
      PQclear(result);
      return -2;
    }
    *batch_info = (batch_info_t*)malloc(sizeof(batch_info_t));
    if (!*batch_info) {
      snprintf(error_text, ARRAY_SIZE(error_text), MEMORY_ALLOC_ERR, sizeof(batch_info_t));
      PQclear(result);
      return -3;
    }
    strncpy(batch_info[0]->queue_name, (char*)PQgetvalue(result, 0, 0), ARRAY_SIZE(batch_info[0]->queue_name));
    strncpy(batch_info[0]->consumer_name, (char*)PQgetvalue(result, 0, 1), ARRAY_SIZE(batch_info[0]->consumer_name));
    SAVE_TIMESTAMP(&batch_info[0]->batch_start, PQgetvalue(result, 0, 2), "batch_start");
    SAVE_TIMESTAMP(&batch_info[0]->batch_end, PQgetvalue(result, 0, 3), "batch_end");
    batch_info[0]->prev_tick_id = (tick_id_t)atol(PQgetvalue(result, 0, 4));
    batch_info[0]->tick_id = (tick_id_t)atol(PQgetvalue(result, 0, 5));
    SAVE_INTERVAL(&batch_info[0]->lag, temp_interval, PQgetvalue(result, 0, 6), "lag");
    batch_info[0]->seq_start = (seq_t)atol(PQgetvalue(result, 0, 7));
    batch_info[0]->seq_end = (seq_t)atol(PQgetvalue(result, 0, 8));
  } else {
    error_number = PQresultStatus(result);
    strncpy(error_text, PQresultErrorMessage(result), ARRAY_SIZE(error_text));
  }
  PQclear(result);
  return size;
}

int event_retry(PGconn* conn, batch_id_t batch_id, event_id_t event_id, int32_t retry_seconds) {
  snprintf(query, ARRAY_SIZE(query), EVENT_RETRY_QUERY, batch_id, event_id, retry_seconds);
  return execute_and_get_int_result(conn, query);
}

int finish_batch(PGconn* conn, batch_id_t batch_id) {
  snprintf(query, ARRAY_SIZE(query), FINISH_BATCH_QUERY, batch_id);
  return execute_and_get_int_result(conn, query);
}
