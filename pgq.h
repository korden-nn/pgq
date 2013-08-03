#ifndef PGQ_H_INCLUDED
#define PGQ_H_INCLUDED

#include <stdint.h>
#include <postgresql/libpq-fe.h>
#include <postgresql/pgtypes_timestamp.h>
#include <postgresql/pgtypes_interval.h>

typedef int64_t   batch_id_t;
typedef int64_t   event_id_t;
typedef int64_t   tick_id_t;
typedef int64_t   seq_t;

#define MAX_QUEUE_NAME_LENGTH     256
#define MAX_CONSUMER_NAME_LENGTH  256

#define MAX_EVENT_TYPE_LENGTH     16
#define MAX_EVENT_DATA_LENGTH     1024
#define MAX_EVENT_EXTRA_LENGTH    128

#ifdef __cplusplus
extern "C" {
#endif

/* Returns error number which occured during last call */
extern int get_error_number();
/* Returns text of error which occured during last call */
extern const char* get_error_text();

/* Returns version of PgQ (skytools) or NULL if fails */
extern char* get_version(PGconn* conn);

/*
Initialize event queue.
Returns
  0  - if event queue already exists
  1  - if queue has been just created
  -1 - if fails
*/
extern int create_queue(PGconn* conn, const char* queue_name);

/* Drop queue and all associated tables. No consumer must be listening on the queue. */
extern int drop_queue(PGconn* conn, const char* queue_name);

/* Drop queue and all associated tables. */
extern int drop_queue_force(PGconn* conn, const char* queue_name);

typedef struct {
  char        name[MAX_QUEUE_NAME_LENGTH];
  int32_t     ntables;
  int32_t     cur_table;
  interval    rotation_period;
  timestamp   switch_time;
  int         external_ticker;
  int         ticker_paused;
  int32_t     ticker_max_count;
  interval    ticker_max_lag;
  interval    ticker_idle_period;
  interval    ticker_lag;
  double      ev_per_sec;
  int64_t     ev_new;
  tick_id_t   last_tick_id;
} queue_info_t;

extern void print_queue_info(FILE* f, queue_info_t* info);

/*
Returns info about all queues in DB
As a return value returns
  -1 - if DB operation fails
  -2 - if received amount of columns is not as expected
  -3 - if memory allocation unsuccess
  N  -  amount of filled in items in 'queues_info' array
*/
extern int get_queues_info(PGconn* conn, queue_info_t** queues_info);

/* Generate new event. */
extern event_id_t insert_event(PGconn* conn, const char* queue_name, const char* ev_type, const char* ev_data);
extern event_id_t insert_event_ex(PGconn* conn, const char* queue_name, const char* ev_type, const char* ev_data,
    const char* extra1, const char* extra2, const char* extra3, const char* extra4);

/*
Attaches this consumer to particular event queue.
Returns
  0  - if the consumer was already attached
  1  - if it is new attachment
  -1 - if fails
*/
extern int register_consumer(PGconn* conn, const char* queue_name, const char* consumer_name);

/*
Unregister and drop resources allocated to customer.
Returns
  N  - amount of unregistered (sub)consumers
  -1 - if fails
*/
extern int unregister_consumer(PGconn* conn, const char* queue_name, const char* consumer_name);

/* Structure which describes consumer */
typedef struct {
  char queue_name[MAX_QUEUE_NAME_LENGTH];
  char consumer_name[MAX_CONSUMER_NAME_LENGTH];
  interval    lag;
  interval    last_seen;
  tick_id_t   last_tick;
  batch_id_t  current_batch;
  tick_id_t   next_tick;
  int64_t     pending_events;
} consumer_info_t;

extern void print_consumer_info(FILE* f, consumer_info_t* info);

/*
As an output param 'consumers_info' returns set of consumers description as in structure above.
Returns
  N  - amount of records in consumers_info array
  -1 - if fails
*/
extern int get_consumers_info(PGconn* conn, const char* queue_name, consumer_info_t** consumers_info);

extern int get_consumer_info(PGconn* conn, const char* queue_name, const char* consumer_name, consumer_info_t** consumer_info);

/*
Allocates next batch of events to consumer.
Returns batch id, to be used in processing functions. If no batches are available, returns 0. That means that the ticker has not cut them yet. This is the appropriate moment for consumer to sleep.
*/
extern batch_id_t next_batch(PGconn* conn, const char* queue_name, const char* consumer_name);

/*
Put whole batch into retry queue to be processed again later
Returns
  N  - amount of events inserted to retry queue
  -1 - if fails
*/
extern int batch_retry(PGconn* conn, batch_id_t batch_id, int32_t retry_seconds);

/* Structure which describes event */
typedef struct {
  event_id_t  id;
  timestamp   time;
  int64_t     txid;
  int32_t     retry;
  char        type[MAX_EVENT_TYPE_LENGTH];
  char        data[MAX_EVENT_DATA_LENGTH];
  char        extra1[MAX_EVENT_EXTRA_LENGTH];
  char        extra2[MAX_EVENT_EXTRA_LENGTH];
  char        extra3[MAX_EVENT_EXTRA_LENGTH];
  char        extra4[MAX_EVENT_EXTRA_LENGTH];
} event_t;

extern void print_event(FILE* f, event_t* event);

/*
As an output param 'events' returns set of events in this batch.
There may be no events in the batch. This is normal. The batch must still be closed with pgq.finish_batch().

Returns
  0  - if there is no events
  N  - amount of events stored in array (memory is allocated into this function)
  -1 - if fails
*/
extern int get_batch_events(PGconn* conn, int64_t batch_id, event_t** events);

typedef struct {
  char        queue_name[MAX_QUEUE_NAME_LENGTH];
  char        consumer_name[MAX_CONSUMER_NAME_LENGTH];
  timestamp   batch_start;
  timestamp   batch_end;
  tick_id_t   prev_tick_id;
  tick_id_t   tick_id;
  interval    lag;
  seq_t       seq_start;
  seq_t       seq_end;
} batch_info_t;

extern void print_batch_info(FILE* f, batch_info_t* info);

/* Returns detailed info about a batch */
extern int get_batch_info(PGconn* conn, batch_id_t batch_id, batch_info_t** batch_info);

/*
Put the event into retry queue to be processed again later
Returns
  0  - event already in retry queue
  1  - success
  -1 - if fails
*/
extern int event_retry(PGconn* conn, batch_id_t batch_id, event_id_t event_id, int32_t retry_seconds);

/*
Tag batch as finished. Until this is not done, the consumer will get same batch again.
After calling finish_batch consumer cannot do any operations with events of that batch. All operations must be done before.
Returns
  0  - if batch not found
  1  - if batch successfully finished
  -1 - if fails
*/
extern int finish_batch(PGconn* conn, batch_id_t batch_id);

#ifdef __cplusplus
}
#endif

#endif
