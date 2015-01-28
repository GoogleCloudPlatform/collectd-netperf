/* TODO(arielshaqed): License and other boilerplate
 *
 * https://collectd.org/wiki/index.php/Plugin_architecture says:
 *
 * "All *.c-files must include a copyright notice and license
 * information. The license must be compatible to collectd's own
 * license, the GPL 2. Unless you have a good reason to split up your
 * plugin into multiple files, please put everything into one .c-file."
 */

#include "grpc/grpc.h"
#include "grpc/grpc_security.h"
#include "grpc/support/alloc.h"
#include "grpc/support/host_port.h"
#include "grpc/support/sync.h"

#include "nanopb/pb.h"
#include "nanopb/pb_encode.h"
#include "grpc_data.pb.h"

#include "collectd.h"
#include "common.h"
#include "plugin.h"
#include "configfile.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#define PLUGIN_NAME "WriteGRPC"
#define MONITORING_SCOPES "https://www.googleapis.com/auth/monitoring.readonly"

/*
 * Configuration and workspace
 */
typedef struct grpc_callback {
  char *host;
  grpc_credentials *cred;
  grpc_channel *channel;
  gpr_mu mutex;  /* Locks cq and *_counter, for concurrent writes */
  grpc_completion_queue *cq;
  size_t write_counter;
  size_t write_accepted_counter;
  double deadline;
} grpc_callback;

static void destroy_cb(void *arg)
{
  grpc_callback *cb = (grpc_callback *)arg;
  free(cb->host);
  if (cb->cred) grpc_credentials_release(cb->cred);
  if (cb->channel) grpc_channel_destroy(cb->channel);
  gpr_mu_destroy(&cb->mutex);
  if (cb->cq) grpc_completion_queue_destroy(cb->cq);
  free(cb);
}

/* ---------------- nanopb probotobuf encoders ---------------- */

typedef struct data_set_and_value_list_s {
  const data_set_t *ds;
  const value_list_t* vl;
} data_set_and_value_list_t;

static bool encode_fixed_string(
    pb_ostream_t *stream, const pb_field_t *field, void * const *arg) {
  const char *string = *(char * const*)arg;
  if (!pb_encode_tag_for_field(stream, field))
    return false;
  if (!pb_encode_string(stream, (uint8_t *)string, strlen(string)))
    return false;

  return true;
}

/* Returns a callback that will encode string. Does not take ownership of
 * string, which must live until after the callback is called. */
static pb_callback_t make_string(const char *string) {
  pb_callback_t ret;
  ret.funcs.encode = encode_fixed_string;
  ret.arg = (void*)string;
  return ret;
}

static _Bool encode_int_values(
    pb_ostream_t *stream, const pb_field_t *field, void * const *arg) {
  const data_set_and_value_list_t *ds_and_vl =
      *(data_set_and_value_list_t * const *)arg;
  const data_set_t *ds = ds_and_vl->ds;
  const value_list_t *vl = ds_and_vl->vl;
  google_internal_cloudlatencytest_v2_IntValue int_value;
  size_t i;

  for (i = 0; i < ds->ds_num; i++) {
    int64_t value;
    switch (ds->ds[i].type) {  /* continue to skip this value */
      case DS_TYPE_COUNTER:
        value = vl->values[i].counter;
        break;
      case DS_TYPE_DERIVE:
        value = vl->values[i].derive;
        break;
      case DS_TYPE_ABSOLUTE:
        value = vl->values[i].absolute;
        break;
      case DS_TYPE_GAUGE:
        continue;
      default:
        ERROR("Data set %s source %s: unknown type %d",
              ds->type, ds->ds[i].name, ds->ds[i].type);
        continue;
    }

      int_value.label = make_string(ds->ds[i].name);
      int_value.has_value = true;
      int_value.value = value;
      if (!pb_encode_tag_for_field(stream, field))
        return false;
      if (!pb_encode_submessage(
              stream,
              google_internal_cloudlatencytest_v2_IntValue_fields,
              &int_value))
        return false;
  }
  return true;
}

static bool encode_double_values(
    pb_ostream_t *stream, const pb_field_t *field, void * const *arg) {
  const data_set_and_value_list_t *ds_and_vl =
      *(data_set_and_value_list_t * const *)arg;
  const data_set_t *ds = ds_and_vl->ds;
  const value_list_t *vl = ds_and_vl->vl;
  google_internal_cloudlatencytest_v2_DoubleValue double_value;
  size_t i;

  for (i = 0; i < ds->ds_num; i++) {
    if (ds->ds[i].type == DS_TYPE_GAUGE) {
      double_value.label = make_string(ds->ds[i].name);
      double_value.has_value = true;
      double_value.value = vl->values[i].gauge;
      if (!pb_encode_tag_for_field(stream, field))
        return false;
      if (!pb_encode_submessage(
              stream,
              google_internal_cloudlatencytest_v2_DoubleValue_fields,
              &double_value))
        return false;
    }
  }
  return true;
}

bool make_and_encode_string_value(
    pb_ostream_t *stream, const pb_field_t *field,
    const char *label, const char *value) {
  google_internal_cloudlatencytest_v2_StringValue string_value;

  string_value.label = make_string(label);
  string_value.value = make_string(value);

  if (!pb_encode_tag_for_field(stream, field))
    return false;
  if (!pb_encode_submessage(
          stream,
          google_internal_cloudlatencytest_v2_StringValue_fields,
          &string_value))
    return false;
  return true;
}

bool encode_string_values(
    pb_ostream_t *stream, const pb_field_t *field, void * const *arg) {
  const data_set_and_value_list_t *ds_and_vl =
      *(data_set_and_value_list_t * const *)arg;
  const value_list_t *vl = ds_and_vl->vl;

  if (!make_and_encode_string_value(stream, field, "type", vl->type))
    return false;
  if (!make_and_encode_string_value(
          stream, field, "type_instance", vl->type_instance))
    return false;
  if (!make_and_encode_string_value(stream, field, "plugin", vl->plugin))
    return false;
  if (!make_and_encode_string_value(
          stream, field, "plugin_instance", vl->plugin_instance))
    return false;
  return true;
}

/* Encodes vl as a Stats protobuf to stream. */
static bool encode_value_list(
    pb_ostream_t *stream, const data_set_and_value_list_t* ds_and_vl) {
  google_internal_cloudlatencytest_v2_Stats stats;

  /* nanopb needs to encode every repeated field in a single call. This
   * means that we iterate multiple times over the value_list_t, once for
   * every reported type.
   */
  stats.time = CDTIME_T_TO_DOUBLE(ds_and_vl->vl->time);
  stats.int_values.funcs.encode = encode_int_values;
  stats.int_values.arg = (void*)ds_and_vl;
  stats.double_values.funcs.encode = encode_double_values;
  stats.double_values.arg = (void*)ds_and_vl;
  stats.string_values.funcs.encode = encode_string_values;
  stats.string_values.arg = (void*)ds_and_vl;
  /* If reporting AggregatedStats, will need to pb_encode_tag_for_field */
  if (!pb_encode(
          stream, google_internal_cloudlatencytest_v2_Stats_fields, &stats))
    return false;
  return true;
}

/* ---------------- gRPC : nanopb interface ---------------- */
typedef struct {
  gpr_slice *slice;
  gpr_uint8 *next;
} pb_ostream_callback_state_t;

/*
 * Writes message to stream.state, interpretted as a gpr_slice.  Fails once
 * slice is full (size of stream should have been set accordingly).
 */
static bool grpc_pb_ostream_callback(
    pb_ostream_t *stream, const uint8_t* buf, size_t count) {
  pb_ostream_callback_state_t *cb =
      (pb_ostream_callback_state_t *) stream->state;

  if (cb->next + count > GPR_SLICE_END_PTR(*cb->slice)) {
    fprintf(stderr, "Slice length %zu exceeded; write %zu; offset %zu",
            GPR_SLICE_LENGTH(*cb->slice),
            count,
            cb->next - GPR_SLICE_START_PTR(*cb->slice));
    return 0;
  }

  memcpy(cb->next, buf, count);
  cb->next += count;

  return 1;
}

/* Returns a deadline secs into the future */
static gpr_timespec get_deadline(double secs)
{
  const time_t now = time(NULL);
  gpr_timespec ret = {0};
  ret.tv_sec = now + secs;
  ret.tv_nsec = (secs - (time_t)secs) * 1e9;
  return ret;
}

/* Empties cq.  If do_sleep, loops to wait until all writes were accepted or
 * cb->deadline passes; otherwise, loops until either all writes were
 * accepted or cq is empty.  Returns a true value if all writes were
 * accepted.  Must call with cb->mutex held. */
static int process_cq(grpc_callback *cb, int do_sleep) {
  grpc_event *ev;

  while (cb->write_accepted_counter < cb->write_counter) {
    gpr_timespec sleep_until = get_deadline(do_sleep ? cb->deadline : 0);
    ev = grpc_completion_queue_next(cb->cq, sleep_until);
    if (!ev) {
      if (do_sleep)
        WARNING("write_grpc process_cq: "
                "Emptied queue for %s, %zu/%zu writes accepted",
                cb->host, cb->write_accepted_counter, cb->write_counter);
      return 0;
    }
    switch (ev->type) {
      case GRPC_WRITE_ACCEPTED:
        DEBUG("write_grpc process_cb: %s: Write %zu/%zu accepted",
              cb->host, cb->write_accepted_counter, cb->write_counter);
        cb->write_accepted_counter++;
        break;
      case GRPC_CLIENT_METADATA_READ:
        DEBUG("write_grpc process_cb: %s: Metadata read", cb->host);
        break;
      case GRPC_READ:
        if (!ev->data.read)
          DEBUG("write_grpc process_cb: %s: NULL read", cb->host);
        /* TODO(arielshaqed): Handle return code (actual payload is empty). */
        break;
      case GRPC_FINISHED:
        DEBUG("write_grpc process_cb: %s: Finished", cb->host);
        break;
      case GRPC_FINISH_ACCEPTED:
        DEBUG("write_grpc process_cb: %s: Finish accepted", cb->host);
        break;
      default:
        WARNING("write_grpc process_cq: %s: Unexpected event type %d",
                cb->host, ev->type);
        break;
    }
  }

  return 1;
}

/* TODO(arielshaqed): Aggregate vl's by putting this on a separate
 * thread, write callback just loads the thread? Or use streaming
 * gRPC? */
static int write_data(const data_set_t *ds, const value_list_t *vl,
                      user_data_t *user_data)
{
  data_set_and_value_list_t ds_and_vl;
  pb_ostream_callback_state_t stream_state;
  pb_ostream_t stream;
  grpc_callback *cb;
  grpc_call *call;
  gpr_slice slice;
  gpr_slice slice_out;
  grpc_byte_buffer* byte_buffer = NULL;
  grpc_call_error grpc_rc;
  int rc = 0;

  cb = user_data->data;

  ds_and_vl.ds = ds;
  ds_and_vl.vl = vl;

  gpr_mu_lock(&cb->mutex);
  /* If preceding write has not yet been accepted, sleep for it now:
   * avoid queuing outbound writes. This applies backpressure on the
   * collectd write queue, keeping relatively few messages in gRPC. */
  if (cb->write_accepted_counter < cb->write_counter) {
    DEBUG("Wait for message %d for %s to be accepted",
          cb->write_counter, cb->host);
    process_cq(cb, 1 /* Wait on CQ */);
  }

  /* Message can be written without gRPC blocking */
  slice = gpr_slice_malloc(4096);  /* Long enough for reasonable Stats */
  stream_state.slice = &slice;
  stream_state.next = GPR_SLICE_START_PTR(*stream_state.slice);
  stream.callback = grpc_pb_ostream_callback;
  stream.state = &stream_state;
  stream.max_size = GPR_SLICE_LENGTH(*stream_state.slice);
  stream.bytes_written = 0;
  stream.errmsg = NULL;

  if (!encode_value_list(&stream, &ds_and_vl)) {
    ERROR("write_grpc encode_value_list: %s", PB_GET_ERROR(&stream));
    rc = -1;
    goto exit;
  }

  slice_out = gpr_slice_sub(slice, 0, stream.bytes_written);
  byte_buffer = grpc_byte_buffer_create(&slice_out, 1);

  call = grpc_channel_create_call(
      cb->channel,
      /* TODO(arielshaqed): Configure this */
      "/google.internal.cloudlatencytest.v2.StatReporterService/UpdateAggregatedStats",
      cb->host,
      get_deadline(cb->deadline));

  if ((grpc_rc = grpc_call_start_write(call, byte_buffer, (void*)16, 0)) !=
      GRPC_CALL_OK) {
    ERROR("write_grpc grpc_call_start_write: %d", rc);
    rc = grpc_rc;
    goto exit;
  }

  process_cq(cb, 0 /* don't sleep */);

exit:
  gpr_slice_unref(slice);
  gpr_slice_unref(slice_out);
  if (byte_buffer) grpc_byte_buffer_destroy(byte_buffer);
  gpr_mu_unlock(&cb->mutex);

  return rc;
}

/* Returns cred for authenticating the client.
 *
 * If use_instance_credentials is true, acquires credentials from
 * metadata server (which must have correct scopes; this is NOT
 * checked).
 *
 * If service_account_json_filename is true, it should be the name of
 * a file containing the JSON service account credentials downloaded
 * from the GCE developer's console.  Returns credentials bound to use
 * specified scopes.
 */
static grpc_credentials *get_client_credentials(
    _Bool use_instance_credentials,
    const char *service_account_json_filename,
    const char *scopes)
{
  grpc_credentials *cred_client = NULL;

  if (use_instance_credentials) {
    cred_client = grpc_compute_engine_credentials_create();
    if (cred_client != NULL) {
      INFO("Loaded GCE credentials from instance metadata server");
      if (service_account_json_filename != NULL) {
        WARNING("Already have credentials; ignoring ServiceAccountJsonFile %s",
                service_account_json_filename);
      }
      return cred_client;
    }
    /* Keep going: might succeed with a configured service account */
    ERROR("Failed to fetch GCE credentials from instance metadata server");
  }

  if (service_account_json_filename != NULL) {
    char json[8192];
    ssize_t json_len;

    if ((json_len = read_file_contents(
            service_account_json_filename, json, sizeof(json) - 1)) < 0) {
      ERROR("Failed to read ServiceAccountJsonFile %s",
            service_account_json_filename);
      return NULL;
    }
    json[json_len] = '\0';  /* Ensure readable string */
    if (! (cred_client = grpc_service_account_credentials_create(
            json, scopes, grpc_max_auth_token_lifetime))) {
      ERROR("Failed to create credentials from ServiceAccountJsonFile %s",
            service_account_json_filename);
      return NULL;
    }
    return cred_client;
  }

  ERROR("Must specify UseInstanceCredentials or ServiceAccountJsonFile");
  return NULL;
}

/* Returns cred bound with root CA for authenticating the server */
static grpc_credentials *get_server_credentials(const char *root_pem_filename)
{
  unsigned char root_pem[16384];
  ssize_t root_pem_size;
  grpc_credentials *cred_server_ssl = NULL;

  if ((root_pem_size = read_file_contents(
          root_pem_filename, (char *)root_pem, sizeof(root_pem))) < 0) {
    ERROR("Failed to read SSL server CA PEM file %s", root_pem_filename);
    return NULL;
  }
  cred_server_ssl = grpc_ssl_credentials_create(
      root_pem, root_pem_size, NULL, 0, NULL, 0);
  return cred_server_ssl;
}

/* Returns credentials for an authenticated connection */
static grpc_credentials *get_credentials(
    _Bool use_instance_credentials,
    const char *service_account_json_filename,
    const char *scopes,
    const char *root_pem_filename)
{
  grpc_credentials *cred_client = get_client_credentials(
      use_instance_credentials, service_account_json_filename, scopes);
  grpc_credentials *cred_server = get_server_credentials(root_pem_filename);
  grpc_credentials *ret = NULL;

  if (cred_client && cred_server)
    ret = grpc_composite_credentials_create(cred_server, cred_client);

  if (cred_client) grpc_credentials_release(cred_client);
  if (cred_server) grpc_credentials_release(cred_server);

  return ret;
}

/* Loads plugin instance configuration from ci. */
static int load_config(oconfig_item_t *ci)
{
  static int grpc_initialized = 0;
  static const grpc_channel_args no_args = {0, NULL};
  grpc_callback *cb = NULL;
  user_data_t user_data;
  _Bool use_instance_credentials = 0;
  char *service_account_json_filename = NULL;
  char *scopes = NULL;
  char *root_pem_filename = NULL;
  int i;
  int rc = -1;

  if (ci == NULL) goto exit;

  if ((cb = malloc(sizeof(*cb))) == NULL) {
    ERROR("write_grpc plugin: failed to allocate memory for callback data.");
    goto exit;
  }
  memset(cb, 0, sizeof(*cb));
  cb->cred = NULL;
  cb->channel = NULL;
  gpr_mu_init(&cb->mutex);
  cb->cq = NULL;
  cb->write_counter = 0;
  cb->write_accepted_counter = 0;
  cb->deadline = 20.0;

  for (i = 0; i < ci->children_num; i++) {
    oconfig_item_t *child = &ci->children[i];

#define STR_EQ(a, b)  (strcasecmp((a), (b)) == 0)

    if (STR_EQ(child->key, "Host")) {
      if (cf_util_get_string(child, &cb->host)) {
        ERROR("Non-string Host value");
        goto exit;
      }
    }
    else if (STR_EQ(child->key, "UseInstanceCredentials")) {
      if (cf_util_get_boolean(child, &use_instance_credentials)) {
        ERROR("Non-boolean UseInstanceCredentials value");
        goto exit;
      }
    }
    else if (STR_EQ(child->key, "ServiceAccountJsonFile")) {
      if (cf_util_get_string(child, &service_account_json_filename)) {
        ERROR("Non-string ServiceAccountJsonFile value");
        goto exit;
      }
    }
    else if (STR_EQ(child->key, "Scopes")) {
      if (cf_util_get_string(child, &scopes)) {
        ERROR("Non-string Scopes value");
        goto exit;
      }
    }
    else if (STR_EQ(child->key, "RootPEMFile")) {
      if (cf_util_get_string(child, &root_pem_filename)) {
        ERROR("Non-string RootPEMFile value");
        goto exit;
      }
    }
    else if (STR_EQ(child->key, "Deadline")) {
      if (cf_util_get_double(child, &cb->deadline)) {
        ERROR("Bad Deadline value");
        goto exit;
      }
    }

#undef  STR_EQ
  }

  if (!root_pem_filename) {
    ERROR("Missing RootPEMFile");
    goto exit;
  }

  /* Configuration loaded; set up gRPC */
  if (!grpc_initialized) {
    grpc_init();
    grpc_initialized = 1;
  }

  if (! (cb->cred = get_credentials(
          use_instance_credentials,
          service_account_json_filename,
          scopes ? scopes : MONITORING_SCOPES,
          root_pem_filename)))
    /* Error already logged */
    goto exit;

  cb->channel = grpc_secure_channel_create(cb->cred, cb->host, &no_args);
  if (!cb->channel)
    /* Error already logged */
    goto exit;

  cb->cq = grpc_completion_queue_create();

  /* Success! */
  rc = 0;

  user_data.data = cb;
  user_data.free_func = destroy_cb;
  plugin_register_write(PLUGIN_NAME, write_data, &user_data);
  /* TODO(arielshaqed): flush */

exit:
  if (rc < 0 && cb)
    destroy_cb(cb);
  free(service_account_json_filename);
  free(scopes);
  free(root_pem_filename);

  return rc;
}

void module_register(void)
{
  plugin_register_complex_config(PLUGIN_NAME, load_config);
}
