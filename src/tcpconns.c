/**
 * collectd - src/tcpconns.c
 * Copyright (C) 2007,2008  Florian octo Forster
 * Copyright (C) 2008       Michael Stapelberg
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; only version 2 of the License is applicable.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author:
 *   Florian octo Forster <octo at collectd.org>
 *   Michael Stapelberg <michael+git at stapelberg.de>
 **/

/**
 * Code within `HAVE_LIBKVM_NLIST' blocks is provided under the following
 * license:
 *
 * $collectd: parts of tcpconns.c, 2008/08/08 03:48:30 Michael Stapelberg $
 * $OpenBSD: inet.c,v 1.100 2007/06/19 05:28:30 ray Exp $
 * $NetBSD: inet.c,v 1.14 1995/10/03 21:42:37 thorpej Exp $
 *
 * Copyright (c) 1983, 1988, 1993
 *      The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "collectd.h"
#include "common.h"
#include "plugin.h"
#include "safe_iop.h"
#include "utils_avltree.h"

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#if defined(__OpenBSD__) || defined(__NetBSD__)
#undef HAVE_SYSCTLBYNAME /* force HAVE_LIBKVM_NLIST path */
#endif

#if !KERNEL_LINUX && !HAVE_SYSCTLBYNAME && !HAVE_LIBKVM_NLIST && !KERNEL_AIX
# error "No applicable input method."
#endif

#if KERNEL_LINUX
# include <asm/types.h>
/* sys/socket.h is necessary to compile when using netlink on older systems. */
# include <sys/socket.h>
# include <linux/netlink.h>
#if HAVE_LINUX_INET_DIAG_H
# include <linux/inet_diag.h>
#endif
#include <linux/rtnetlink.h>
# include <netinet/tcp.h>
# include <sys/socket.h>
# include <arpa/inet.h>
/* #endif KERNEL_LINUX */

#elif HAVE_SYSCTLBYNAME
# include <sys/socketvar.h>
# include <sys/sysctl.h>

/* Some includes needed for compiling on FreeBSD */
#include <sys/time.h>
#if HAVE_SYS_TYPES_H
# include <sys/types.h>
#endif
#if HAVE_SYS_SOCKET_H
# include <sys/socket.h>
#endif
#if HAVE_NET_IF_H
# include <net/if.h>
#endif

# include <net/route.h>
# include <netinet/in.h>
# include <netinet/in_systm.h>
# include <netinet/ip.h>
# include <netinet/ip6.h>
# include <netinet/in_pcb.h>
# include <netinet/ip_var.h>
# include <netinet/tcp.h>
# include <netinet/tcpip.h>
# include <netinet/tcp_seq.h>
# include <netinet/tcp_var.h>
/* #endif HAVE_SYSCTLBYNAME */

/* This is for OpenBSD and NetBSD. */
#elif HAVE_LIBKVM_NLIST
# include <sys/queue.h>
# include <sys/socket.h>
# include <net/route.h>
# include <netinet/in.h>
# include <netinet/in_systm.h>
# include <netinet/ip.h>
# include <netinet/ip_var.h>
# include <netinet/in_pcb.h>
# include <netinet/tcp.h>
# include <netinet/tcp_timer.h>
# include <netinet/tcp_var.h>
# include <netdb.h>
# include <arpa/inet.h>
# if !defined(HAVE_BSD_NLIST_H) || !HAVE_BSD_NLIST_H
#  include <nlist.h>
# else /* HAVE_BSD_NLIST_H */
#  include <bsd/nlist.h>
# endif
# include <kvm.h>
/* #endif HAVE_LIBKVM_NLIST */

#elif KERNEL_AIX
# include <arpa/inet.h>
# include <sys/socketvar.h>
#endif /* KERNEL_AIX */

/* Bug: Language spec does NOT guarantee propogation of values between
 * threads for a volatile variable. Do it anyway, in practice it happens.
 */
static volatile _Bool shutdown_module = 0;

#if KERNEL_LINUX
#if HAVE_STRUCT_LINUX_INET_DIAG_REQ
struct nlreq {
  struct nlmsghdr nlh;
  struct inet_diag_req r;
};

/* Identifies fields to report from tcp_info */
enum tcpi_field_type {
  UINT8  = 1,
  UINT32 = 4,
  UINT64 = 8,
};
#define TCPI_FIELD_TYPE_SIZE(type) ((size_t)type)
struct tcpi_field_selector {
  const char *name;  /* Name of value_list field to report, NULL for last */
  int ds_type;       /* TODO(arielshaqed): No way to report 64-bit values */
  enum tcpi_field_type tcpi_type;
  uint32_t offset;  /* Offset inside tcp_info to read field. Must be aligned. */
  /* TODO(arielshaqed): States in which to record the field? */
};

static struct tcpi_field_selector *tcpi_fields_to_report = NULL;
static size_t num_tcpi_fields_to_report = 0;
static size_t tcpi_fields_to_report_size = 0;
static size_t num_tcpi_counter_fields = 0;

static uint64_t counter_max_delta = 1000000;

/* Cache for computing deltas of DELTA values. */
static cdtime_t counter_cache_timeout = DOUBLE_TO_CDTIME_T(120.0);
struct counter_cache_entry_s {
  /* Cache entry holds num_tcpi_counter_fields values -- one 64-bit value
   * for every COUNTER entry in order in tcpi_fields_to_report, This is
   * later used to compute a new value including wrap-around. */
  uint64_t *values;

  cdtime_t last_update_time;
};
typedef struct counter_cache_entry_s counter_cache_entry_t;
static pthread_mutex_t counter_cache_lock = PTHREAD_MUTEX_INITIALIZER;
static c_avl_tree_t   *counter_cache_tree = NULL;
static pthread_t counter_cache_cleanup_thread;
#endif

static const char *tcp_state[] =
{
  "", /* 0 */
  "ESTABLISHED",
  "SYN_SENT",
  "SYN_RECV",
  "FIN_WAIT1",
  "FIN_WAIT2",
  "TIME_WAIT",
  "CLOSED",
  "CLOSE_WAIT",
  "LAST_ACK",
  "LISTEN", /* 10 */
  "CLOSING"
};

# define TCP_STATE_LISTEN 10
# define TCP_STATE_MIN 1
# define TCP_STATE_MAX 11
/* #endif KERNEL_LINUX */

#elif HAVE_SYSCTLBYNAME
static const char *tcp_state[] =
{
  "CLOSED",
  "LISTEN",
  "SYN_SENT",
  "SYN_RECV",
  "ESTABLISHED",
  "CLOSE_WAIT",
  "FIN_WAIT1",
  "CLOSING",
  "LAST_ACK",
  "FIN_WAIT2",
  "TIME_WAIT"
};

# define TCP_STATE_LISTEN 1
# define TCP_STATE_MIN 0
# define TCP_STATE_MAX 10
/* #endif HAVE_SYSCTLBYNAME */

#elif HAVE_LIBKVM_NLIST
static const char *tcp_state[] =
{
  "CLOSED",
  "LISTEN",
  "SYN_SENT",
  "SYN_RECV",
  "ESTABLISHED",
  "CLOSE_WAIT",
  "FIN_WAIT1",
  "CLOSING",
  "LAST_ACK",
  "FIN_WAIT2",
  "TIME_WAIT"
};

static kvm_t *kvmd;
static u_long      inpcbtable_off = 0;
struct inpcbtable *inpcbtable_ptr = NULL;

# define TCP_STATE_LISTEN 1
# define TCP_STATE_MIN 1
# define TCP_STATE_MAX 10
/* #endif HAVE_LIBKVM_NLIST */

#elif KERNEL_AIX
static const char *tcp_state[] =
{
  "CLOSED",
  "LISTEN",
  "SYN_SENT",
  "SYN_RECV",
  "ESTABLISHED",
  "CLOSE_WAIT",
  "FIN_WAIT1",
  "CLOSING",
  "LAST_ACK",
  "FIN_WAIT2",
  "TIME_WAIT"
};

# define TCP_STATE_LISTEN 1
# define TCP_STATE_MIN 0
# define TCP_STATE_MAX 10

struct netinfo_conn {
  uint32_t unknow1[2];
  uint16_t dstport;
  uint16_t unknow2;
  struct in6_addr dstaddr;
  uint16_t srcport;
  uint16_t unknow3;
  struct in6_addr srcaddr;
  uint32_t unknow4[36];
  uint16_t tcp_state;
  uint16_t unknow5[7];
};

struct netinfo_header {
  unsigned int proto;
  unsigned int size;
};

# define NETINFO_TCP 3
extern int netinfo (int proto, void *data, int *size,  int n);
#endif /* KERNEL_AIX */

#define PORT_COLLECT_LOCAL  0x01
#define PORT_COLLECT_REMOTE 0x02
#define PORT_IS_LISTENING   0x04

typedef struct port_entry_s
{
  uint16_t port;
  uint16_t flags;
  uint32_t count_local[TCP_STATE_MAX + 1];
  uint32_t count_remote[TCP_STATE_MAX + 1];
  struct port_entry_s *next;
} port_entry_t;

static const char *config_keys[] =
{
  "ListeningPorts",
  "LocalPort",
  "RemotePort",
  "AllPortsSummary",
  "ReportByPorts",
  "ReportByConnections",
  "ConnectionsAgeLimitSecs",
  "TCPInfoField",
};
static int config_keys_num = STATIC_ARRAY_SIZE (config_keys);

static int port_collect_listening = 0;
static int port_collect_total = 0;
static int report_by_connections = 0;
static int report_by_ports = 1;
static int connections_age_limit_msecs = -1;
static port_entry_t *port_list_head = NULL;
static uint32_t count_total[TCP_STATE_MAX + 1];

#if KERNEL_LINUX
#if HAVE_STRUCT_LINUX_INET_DIAG_REQ

static void counter_cache_init(void) {
  pthread_mutex_lock(&counter_cache_lock);
  counter_cache_tree = c_avl_create((void*) strcmp);
  pthread_mutex_unlock(&counter_cache_lock);
}

static void counter_cache_free_entry(counter_cache_entry_t *entry) {
  sfree(entry->values);
  sfree(entry);
}

/* Returns true and fills in entry with counter cache entry for this key
 * (connection instance id) if it exists; otherwise returns false and fills
 * in entry to point at a new an (all-zeroes) entry. Does NOT change
 * last_update_time or set any values for entry, you need to do that
 * yourself. */
static _Bool get_counter_cache_entry(char *key, counter_cache_entry_t **entry) {
  int rc;
  int ret = 1;
  pthread_mutex_lock(&counter_cache_lock);
  if (c_avl_get(counter_cache_tree, key, (void**)entry) == 0)
    goto exit;
  /* Not found: create a new instance */
  ret = 0;
  key = strdup(key);
  *entry = malloc(sizeof(**entry));
  (*entry)->last_update_time = 0;
  (*entry)->values = calloc(num_tcpi_counter_fields, sizeof(*(*entry)->values));
  rc = c_avl_insert(counter_cache_tree, key, *entry);
  if (rc > 0) {
    ERROR("[I; leak] "
          "Cache inconsistent: expected to insert a new element for %s",
          key);
    goto exit;
  } else if (rc < 0) {
    ERROR("[I; crash] Cache insert failed for %s", key);
    sfree(key);
    counter_cache_free_entry(*entry);
    *entry = NULL;
    goto exit;
  }
exit:
  pthread_mutex_unlock(&counter_cache_lock);
  return ret;
}

/* Cleans up cache by removing entries last updated more than
 * counter_cache_timeout before t.  Returns number of elements removed. */
static size_t counter_cache_cleanup(cdtime_t t) {
  size_t i;
  char **remove_keys = NULL;
  size_t remove_keys_num = 0;
  size_t remove_keys_size = 0;
  c_avl_iterator_t *iter;
  char *key;
  counter_cache_entry_t *entry;
  char t_str[32];

  cdtime_to_iso8601(t_str, sizeof(t_str), t);
  INFO("counter_cache_cleanup: Clean up @%s", t_str);

  pthread_mutex_lock(&counter_cache_lock);
  if (!counter_cache_tree)
    /* Already freed during shutdown */
    return 0;
  iter = c_avl_get_iterator(counter_cache_tree);
  while (c_avl_iterator_next(iter, (void**)&key, (void**)&entry) == 0) {
    if (entry->last_update_time < t - counter_cache_timeout) {
      cdtime_to_iso8601(t_str, sizeof(t_str), entry->last_update_time);
      DEBUG("counter_cache_cleanup: Remove entry for %s (last update @%s)",
            t_str);

      if (remove_keys_size < remove_keys_num + 1) {
        remove_keys_size *= 2;
        if (remove_keys_size < remove_keys_num + 1)
          remove_keys_size = remove_keys_num + 1;
        remove_keys = realloc(
            remove_keys, remove_keys_size * sizeof(*remove_keys));
      }

      remove_keys[remove_keys_num++] = key;
    }
  }
  c_avl_iterator_destroy(iter);

  for (i = 0; i < remove_keys_num; i++) {
    entry = NULL;
    if (!c_avl_remove(counter_cache_tree, remove_keys[i],
                      NULL, (void**)&entry)) {
      ERROR("[I] Key-to-remove %s no longer in tree", remove_keys[i]);
    } else if (entry) {
      sfree(remove_keys[i]);
      counter_cache_free_entry(entry);
    }
  }
  sfree(remove_keys);

  pthread_mutex_unlock(&counter_cache_lock);

  INFO("Cleaned up %zu keys", remove_keys_num);
  return remove_keys_num;
}

static void *call_counter_cache_cleanup(void *unused) {
  while (!shutdown_module) {
    counter_cache_cleanup(cdtime());
    sleep(CDTIME_T_TO_DOUBLE(counter_cache_timeout) / 4 + 1);
  }
  pthread_exit(NULL);
  return NULL;  /* UNREACHED */
}

static uint64_t get_max_for_field_type(enum tcpi_field_type type) {
  switch(type) {
    case UINT8: return 1ULL << 8;
    case UINT32: return 1ULL << 32;
    case UINT64: return 0;  /* Arithmetic works when adding and subtracting. */
    default: return 0;      /* Impossible, but this value might be good. */
  }
}

/* Returns a wraparound-adjusted delta for going from old to new in field of
 * given type. Values > counter_max_delta are ignored and return 0. */
static uint64_t get_wraparound_delta(
    enum tcpi_field_type type, uint64_t new, uint64_t old) {
  uint64_t delta;
  if (old <= new) {
    /* Likely case: no wraparound */
    delta = new - old;
  }
  else {
    delta = get_max_for_field_type(type) - (old - new);
  }
  return delta <= counter_max_delta ? delta : 0;
}

static _Bool parse_ds_type(const char *str, int *ds_type) {
#define STR_CASE(name) do {                               \
      if (strcasecmp(str, #name) == 0) {                  \
        *ds_type = DS_TYPE_ ## name;                      \
        return 1;                                         \
      }                                                   \
  } while (0)

  STR_CASE(COUNTER);
  STR_CASE(GAUGE);
  STR_CASE(DERIVE);
  STR_CASE(ABSOLUTE);
  return 0;
#undef STR_CASE
}

static _Bool parse_c_type(const char *str, enum tcpi_field_type *c_type) {
#define STR_CASE(name) do {                               \
      if (strcasecmp(str, #name) == 0) {                  \
        *c_type = name;                                   \
        return 1;                                         \
      }                                                   \
  } while (0)

  STR_CASE(UINT8);
  STR_CASE(UINT32);
  STR_CASE(UINT64);
  return 0;
#undef STR_CASE
}

static void tcpinfo_field_selector_dispose(struct tcpi_field_selector *field) {
  sfree(*(char**)&field->name);
}

static _Bool parse_tcpinfo_field_selector(
    const char *selector_orig, struct tcpi_field_selector *field) {
  /* Parse a field selector that looks like this:
        NAME      DS_TYPE     C_TYPE:OFFSET
        rtt       GAUGE       uint32:64 */
  char *parse_ptr = NULL;
  char *selector = strdup(selector_orig);
  char *token;
  const char *c_type_string;
  const char *offset_string;
  _Bool rc = 0;
  if (!(token = strtok_r(selector, " \t\n,", &parse_ptr))) {
    ERROR ("Field selector \"%s\" missing NAME", selector_orig);
    goto exit;
  }
  field->name = strdup(token);
  if (!(token = strtok_r(NULL, " \t\n,", &parse_ptr))) {
    ERROR ("Field selector \"%s\" missing DS_TYPE", selector_orig);
    goto exit;
  }
  if (!parse_ds_type(token, &field->ds_type)) {
    ERROR ("Bad DS_TYPE \"%s\" in field selector \"%s\"", token, selector_orig);
    goto exit;
  }
  if (field->ds_type == DS_TYPE_COUNTER)
    num_tcpi_counter_fields++;
  if (!(token = strtok_r(NULL, " \t\n,", &parse_ptr))) {
    ERROR ("Field selector \"%s\" missing C_TYPE:OFFSET", selector_orig);
    goto exit;
  }
  c_type_string = token;
  if (!(token = strchr(token, ':'))) {
    ERROR ("Missing ':' delimiter in C_TYPE:OFFSET \"%s\" "
           "in field selector \"%s\"",
           c_type_string, selector_orig);
    goto exit;
  }
  *token = '\0';
  offset_string = token+1;
  if (!parse_c_type(c_type_string, &field->tcpi_type)) {
    ERROR ("Bad C_TYPE \"%s\" in field selector \"%s\"",
           c_type_string, selector_orig);
    goto exit;
  }
  errno = 0;
  field->offset = strtoull(offset_string, &token, 0);
  if (errno) {
    char err[256];
    ERROR ("Bad offset \"%s\" in field selector \"%s\": %s",
           offset_string, selector_orig, sstrerror(errno, err, sizeof(err)));
    goto exit;
  }
  if (*token != '\0') {
    ERROR ("Offset \"%s\" in field selector \"%s\" contains trailing garbage",
           offset_string, selector_orig);
    goto exit;
  }
  rc = 1;

exit:
  sfree(selector);
  return rc;
}

/* This depends on linux inet_diag_req because if this structure is missing,
 * sequence_number is useless and we get a compilation warning.
 */
static uint32_t sequence_number = 0;
#endif

enum
{
  SRC_DUNNO,
  SRC_NETLINK,
  SRC_PROC
} linux_source = SRC_DUNNO;
#endif

static void conn_prepare_vl (value_list_t *vl, value_t *values)
{
  vl->values = values;
  vl->values_len = 1;
  sstrncpy (vl->host, hostname_g, sizeof (vl->host));
  sstrncpy (vl->plugin, "tcpconns", sizeof (vl->plugin));
  sstrncpy (vl->type, "tcp_connections", sizeof (vl->type));
}

static void conn_submit_port_entry (port_entry_t *pe)
{
  value_t values[1];
  value_list_t vl = VALUE_LIST_INIT;
  int i;

  conn_prepare_vl (&vl, values);

  if (((port_collect_listening != 0) && (pe->flags & PORT_IS_LISTENING))
      || (pe->flags & PORT_COLLECT_LOCAL))
  {
    ssnprintf (vl.plugin_instance, sizeof (vl.plugin_instance),
	"%"PRIu16"-local", pe->port);

    for (i = 1; i <= TCP_STATE_MAX; i++)
    {
      vl.values[0].gauge = pe->count_local[i];

      sstrncpy (vl.type_instance, tcp_state[i], sizeof (vl.type_instance));

      plugin_dispatch_values (&vl);
    }
  }

  if (pe->flags & PORT_COLLECT_REMOTE)
  {
    ssnprintf (vl.plugin_instance, sizeof (vl.plugin_instance),
	"%"PRIu16"-remote", pe->port);

    for (i = 1; i <= TCP_STATE_MAX; i++)
    {
      vl.values[0].gauge = pe->count_remote[i];

      sstrncpy (vl.type_instance, tcp_state[i], sizeof (vl.type_instance));

      plugin_dispatch_values (&vl);
    }
  }
} /* void conn_submit */

static void conn_submit_port_total (void)
{
  value_t values[1];
  value_list_t vl = VALUE_LIST_INIT;
  int i;

  conn_prepare_vl (&vl, values);

  sstrncpy (vl.plugin_instance, "all", sizeof (vl.plugin_instance));

  for (i = 1; i <= TCP_STATE_MAX; i++)
  {
    vl.values[0].gauge = count_total[i];

    sstrncpy (vl.type_instance, tcp_state[i], sizeof (vl.type_instance));

    plugin_dispatch_values (&vl);
  }
}

static void conn_submit_all (void)
{
  port_entry_t *pe;

  if (port_collect_total)
    conn_submit_port_total ();

  for (pe = port_list_head; pe != NULL; pe = pe->next)
    conn_submit_port_entry (pe);
} /* void conn_submit_all */

static port_entry_t *conn_get_port_entry (uint16_t port, int create)
{
  port_entry_t *ret;

  ret = port_list_head;
  while (ret != NULL)
  {
    if (ret->port == port)
      break;
    ret = ret->next;
  }

  if ((ret == NULL) && (create != 0))
  {
    ret = (port_entry_t *) malloc (sizeof (port_entry_t));
    if (ret == NULL)
      return (NULL);
    memset (ret, '\0', sizeof (port_entry_t));

    ret->port = port;
    ret->next = port_list_head;
    port_list_head = ret;
  }

  return (ret);
} /* port_entry_t *conn_get_port_entry */

/* Removes ports that were added automatically due to the `ListeningPorts'
 * setting but which are no longer listening. */
static void conn_reset_port_entry (void)
{
  port_entry_t *prev = NULL;
  port_entry_t *pe = port_list_head;

  memset (&count_total, '\0', sizeof(count_total));

  while (pe != NULL)
  {
    /* If this entry was created while reading the files (ant not when handling
     * the configuration) remove it now. */
    if ((pe->flags & (PORT_COLLECT_LOCAL
	    | PORT_COLLECT_REMOTE
	    | PORT_IS_LISTENING)) == 0)
    {
      port_entry_t *next = pe->next;

      DEBUG ("tcpconns plugin: Removing temporary entry "
	  "for listening port %"PRIu16, pe->port);

      if (prev == NULL)
	port_list_head = next;
      else
	prev->next = next;

      sfree (pe);
      pe = next;

      continue;
    }

    memset (pe->count_local, '\0', sizeof (pe->count_local));
    memset (pe->count_remote, '\0', sizeof (pe->count_remote));
    pe->flags &= ~PORT_IS_LISTENING;

    pe = pe->next;
  }
} /* void conn_reset_port_entry */

static int conn_handle_ports (uint16_t port_local, uint16_t port_remote, uint8_t state)
{
  port_entry_t *pe = NULL;

  if ((state > TCP_STATE_MAX)
#if TCP_STATE_MIN > 0
      || (state < TCP_STATE_MIN)
#endif
     )
  {
    NOTICE ("tcpconns plugin: Ignoring connection with "
	"unknown state 0x%02"PRIx8".", state);
    return (-1);
  }

  count_total[state]++;

  /* Listening sockets */
  if ((state == TCP_STATE_LISTEN) && (port_collect_listening != 0))
  {
    pe = conn_get_port_entry (port_local, 1 /* create */);
    if (pe != NULL)
      pe->flags |= PORT_IS_LISTENING;
  }

  DEBUG ("tcpconns plugin: Connection %"PRIu16" <-> %"PRIu16" (%s)",
      port_local, port_remote, tcp_state[state]);

  pe = conn_get_port_entry (port_local, 0 /* no create */);
  if (pe != NULL)
    pe->count_local[state]++;

  pe = conn_get_port_entry (port_remote, 0 /* no create */);
  if (pe != NULL)
    pe->count_remote[state]++;

  return (0);
} /* int conn_handle_ports */

#if KERNEL_LINUX

#if HAVE_STRUCT_LINUX_INET_DIAG_REQ
/* Batched reporting of value_list_t  */
typedef struct value_list_batch_s {
    size_t size;
    size_t cur;
    value_list_t buffer[1];       /* Allocated to have size entries */
} value_list_batch_t;

static value_list_batch_t *value_list_batch_create(size_t size)
{
    value_list_batch_t *ret;
    size_t ret_size;
    size_t i;
    const value_list_t vl_init = VALUE_LIST_INIT;
    if (size < 4)
    {
        ERROR("Buffer size %zu < 4; using 4", size);
        size = 4;
    }
    if (!safe_mul(&ret_size, size-1, sizeof(value_list_t)) ||
        !safe_add(&ret_size, ret_size, sizeof(value_list_batch_t)))
    {
        ERROR("Buffer size overflow for %zu + %zu * %zu",
              sizeof(value_list_batch_t), size-1, sizeof(value_list_t));
        return NULL;
    }
    if (!(ret = malloc(ret_size))) {
        ERROR("Failed to allocate %zu byte buffer for value_list_batch",
              ret_size);
        return NULL;
    }
    ret->size = size;
    for (i = 0; i < size; i++)
        ret->buffer[i] = vl_init;
    ret->cur = 0;
    return ret;
}

static void value_list_batch_flush(value_list_batch_t *batch)
{
    size_t i;
    /* TODO(arielshaqed): Use new batched reporting API! */
    for (i = 0; i < batch->cur; i++) {
        plugin_dispatch_values (&batch->buffer[i]);
    }
    for (i = 0; i < batch->cur; i++) {
        free(batch->buffer[i].values);
        /* Also free meta? */
    }
    batch->cur = 0;
}

static void value_list_batch_free(value_list_batch_t *batch)
{
    value_list_batch_flush(batch);
    free(batch);
}

static int value_list_batch_maybe_flush(value_list_batch_t *batch)
{
    if (batch->cur >= batch->size) {
        value_list_batch_flush(batch);
        return 1;
    }
    return 0;
}

/* Returns a value_list to populate with values; must call
 * value_list_batch_release or value_list_batch_abort before calling again. */
static value_list_t *value_list_batch_get(value_list_batch_t *batch)
{
    value_list_batch_maybe_flush(batch);
    return &batch->buffer[batch->cur];
}

/* Frees value_list without sending it on. */
static void value_list_batch_abort(value_list_batch_t *batch)
{
  free(batch->buffer[batch->cur].values);
}

static void value_list_batch_release(value_list_batch_t *batch)
{
  batch->cur++;
  value_list_batch_maybe_flush(batch);
}

/* Return 1 if tcpi should be reported */
static int filter_tcpi(const struct tcp_info* tcpi, size_t tcpi_size)
{
  /* Returns 0 if tcpi_size is not large enough to contain field. */
#define	NEED_FIELD(field, tcpi_size)                                    \
  do {                                                                  \
      if (offsetof(struct tcp_info, field) +                            \
          sizeof(((struct tcp_info *)NULL)->field) >                    \
          tcpi_size) {                                                  \
        ERROR ("tcp_info size %zu too small to hold %s",                \
               tcpi_size, #field);                                      \
        return 0;                                                       \
      }                                                                 \
  } while(0)

  NEED_FIELD(tcpi_last_data_sent, tcpi_size);
  /* NEED_FIELD(tcpi_last_ack_sent, tcpi_size);*/
  NEED_FIELD(tcpi_last_data_recv, tcpi_size);
  NEED_FIELD(tcpi_last_ack_recv, tcpi_size);

  /* Skip last ACK sent, it's documented "Not remembered, sorry." */
  return connections_age_limit_msecs < 0 ||
      tcpi->tcpi_last_data_sent < connections_age_limit_msecs ||
        /* tcpi->tcpi_last_ack_sent < connections_age_limit_msecs || */
      tcpi->tcpi_last_data_recv < connections_age_limit_msecs ||
      tcpi->tcpi_last_ack_recv < connections_age_limit_msecs;
}

/* Update entries for specified connections.  May call conn_buffer_flush. */
static void conn_handle_tcpi(
    value_list_batch_t *batch, uint8_t state,
    const char src[], uint16_t sport, const char dst[], uint16_t dport,
    const struct tcp_info* tcpi, size_t tcpi_size)
{
    value_list_t *vl = value_list_batch_get(batch);
    const char *state_name = TCP_STATE_MIN <= state && state <= TCP_STATE_MAX ?
        tcp_state[state] : "UNKNOWN";
    int tcpi_field_index;
    int counter_field_index;
    int value_index;
    counter_cache_entry_t *entry;
    _Bool in_cache = 0;
    _Bool ok = 1;

    vl->values = calloc(num_tcpi_fields_to_report, sizeof(value_t));
    sstrncpy (vl->host, hostname_g, sizeof (vl->host));
    sstrncpy (vl->plugin, "tcpconns", sizeof (vl->plugin));
    snprintf(vl->plugin_instance, sizeof (vl->plugin_instance),
             "%s:%u_%s:%u_%s", src, sport, dst, dport, state_name);
    sstrncpy (vl->type, "tcp_connections_perf", sizeof (vl->type));

    if (num_tcpi_counter_fields > 0)
      in_cache = get_counter_cache_entry(vl->plugin_instance, &entry);

    for (tcpi_field_index = 0, value_index = 0, counter_field_index = 0;
         tcpi_field_index < num_tcpi_fields_to_report;
         tcpi_field_index++) {
        const struct tcpi_field_selector *field =
            &tcpi_fields_to_report[tcpi_field_index];
        unsigned char *value_bytes;
        uint64_t value;
        if (field->offset + TCPI_FIELD_TYPE_SIZE(field->tcpi_type) >
            tcpi_size) {
          ERROR ("tcpconns plugin: conn_handle_tcpi: "
                 "Field %s not in reported tcp_info "
                 "(%" PRIu32 " + %zu > %zu)",
                 field->name,
                 field->offset,
                 TCPI_FIELD_TYPE_SIZE(field->tcpi_type),
                 tcpi_size);
          ok = 0;
          break;
        }
        value_bytes = (unsigned char *)tcpi + field->offset;
        switch(field->tcpi_type) {
          case UINT8:
            value = *(uint8_t*)value_bytes;
            break;
          case UINT32:
            value = *(uint32_t*)value_bytes;
            break;
          case UINT64:
            value = *(uint64_t*)value_bytes;
            break;
          default:
            /* Read configuration should have caught this error */
            continue;
        }
        switch (field->ds_type) {
          case DS_TYPE_COUNTER: {
            /* Overload "COUNTER" (which
               https://collectd.org/wiki/index.php/Data_source#Data_source_types
               says should be divided by delta-T) to compute a simple delta. */
            assert(num_tcpi_counter_fields > 0);
            if (in_cache) {
              vl->values[value_index].counter =
                  get_wraparound_delta(field->tcpi_type,
                                       value,
                                       entry->values[counter_field_index]);
            }
            else {
              vl->values[value_index].counter = 0;
            }
            if (vl->values[value_index].counter > 0) {
              DEBUG("%s%s; +%llu --> %" PRIu64,
                    vl->plugin_instance,
                    in_cache ? "" : " (new)",
                    vl->values[value_index].counter,
                    value);
            }
            entry->values[counter_field_index] = value;
            value_index++;
            counter_field_index++;
            break;
          }
          case DS_TYPE_GAUGE:
            vl->values[value_index++].gauge = value;
            break;
          case DS_TYPE_DERIVE:
            vl->values[value_index++].derive = value;
            break;
          case DS_TYPE_ABSOLUTE:
            vl->values[value_index++].absolute = value;
            break;
          default: /* Read configuration should have caught this error */
            break;
        }
    }
    entry->last_update_time = cdtime();
    if (ok) {
      vl->values_len = value_index;
      value_list_batch_release(batch);
    } else {
      value_list_batch_abort(batch);
    }
} /* conn_handle_tcpi */

/* Returns tcp_info in an rtattr in h. Returns NULL if all rtattr's scanned
 * and no tcp_info found. Fills in number of relevant bytes of h in
 * *size. */
static struct tcp_info *get_tcp_info(struct nlmsghdr *h, size_t *size)
{
  struct inet_diag_msg *r = NLMSG_DATA(h);
  ssize_t remaining_len = h->nlmsg_len - NLMSG_LENGTH(sizeof(*r));
  struct rtattr *attr = (struct rtattr*) (r + 1);
  for (;
       remaining_len > 0 && RTA_OK(attr, remaining_len);
       attr = RTA_NEXT(attr, remaining_len)) {
      DEBUG ("Type = %d ; %zd bytes remaining", attr->rta_type, remaining_len);
    if (attr->rta_type == INET_DIAG_INFO) {
      *size = RTA_PAYLOAD(attr);
      return RTA_DATA(attr);
      break;
    }
  }
  return NULL;
} /* get_tcp_info */

#endif  /* HAVE_STRUCT_LINUX_INET_DIAG_REQ */

/* Returns zero on success, less than zero on socket error and greater than
 * zero on other errors. */
static int conn_read_netlink (void)
{
#if HAVE_STRUCT_LINUX_INET_DIAG_REQ
  int fd;
  struct sockaddr_nl nladdr;
  struct nlreq req;
  struct inet_diag_msg *r;
  value_list_batch_t *batch = value_list_batch_create(2048);
  char buf[32768];

  if (!batch)
  {
    /* Error already logged */
    return (-1);
  }

  /* If this fails, it's likely a permission problem. We'll fall back to
   * reading this information from files below. */
  fd = socket(AF_NETLINK, SOCK_RAW, NETLINK_INET_DIAG);
  if (fd < 0)
  {
    ERROR ("tcpconns plugin: conn_read_netlink: socket(AF_NETLINK, SOCK_RAW, "
	"NETLINK_INET_DIAG) failed: %s",
	sstrerror (errno, buf, sizeof (buf)));
    return (-1);
  }

  memset(&nladdr, 0, sizeof(nladdr));
  nladdr.nl_family = AF_NETLINK;

  memset(&req, 0, sizeof(req));
  req.nlh.nlmsg_len = sizeof(req);
  req.nlh.nlmsg_type = TCPDIAG_GETSOCK;
  /* NLM_F_ROOT: return the complete table instead of a single entry.
   * NLM_F_MATCH: return all entries matching criteria (not implemented)
   * NLM_F_REQUEST: must be set on all request messages */
  req.nlh.nlmsg_flags = NLM_F_ROOT | NLM_F_MATCH | NLM_F_REQUEST;
  req.nlh.nlmsg_pid = 0;
  /* The sequence_number is used to track our messages. Since netlink is not
   * reliable, we don't want to end up with a corrupt or incomplete old
   * message in case the system is/was out of memory. */
  req.nlh.nlmsg_seq = ++sequence_number;
  req.r.idiag_family = AF_INET;
  req.r.idiag_states = 0xffff;
  req.r.idiag_ext = 1 << (INET_DIAG_INFO - 1);

  if (send (fd, &req, sizeof(req), /* flags = */ 0) < 0)
  {
    ERROR ("tcpconns plugin: conn_read_netlink: send(2) failed: %s",
	sstrerror (errno, buf, sizeof (buf)));
    close (fd);
    value_list_batch_free(batch);
    return (-1);
  }

  while (1)
  {
    int status;
    struct nlmsghdr *h;

    status = recv(fd, buf, sizeof(buf), /* flags = */ 0);
    if (status < 0)
    {
      if ((errno == EINTR) || (errno == EAGAIN))
        continue;

      ERROR ("tcpconns plugin: conn_read_netlink: recv(2) failed: %s",
	  sstrerror (errno, buf, sizeof (buf)));
      close (fd);
      value_list_batch_free(batch);
      return (-1);
    }
    else if (status == 0)
    {
      close (fd);
      DEBUG ("tcpconns plugin: conn_read_netlink: Unexpected zero-sized "
	  "reply from netlink socket.");
      close (fd);
      value_list_batch_free(batch);
      return (0);
    }

    h = (struct nlmsghdr*)buf;
    while (NLMSG_OK(h, status))
    {
      if (h->nlmsg_seq != sequence_number)
      {
        INFO ("tcpconns plugin: conn_read_netlink: sequence numbers mismatch "
            "received %u != expected %u",
            h->nlmsg_seq, sequence_number);
	h = NLMSG_NEXT(h, status);
	continue;
      }

      if (h->nlmsg_type == NLMSG_DONE)
      {
        DEBUG ("tcpconns plugin: conn_read_netlink: done!");
	close (fd);
	value_list_batch_free(batch);
	return (0);
      }
      else if (h->nlmsg_type == NLMSG_ERROR)
      {
	struct nlmsgerr *msg_error;

	msg_error = NLMSG_DATA(h);
	WARNING ("tcpconns plugin: conn_read_netlink: Received error %i.",
	    msg_error->error);

	close (fd);
	value_list_batch_free(batch);
	return (1);
      }

      r = NLMSG_DATA(h);
      do {
        /* We access tcpi by configurable offset; must check each access via
         * tcpi before use! */
	struct tcp_info *tcpi;
        size_t tcpi_size;
        u_int8_t state = r->idiag_state;
	unsigned short sport = ntohs(r->id.idiag_sport);
	unsigned short dport = ntohs(r->id.idiag_dport);

        tcpi = get_tcp_info(h, &tcpi_size);
        if (!tcpi)
          break;

	/* This code does not (need to) distinguish between IPv4 and IPv6. */
        if (report_by_ports)
          conn_handle_ports (sport, dport, state);

        if (report_by_connections) {
          if (r->idiag_state != TCP_STATE_LISTEN && tcpi &&
              filter_tcpi(tcpi, tcpi_size)) {
	    char src[INET6_ADDRSTRLEN];
	    char dst[INET6_ADDRSTRLEN];
	    if (!inet_ntop(r->idiag_family, r->id.idiag_src, src, sizeof(src)))
              strncpy(src, "<UNKNOWN>", sizeof(src));
	    if (!inet_ntop(r->idiag_family, r->id.idiag_dst, dst, sizeof(dst)))
              strncpy(dst, "<UNKNOWN>", sizeof(dst));
	    conn_handle_tcpi (
                batch, r->idiag_state, src, sport, dst, dport, tcpi, tcpi_size);
          }
	}
      } while (0);

      h = NLMSG_NEXT(h, status);
    } /* while (NLMSG_OK) */
  } /* while (1) */

  /* Not reached because the while() loop above handles the exit condition. */
  close(fd);
  value_list_batch_free(batch);
  return (0);
#else
  return (1);
#endif  /* HAVE_STRUCT_LINUX_INET_DIAG_REQ */
} /* int conn_read_netlink */

static int conn_handle_line (char *buffer)
{
  char *fields[32];
  int   fields_len;

  char *endptr;

  char *port_local_str;
  char *port_remote_str;
  uint16_t port_local;
  uint16_t port_remote;

  uint8_t state;

  int buffer_len = strlen (buffer);

  while ((buffer_len > 0) && (buffer[buffer_len - 1] < 32))
    buffer[--buffer_len] = '\0';
  if (buffer_len <= 0)
    return (-1);

  fields_len = strsplit (buffer, fields, STATIC_ARRAY_SIZE (fields));
  if (fields_len < 12)
  {
    DEBUG ("tcpconns plugin: Got %i fields, expected at least 12.", fields_len);
    return (-1);
  }

  port_local_str  = strchr (fields[1], ':');
  port_remote_str = strchr (fields[2], ':');

  if ((port_local_str == NULL) || (port_remote_str == NULL))
    return (-1);
  port_local_str++;
  port_remote_str++;
  if ((*port_local_str == '\0') || (*port_remote_str == '\0'))
    return (-1);

  endptr = NULL;
  port_local = (uint16_t) strtol (port_local_str, &endptr, 16);
  if ((endptr == NULL) || (*endptr != '\0'))
    return (-1);

  endptr = NULL;
  port_remote = (uint16_t) strtol (port_remote_str, &endptr, 16);
  if ((endptr == NULL) || (*endptr != '\0'))
    return (-1);

  endptr = NULL;
  state = (uint8_t) strtol (fields[3], &endptr, 16);
  if ((endptr == NULL) || (*endptr != '\0'))
    return (-1);

  return (conn_handle_ports (port_local, port_remote, state));
} /* int conn_handle_line */

static int conn_read_file (const char *file)
{
  FILE *fh;
  char buffer[1024];

  fh = fopen (file, "r");
  if (fh == NULL)
    return (-1);

  while (fgets (buffer, sizeof (buffer), fh) != NULL)
  {
    conn_handle_line (buffer);
  } /* while (fgets) */

  fclose (fh);

  return (0);
} /* int conn_read_file */
/* #endif KERNEL_LINUX */

#elif HAVE_SYSCTLBYNAME
/* #endif HAVE_SYSCTLBYNAME */

#elif HAVE_LIBKVM_NLIST
#endif /* HAVE_LIBKVM_NLIST */

static int conn_config (const char *key, const char *value)
{
  if (strcasecmp (key, "ListeningPorts") == 0)
  {
    if (IS_TRUE (value))
      port_collect_listening = 1;
    else
      port_collect_listening = 0;
  }
  else if ((strcasecmp (key, "LocalPort") == 0)
      || (strcasecmp (key, "RemotePort") == 0))
  {
      port_entry_t *pe;
      int port = atoi (value);

      if ((port < 1) || (port > 65535))
      {
	ERROR ("tcpconns plugin: Invalid port: %i", port);
	return (1);
      }

      pe = conn_get_port_entry ((uint16_t) port, 1 /* create */);
      if (pe == NULL)
      {
	ERROR ("tcpconns plugin: conn_get_port_entry failed.");
	return (1);
      }

      if (strcasecmp (key, "LocalPort") == 0)
	pe->flags |= PORT_COLLECT_LOCAL;
      else
	pe->flags |= PORT_COLLECT_REMOTE;
  }
  else if (strcasecmp (key, "AllPortsSummary") == 0)
  {
    if (IS_TRUE (value))
      port_collect_total = 1;
    else
      port_collect_total = 0;
  }
  else if (strcasecmp (key, "ReportByConnections") == 0)
  {
    if (IS_TRUE (value)) {
      report_by_connections = 1;
#if !(KERNEL_LINUX && HAVE_STRUCT_LINUX_INET_DIAG_REQ)
      ERROR ("tcpconns plugin: Platform does not support ReportByConnections.");
#endif
    }
    else
      report_by_connections = 0;
  }
  else if (strcasecmp (key, "ReportByPorts") == 0)
  {
    if (IS_TRUE (value))
      report_by_ports = 1;
    else
      report_by_ports = 0;
  }
  else if (strcasecmp (key, "ConnectionsAgeLimitSecs") == 0) {
    connections_age_limit_msecs = atof(value) * 1000;
  }
  else if (strcasecmp (key, "CounterMaxDelta") == 0) {
    counter_max_delta = atoi(value);
  }
  else if (strcasecmp (key, "CounterCacheTimeoutSecs") == 0) {
    counter_cache_timeout = DOUBLE_TO_CDTIME_T(atof(value));
  }
  else if (strcasecmp (key, "TCPInfoField") == 0) {
    struct tcpi_field_selector field;
    field.name = NULL;
    if (!parse_tcpinfo_field_selector(value, &field)) {
      ERROR ("tcpconns plugin: Failed to parse field selector \"%s\"", value);
      tcpinfo_field_selector_dispose(&field);
      return (1);
    }
    if (tcpi_fields_to_report_size <= num_tcpi_fields_to_report) {
      tcpi_fields_to_report_size = tcpi_fields_to_report_size * 2;
      if (tcpi_fields_to_report_size < 3)
        tcpi_fields_to_report_size = 3;
      if (!(tcpi_fields_to_report = realloc(
              tcpi_fields_to_report,
              tcpi_fields_to_report_size * sizeof(*tcpi_fields_to_report)))) {
        ERROR ("tcpconns plugin: Out of memory for %zu fields (field %s)",
               tcpi_fields_to_report_size, value);
        tcpinfo_field_selector_dispose(&field);
        return (1);
      }
    }
    tcpi_fields_to_report[num_tcpi_fields_to_report++] = field;
  }
  else
  {
    return (-1);
  }

  if (num_tcpi_fields_to_report > 0 && !report_by_connections) {
    WARNING ("Requested reporting of %zu tcp_info fields "
             "but not reporting connections", num_tcpi_fields_to_report);
  }

  return (0);
} /* int conn_config */

#if KERNEL_LINUX
static int conn_init (void)
{
  if (port_collect_total == 0 && port_list_head == NULL)
    port_collect_listening = 1;

#ifdef HAVE_LINUX_INET_DIAG_H
  if (num_tcpi_counter_fields > 0) {
    int pthread_status;
    counter_cache_init();
    pthread_status =
        pthread_create(&counter_cache_cleanup_thread, NULL,
                       call_counter_cache_cleanup, NULL);
    if (pthread_status != 0) {
      char errbuf[1024];
      ERROR("tcpconns conn_init (counter_cache): pthread_create failed: %s",
            sstrerror(pthread_status, errbuf, sizeof(errbuf)));
      ERROR("\tCache cleanup will NOT occur.");
    }
  }
  if (report_by_connections) {  /* Define what dataset we shall be reporting */
    data_set_t data_set;
    size_t i;
    if (num_tcpi_fields_to_report == 0 && report_by_connections) {
      ERROR ("TCPConns plugin: "
             "ReportByConnections requested, "
             "but no TCPInfoField lines configured.");
      return 1;
    }
    strncpy(data_set.type, "tcp_connections_perf", sizeof(data_set.type));
    data_set.ds_num = num_tcpi_fields_to_report;
    data_set.ds = calloc(num_tcpi_fields_to_report, sizeof(*data_set.ds));
    for (i = 0; i < num_tcpi_fields_to_report; i++) {
      strncpy(data_set.ds[i].name, tcpi_fields_to_report[i].name,
              sizeof(data_set.ds[i].name));
      data_set.ds[i].type = tcpi_fields_to_report[i].ds_type;
      if (data_set.ds[i].type == DS_TYPE_COUNTER) {
        data_set.ds[i].min = 0;
        data_set.ds[i].max = counter_max_delta;
      }
      else {
        data_set.ds[i].min = NAN;
        data_set.ds[i].max = NAN;
      }
    }
    if (plugin_unregister_data_set ("tcp_connections_perf") < 0) {
      WARNING ("TCPConns plugin: "
               "Expected to find dataset \"tcp_connections_perf\" in types.db");
    }
    plugin_register_data_set (&data_set);
    sfree (data_set.ds);
  }
#endif /* HAVE_LINUX_INET_DIAG_H */

  return (0);
} /* int conn_init */

static int conn_read (void)
{
  int status;

  conn_reset_port_entry ();

  if (linux_source == SRC_NETLINK)
  {
    status = conn_read_netlink ();
  }
  else if (linux_source == SRC_PROC)
  {
    int errors_num = 0;

    if (conn_read_file ("/proc/net/tcp") != 0)
      errors_num++;
    if (conn_read_file ("/proc/net/tcp6") != 0)
      errors_num++;

    if (errors_num < 2)
      status = 0;
    else
      status = ENOENT;
  }
  else /* if (linux_source == SRC_DUNNO) */
  {
    /* Try to use netlink for getting this data, it is _much_ faster on systems
     * with a large amount of connections. */
    status = conn_read_netlink ();
    if (status == 0)
    {
      INFO ("tcpconns plugin: Reading from netlink succeeded. "
	  "Will use the netlink method from now on.");
      linux_source = SRC_NETLINK;
    }
    else
    {
      INFO ("tcpconns plugin: Reading from netlink failed. "
	  "Will read from /proc from now on.");
      linux_source = SRC_PROC;
      if (report_by_connections)
        ERROR ("tcpconns plugin: "
               "Ignore ReportByConnections (not reading Netlink inet_diag)");

      /* return success here to avoid the "plugin failed" message. */
      return (0);
    }
  }

  if (status == 0)
    conn_submit_all ();
  else
    return (status);

  return (0);
} /* int conn_read */
/* #endif KERNEL_LINUX */

#elif HAVE_SYSCTLBYNAME
static int conn_read (void)
{
  int status;
  char *buffer;
  size_t buffer_len;;

  struct xinpgen *in_orig;
  struct xinpgen *in_ptr;

  conn_reset_port_entry ();

  buffer_len = 0;
  status = sysctlbyname ("net.inet.tcp.pcblist", NULL, &buffer_len, 0, 0);
  if (status < 0)
  {
    ERROR ("tcpconns plugin: sysctlbyname failed.");
    return (-1);
  }

  buffer = (char *) malloc (buffer_len);
  if (buffer == NULL)
  {
    ERROR ("tcpconns plugin: malloc failed.");
    return (-1);
  }

  status = sysctlbyname ("net.inet.tcp.pcblist", buffer, &buffer_len, 0, 0);
  if (status < 0)
  {
    ERROR ("tcpconns plugin: sysctlbyname failed.");
    sfree (buffer);
    return (-1);
  }

  if (buffer_len <= sizeof (struct xinpgen))
  {
    ERROR ("tcpconns plugin: (buffer_len <= sizeof (struct xinpgen))");
    sfree (buffer);
    return (-1);
  }

  in_orig = (struct xinpgen *) buffer;
  for (in_ptr = (struct xinpgen *) (((char *) in_orig) + in_orig->xig_len);
      in_ptr->xig_len > sizeof (struct xinpgen);
      in_ptr = (struct xinpgen *) (((char *) in_ptr) + in_ptr->xig_len))
  {
    struct tcpcb *tp = &((struct xtcpcb *) in_ptr)->xt_tp;
    struct inpcb *inp = &((struct xtcpcb *) in_ptr)->xt_inp;
    struct xsocket *so = &((struct xtcpcb *) in_ptr)->xt_socket;

    /* Ignore non-TCP sockets */
    if (so->xso_protocol != IPPROTO_TCP)
      continue;

    /* Ignore PCBs which were freed during copyout. */
    if (inp->inp_gencnt > in_orig->xig_gen)
      continue;

    if (((inp->inp_vflag & INP_IPV4) == 0)
	&& ((inp->inp_vflag & INP_IPV6) == 0))
      continue;

    conn_handle_ports (ntohs (inp->inp_lport), ntohs (inp->inp_fport),
	tp->t_state);
  } /* for (in_ptr) */

  in_orig = NULL;
  in_ptr = NULL;
  sfree (buffer);

  conn_submit_all ();

  return (0);
} /* int conn_read */
/* #endif HAVE_SYSCTLBYNAME */

#elif HAVE_LIBKVM_NLIST
static int kread (u_long addr, void *buf, int size)
{
  int status;

  status = kvm_read (kvmd, addr, buf, size);
  if (status != size)
  {
    ERROR ("tcpconns plugin: kvm_read failed (got %i, expected %i): %s\n",
	status, size, kvm_geterr (kvmd));
    return (-1);
  }
  return (0);
} /* int kread */

static int conn_init (void)
{
  char buf[_POSIX2_LINE_MAX];
  struct nlist nl[] =
  {
#define N_TCBTABLE 0
    { "_tcbtable" },
    { "" }
  };
  int status;

  kvmd = kvm_openfiles (NULL, NULL, NULL, O_RDONLY, buf);
  if (kvmd == NULL)
  {
    ERROR ("tcpconns plugin: kvm_openfiles failed: %s", buf);
    return (-1);
  }

  status = kvm_nlist (kvmd, nl);
  if (status < 0)
  {
    ERROR ("tcpconns plugin: kvm_nlist failed with status %i.", status);
    return (-1);
  }

  if (nl[N_TCBTABLE].n_type == 0)
  {
    ERROR ("tcpconns plugin: Error looking up kernel's namelist: "
	"N_TCBTABLE is invalid.");
    return (-1);
  }

  inpcbtable_off = (u_long) nl[N_TCBTABLE].n_value;
  inpcbtable_ptr = (struct inpcbtable *) nl[N_TCBTABLE].n_value;

  return (0);
} /* int conn_init */

static int conn_read (void)
{
  struct inpcbtable table;
#if !defined(__OpenBSD__) && (defined(__NetBSD_Version__) && __NetBSD_Version__ <= 699002700)
  struct inpcb *head;
#endif
  struct inpcb *next;
  struct inpcb inpcb;
  struct tcpcb tcpcb;
  int status;

  conn_reset_port_entry ();

  /* Read the pcbtable from the kernel */
  status = kread (inpcbtable_off, &table, sizeof (table));
  if (status != 0)
    return (-1);

#if defined(__OpenBSD__) || (defined(__NetBSD_Version__) && __NetBSD_Version__ > 699002700)
  /* inpt_queue is a TAILQ on OpenBSD */
  /* Get the first pcb */
  next = (struct inpcb *)TAILQ_FIRST (&table.inpt_queue);
  while (next)
#else
  /* Get the `head' pcb */
  head = (struct inpcb *) &(inpcbtable_ptr->inpt_queue);
  /* Get the first pcb */
  next = (struct inpcb *)CIRCLEQ_FIRST (&table.inpt_queue);

  while (next != head)
#endif
  {
    /* Read the pcb pointed to by `next' into `inpcb' */
    kread ((u_long) next, &inpcb, sizeof (inpcb));

    /* Advance `next' */
#if defined(__OpenBSD__) || (defined(__NetBSD_Version__) && __NetBSD_Version__ > 699002700)
    /* inpt_queue is a TAILQ on OpenBSD */
    next = (struct inpcb *)TAILQ_NEXT (&inpcb, inp_queue);
#else
    next = (struct inpcb *)CIRCLEQ_NEXT (&inpcb, inp_queue);
#endif

    /* Ignore sockets, that are not connected. */
#ifdef __NetBSD__
    if (inpcb.inp_af == AF_INET6)
      continue; /* XXX see netbsd/src/usr.bin/netstat/inet6.c */
#else
    if (!(inpcb.inp_flags & INP_IPV6)
	&& (inet_lnaof(inpcb.inp_laddr) == INADDR_ANY))
      continue;
    if ((inpcb.inp_flags & INP_IPV6)
	&& IN6_IS_ADDR_UNSPECIFIED (&inpcb.inp_laddr6))
      continue;
#endif

    kread ((u_long) inpcb.inp_ppcb, &tcpcb, sizeof (tcpcb));
    conn_handle_ports (ntohs(inpcb.inp_lport), ntohs(inpcb.inp_fport), tcpcb.t_state);
  } /* while (next != head) */

  conn_submit_all ();

  return (0);
}
/* #endif HAVE_LIBKVM_NLIST */

#elif KERNEL_AIX

static int conn_read (void)
{
  int size;
  int i;
  int nconn;
  void *data;
  struct netinfo_header *header;
  struct netinfo_conn *conn;

  conn_reset_port_entry ();

  size = netinfo(NETINFO_TCP, 0, 0, 0);
  if (size < 0)
  {
    ERROR ("tcpconns plugin: netinfo failed return: %i", size);
    return (-1);
  }

  if (size == 0)
    return (0);

  if ((size - sizeof (struct netinfo_header)) % sizeof (struct netinfo_conn))
  {
    ERROR ("tcpconns plugin: invalid buffer size");
    return (-1);
  }

  data = malloc(size);
  if (data == NULL)
  {
    ERROR ("tcpconns plugin: malloc failed");
    return (-1);
  }

  if (netinfo(NETINFO_TCP, data, &size, 0) < 0)
  {
    ERROR ("tcpconns plugin: netinfo failed");
    free(data);
    return (-1);
  }

  header = (struct netinfo_header *)data;
  nconn = header->size;
  conn = (struct netinfo_conn *)(data + sizeof(struct netinfo_header));

  for (i=0; i < nconn; conn++, i++)
  {
    conn_handle_ports (conn->srcport, conn->dstport, conn->tcp_state);
  }

  free(data);

  conn_submit_all ();

  return (0);
}
#endif /* KERNEL_AIX */

static int conn_shutdown(void) {
  /* Signal other threads (currently only the cache cleanup thread for
   * fine-grained tcp_info reporting on Linux) to stop. */
  shutdown_module = 1;

  /* Clean up the cache. This lock can only contend with cleanup, so
   * it doesn't matter if we block it for a few msec. */
  {
    char *key;
    counter_cache_entry_t *entry;
    size_t cache_size_freed = 0;
    pthread_mutex_lock(&counter_cache_lock);
    while (c_avl_pick(counter_cache_tree, (void**)&key, (void**)&entry) == 0) {
      sfree(key);
      counter_cache_free_entry(entry);
      cache_size_freed++;
    }
    c_avl_destroy(counter_cache_tree);
    counter_cache_tree = NULL;
    pthread_mutex_unlock(&counter_cache_lock);
    INFO("tcpconns conn_shutdown: released %zu cache elements",
         cache_size_freed);
  }

  return 0;
}

void module_register (void)
{
  plugin_register_config("tcpconns", conn_config, config_keys, config_keys_num);
#if KERNEL_LINUX
  plugin_register_init ("tcpconns", conn_init);
#elif HAVE_SYSCTLBYNAME
  /* no initialization */
#elif HAVE_LIBKVM_NLIST
  plugin_register_init ("tcpconns", conn_init);
#elif KERNEL_AIX
  /* no initialization */
#endif
  plugin_register_read ("tcpconns", conn_read);

  plugin_register_shutdown("tcpconns", conn_shutdown);
} /* void module_register */

/*
 * vim: set shiftwidth=2 softtabstop=2 tabstop=8 fdm=marker :
 */
