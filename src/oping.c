/**
 * Object oriented C module to send ICMP and ICMPv6 `echo's.
 * Copyright (C) 2006  Florian octo Forster <octo at verplant.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 */

#if HAVE_CONFIG_H
# include <config.h>
#endif

#if STDC_HEADERS
# include <stdlib.h>
# include <stdio.h>
# include <string.h>
# include <errno.h>
# include <assert.h>
#else
# error "You don't have the standard C99 header files installed"
#endif /* STDC_HEADERS */

#if HAVE_NETDB_H
# include <netdb.h> /* NI_MAXHOST */
#endif

#include <liboping.h>

int main (int argc, char **argv)
{
	pingobj_t      *ping;
	pingobj_iter_t *iter;

	int i;

	if (argc < 2)
	{
		printf ("Usage: %s <host> [host [host [...]]]\n", argv[0]);
		return (1);
	}

	if ((ping = ping_construct ()) == NULL)
	{
		fprintf (stderr, "ping_construct failed\n");
		return (1);
	}

	for (i = 1; i < argc; i++)
	{
		if (ping_host_add (ping, argv[i]) > 0)
		{
			fprintf (stderr, "ping_host_add (%s) failed\n", argv[i]);
			return (1);
		}
	}

	while (1)
	{
		if (ping_send (ping) < 0)
		{
			fprintf (stderr, "ping_send failed\n");
			return (1);
		}

		for (iter = ping_iterator_get (ping);
				iter != NULL;
				iter = ping_iterator_next (iter))
		{
			char     host[NI_MAXHOST];
			char     addr[NI_MAXHOST];
			double   latency;
			uint16_t sequence;
			size_t   buffer_len;

			buffer_len = sizeof (host);
			if (ping_iterator_get_info (iter, PING_INFO_HOSTNAME,
						host, &buffer_len) != 0)
			{
				fprintf (stderr, "ping_iterator_get_info failed.\n");
				continue;
			}

			buffer_len = sizeof (addr);
			if (ping_iterator_get_info (iter, PING_INFO_ADDRESS,
						addr, &buffer_len) != 0)
			{
				fprintf (stderr, "ping_iterator_get_info failed.\n");
				continue;
			}

			buffer_len = sizeof (latency);
			ping_iterator_get_info (iter, PING_INFO_LATENCY,
					&latency, &buffer_len);

			buffer_len = sizeof (sequence);
			ping_iterator_get_info (iter, PING_INFO_SEQUENCE,
					&sequence, &buffer_len);

			printf ("echo reply from %s (%s): icmp_seq=%u time=%.1f ms\n",
					host, addr, (unsigned int) sequence, latency);
		}

		sleep (1);
	}

	return (0);
}

/*
 * octo@leeloo:~ $ ping verplant.org
 * PING verplant.org (213.95.21.52) 56(84) bytes of data.
 * 64 bytes from verplant.org (213.95.21.52): icmp_seq=1 ttl=57 time=141 ms
 * 64 bytes from verplant.org (213.95.21.52): icmp_seq=2 ttl=57 time=47.0 ms
 * 64 bytes from verplant.org (213.95.21.52): icmp_seq=3 ttl=57 time=112 ms
 * 64 bytes from verplant.org (213.95.21.52): icmp_seq=4 ttl=57 time=46.7 ms
 *
 * --- verplant.org ping statistics ---
 *  4 packets transmitted, 4 received, 0% packet loss, time 3002ms
 *  rtt min/avg/max/mdev = 46.782/86.870/141.373/41.257 ms
 */
