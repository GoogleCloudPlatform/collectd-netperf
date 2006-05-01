man_MANS = liboping.3 ping_construct.3 ping_setopt.3 ping_host_add.3 \
	   ping_send.3 ping_get_error.3 ping_iterator_get.3 \
	   ping_iterator_get_info.3 ping_iterator_get_context.3

liboping_3_SOURCES = liboping.pod
ping_construct_3_SOURCES = ping_construct.pod
ping_setopt_3_SOURCES = ping_setopt.pod
ping_host_add_3_SOURCES = ping_host_add.pod
ping_send_3_SOURCES = ping_send.pod
ping_get_error_3_SOURCES = ping_get_error.pod
ping_iterator_get_3_SOURCES = ping_iterator_get.pod
ping_iterator_get_info_3_SOURCES = ping_iterator_get_info.pod
ping_iterator_get_context_3_SOURCES = ping_iterator_get_context.pod

.pod.1:
	pod2man --release=$(VERSION) --center=$(PACKAGE) $< >$@

.pod.3:
	pod2man --section=3 --release=$(VERSION) --center=$(PACKAGE) $< >$@
