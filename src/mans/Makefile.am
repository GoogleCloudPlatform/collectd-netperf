man_MANS = liboping.3 ping_construct.3 ping_setopt.3 ping_host_add.3 ping_send.3

liboping_3_SOURCES = liboping.pod
ping_construct_3_SOURCES = ping_construct.pod
ping_setopt_3_SOURCES = ping_setopt.pod
ping_host_add_3_SOURCES = ping_host_add.pod
ping_send_3_SOURCES = ping_send.pod

.pod.1:
	pod2man --release=$(VERSION) --center=$(PACKAGE) $< >$@

.pod.3:
	pod2man --section=3 --release=$(VERSION) --center=$(PACKAGE) $< >$@
