man_MANS = liboping.3 ping_construct.3 ping_setopt.3

liboping_3_SOURCES = liboping.pod
ping_construct_3_SOURCES = ping_construct.pod
ping_setopt_3_SOURCES = ping_setopt.pod

.pod.1:
	pod2man --release=$(VERSION) --center=$(PACKAGE) $< >$@

.pod.3:
	pod2man --section=3 --release=$(VERSION) --center=$(PACKAGE) $< >$@
