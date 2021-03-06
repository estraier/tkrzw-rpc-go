# Makefile for Tkrzw-RPC for Go

PACKAGE = tkrzw-rpc-go
VERSION = 0.1.4
PACKAGEDIR = $(PACKAGE)-$(VERSION)
PACKAGETGZ = $(PACKAGE)-$(VERSION).tar.gz

GOCMD = go
RUNENV = LD_LIBRARY_PATH=.:/lib:/usr/lib:/usr/local/lib:$(HOME)/lib
MODULEFILES = tkrzw_rpc_pb.rb tkrzw_rpc.rb tkrzw_rpc_services_pb.rb

all :
	$(RUNENV) $(GOCMD) get
	$(RUNENV) $(GOCMD) build
	[ ! -f perf/Makefile ] || cd perf && $(MAKE)
	[ ! -f wicked/Makefile ] || cd wicked && $(MAKE)
	@printf '\n'
	@printf '#================================================================\n'
	@printf '# Ready to install.\n'
	@printf '#================================================================\n'

clean :
	rm -rf casket casket* *~ *.tmp *.tkh *.tkt *.tks *.flat *.log *.so \
	  hoge moge tako ika uni go.sum
	[ ! -f perf/Makefile ] || cd perf && $(MAKE) clean
	[ ! -f wicked/Makefile ] || cd wicked && $(MAKE) clean

install :
	@printf '\n'
	@printf '#================================================================\n'
	@printf '# Installation is not necessary.\n'
	@printf '# Just import "github.com/estraier/tkrzw-go-rpc" in your code and run "go get".\n'
	@printf '#================================================================\n'

dist :
	$(MAKE) fmt
	[ ! -f perf/Makefile ] || cd perf && $(MAKE) fmt
	[ ! -f wicked/Makefile ] || cd wicked && $(MAKE) fmt
	[ ! -f example1/Makefile ] || cd example1 && $(MAKE) fmt
	[ ! -f example2/Makefile ] || cd example2 && $(MAKE) fmt
	$(MAKE) distclean
	rm -Rf "../$(PACKAGEDIR)" "../$(PACKAGETGZ)"
	cd .. && cp -R tkrzw-rpc-go $(PACKAGEDIR) && \
	  tar --exclude=".*" -cvf - $(PACKAGEDIR) | gzip -c > $(PACKAGETGZ)
	rm -Rf "../$(PACKAGEDIR)"
	sync ; sync

distclean : clean apidocclean
	[ ! -f perf/Makefile ] || cd perf && $(MAKE) clean
	[ ! -f wicked/Makefile ] || cd wicked && $(MAKE) clean
	[ ! -f example1/Makefile ] || cd example1 && $(MAKE) clean
	[ ! -f example2/Makefile ] || cd example2 && $(MAKE) clean

check : test runperf runwicked
	@printf '\n'
	@printf '#================================================================\n'
	@printf '# Checking completed.\n'
	@printf '#================================================================\n'

test :
	$(RUNENV) $(GOCMD) get
	$(RUNENV) $(GOCMD) test -v

runperf :
	[ ! -f perf/Makefile ] || cd perf && $(MAKE) run

runwicked :
	[ ! -f wicked/Makefile ] || cd wicked && $(MAKE) run

vet :
	$(RUNENV) $(GOCMD) get
	$(RUNENV) $(GOCMD) vet

fmt :
	$(RUNENV) $(GOCMD) get
	$(RUNENV) $(GOCMD) fmt

apidoc :
	rm -rf api-doc tmp-doc
	mkdir tmp-doc ; cp go.mod doc.go util.go status.go remote_dbm.go iterator.go tmp-doc
	cd tmp-doc ; PATH=$$PATH:$$HOME/go/bin:$$HOME/.local/bin \
	  godoc -http "localhost:8080" -play -goroot /usr/share/go & sleep 2
	mkdir api-doc
	curl -s "http://localhost:8080/lib/godoc/style.css" > api-doc/style.css
	echo '#topbar { display: none; }' >> api-doc/style.css
	echo '#short-nav, #pkg-subdirectories, .pkg-dir { display: none; }' >> api-doc/style.css
	echo 'div.param { margin-left: 2.5ex; max-width: 48rem; }' >> api-doc/style.css
	echo 'div.param .tag { font-size: 80%; opacity: 0.8; }' >> api-doc/style.css
	echo 'div.param .name { font-family: monospace; }' >> api-doc/style.css
	echo 'div.list { display: list-item; list-style: circle outside; }' >> api-doc/style.css
	echo 'div.list { margin-left: 4.5ex; max-width: 48rem; }' >> api-doc/style.css
	curl -s "http://localhost:8080/pkg/github.com/estraier/tkrzw-rpc-go/" |\
	  grep -v '^<script.*</script>$$' |\
	  sed -e 's/\/[a-z\/]*style.css/style.css/' \
	    -e 's/\/pkg\/builtin\/#/#/' \
	    -e 's/^\(@param\) \+\([a-zA-Z0-9_]\+\) \+\(.*\)/<div class="param"><span class="tag">\1<\/span> <span class="name">\2<\/span> \3<\/div>/' \
	    -e 's/^\(@return\) \+\(.*\)/<div class="param"><span class="tag">\1<\/span> \2<\/div>/' \
	    -e 's/^- \(.*\)/<div class="list">\1<\/div>/' > api-doc/index.html
	killall godoc

apidocclean :
	rm -rf api-doc tmp-doc

protocode : tkrzw_rpc.proto
	PATH="$$PATH:$$($(GOCMD) env GOPATH)/bin" ; export PATH ; \
	  protoc --go_out=. --go_opt=paths=source_relative \
	    --go-grpc_out=. --go-grpc_opt=paths=source_relative tkrzw_rpc.proto

.PHONY: all clean install uninstall dist distclean check apidoc apidocclean pbrb

# END OF FILE
