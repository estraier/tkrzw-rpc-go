# Makefile for the performance checker for Tkrzw-RPC

GOCMD := go
RUNENV := LD_LIBRARY_PATH=.:/lib:/usr/lib:/usr/local/lib:$(HOME)/lib

build :
	$(RUNENV) $(GOCMD) get
	$(RUNENV) $(GOCMD) build perf.go

run :
	$(RUNENV) $(GOCMD) get
	$(RUNENV) $(GOCMD) run perf.go --iter 10000 --threads 3
	$(RUNENV) $(GOCMD) run perf.go --iter 10000 --threads 3 --random

vet :
	$(RUNENV) $(GOCMD) get
	$(RUNENV) $(GOCMD) vet

fmt :
	$(RUNENV) $(GOCMD) get
	$(RUNENV) $(GOCMD) fmt

clean :
	rm -rf perf casket* *.tkh *.tkt *.tks *~ hoge moge tako ika uni go.sum
