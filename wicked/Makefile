# Makefile for the wicked testing for Tkrzw-RPC

GOCMD := go
RUNENV := LD_LIBRARY_PATH=.:/lib:/usr/lib:/usr/local/lib:$(HOME)/lib

build :
	$(RUNENV) $(GOCMD) get
	$(RUNENV) $(GOCMD) build wicked.go

run :
	$(RUNENV) $(GOCMD) get
	$(RUNENV) $(GOCMD) run wicked.go --iter 5000 --threads 3

vet :
	$(RUNENV) $(GOCMD) get
	$(RUNENV) $(GOCMD) vet

fmt :
	$(RUNENV) $(GOCMD) get
	$(RUNENV) $(GOCMD) fmt

clean :
	rm -rf wicked casket* *.tkh *.tkt *.tks *~ hoge moge tako ika uni go.sum
