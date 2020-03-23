PROJECT = avlizer

all: compile
t: eunit xref dialyzer

rebar_cmd = rebar3 $(profile:%=as %)

.PHONY: compile
compile:
	@$(rebar_cmd) compile

.PHONY: deps
deps:
	@$(rebar_cmd) get-deps

.PHONY: xref
xref:
	@$(rebar_cmd) xref

.PHONY: clean
clean:
	@$(rebar_cmd) clean

.PHONY: distclean
distclean:
	@$(rebar_cmd) clean
	@rm -rf _build

.PHONY: eunit
eunit:
	@$(rebar_cmd) eunit -v

.PHONY: edoc
edoc: profile=edown
edoc:
	@$(rebar_cmd) edoc

.PHONY: cover
cover:
	@$(rebar_cmd) cover -v

.PHONY: dialyzer
dialyzer:
	@$(rebar_cmd) dialyzer

.PHONY: hex-publish
hex-publish: profile=dev
hex-publish:
	@$(rebar_cmd) hex publish
