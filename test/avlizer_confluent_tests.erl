-module(avlizer_confluent_tests).

-include_lib("eunit/include/eunit.hrl").

%% The simplest use case, the least performant though.
simple_test_() ->
  with_meck(
   fun() ->
       {ok, Id} = avlizer_confluent:register_schema("subj", test_type()),
       Bin = avlizer_confluent:encode(Id, 42),
       42 = avlizer_confluent:decode(Id, Bin)
   end).

get_encoder_decoder_test_() ->
  with_meck(
    fun() ->
        {ok, Id} = avlizer_confluent:register_schema("subj", test_schema()),
        Encoder = avlizer_confluent:get_encoder(Id),
        Decoder = avlizer_confluent:get_decoder(Id),
        Bin = avlizer_confluent:encode(Encoder, 42),
        42 = avlizer_confluent:decode(Decoder, Bin)
    end).

make_encoder_decoder_test_() ->
  with_meck(
    fun() ->
        {ok, Id} = avlizer_confluent:register_schema("subj", test_schema()),
        Encoder = avlizer_confluent:make_encoder(Id),
        Decoder = avlizer_confluent:make_decoder(Id),
        Bin = avlizer_confluent:encode(Encoder, 42),
        42 = avlizer_confluent:decode(Decoder, Bin)
    end).

with_meck(RunTestFun) ->
  {setup, fun setup/0, fun cleanup/1, RunTestFun}.

setup() ->
  os:putenv("AVLIZER_CONFLUENT_SCHEMAREGISTRY_URL", "theurl"),
  application:ensure_all_started(?APPLICATION),
  meck:new(httpc, [passthrough]),
  meck:expect(httpc, request,
              fun(get, {"theurl" ++ _, _}, _, _) ->
                  Body = test_download(),
                  {ok, {{ignore, 200, "OK"}, headers, Body}};
                 (post, {"theurl" ++ _, _, _, _}, _, _) ->
                  Body = <<"{\"id\": 1}">>,
                  {ok, {{ignore, 200, "OK"}, headers, Body}}
              end),
  ok.

cleanup(_) ->
  os:unsetenv("AVLIZER_CONFLUENT_SCHEMAREGISTRY_URL"),
  meck:unload(),
  application:stop(?APPLICATION),
  ok.

%% make a fake JSON as if downloaded from schema registry
test_download() ->
  SchemaJSON = test_schema(),
  jsone:encode(#{<<"schema">> => SchemaJSON}).

test_schema() ->
  avro:encode_schema(test_type()).

test_type() ->
  avro_primitive:int_type().

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
