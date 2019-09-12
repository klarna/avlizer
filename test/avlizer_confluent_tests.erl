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

simple_fp_test_() ->
  with_meck(
   fun() ->
       Name = <<"name-1">>,
       {ok, Fp} = avlizer_confluent:register_schema_with_fp(Name, test_type()),
       Bin = avlizer_confluent:encode(Name, Fp, 42),
       42 = avlizer_confluent:decode(Name, Fp, Bin)
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

get_encoder_decoder_fp_test_() ->
  with_meck(
    fun() ->
        Name = <<"name-2">>,
        Sc = test_schema(),
        {ok, Fp} = avlizer_confluent:register_schema_with_fp(Name, Sc),
        Encoder = avlizer_confluent:get_encoder(Name, Fp),
        Decoder = avlizer_confluent:get_decoder(Name, Fp),
        Bin = avlizer_confluent:encode(Encoder, 42),
        42 = avlizer_confluent:decode(Decoder, Bin)
    end).

get_encoder_decoder_assign_fp_test_() ->
  with_meck(
    fun() ->
        Name = "name-3",
        Fp = 1,
        Sc = test_schema(),
        ok = avlizer_confluent:register_schema_with_fp(Name, Fp, Sc),
        Encoder = avlizer_confluent:get_encoder(Name, Fp),
        Decoder = avlizer_confluent:get_decoder(Name, Fp),
        Bin = avlizer_confluent:encode(Encoder, 42),
        42 = avlizer_confluent:decode(Decoder, Bin)
    end).

get_latest_schema_test() ->
  with_meck(
    fun() ->
      {ok, _Id} = avlizer_confluent:register_schema("subj", Schema = test_schema()),
      {ok, Schema} = avlizer_confluent:get_schema("subj")
    end).

get_schema_test() ->
  with_meck(
    fun() ->
      {ok, _Id} = avlizer_confluent:register_schema("subj", Schema = test_schema()),
      {ok, Schema} = avlizer_confluent:get_schema("subj", 1),
      {ok, Schema} = avlizer_confluent:get_schema("subj", "1")
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

make_encoder_decoder_with_fp_test_() ->
  with_meck(
    fun() ->
        Name = <<"name-4">>,
        Fp = "md5-hex",
        Sc = test_schema(),
        ok = avlizer_confluent:register_schema_with_fp(Name, Fp, Sc),
        Encoder = avlizer_confluent:make_encoder(Name, Fp),
        Decoder = avlizer_confluent:make_decoder(Name, Fp),
        Bin = avlizer_confluent:encode(Encoder, 42),
        42 = avlizer_confluent:decode(Decoder, Bin)
    end).

register_without_cache_test_() ->
  with_meck(
    fun() ->
        Name = <<"name-5">>,
        Fp = 2,
        Sc = test_schema(),
        Ref = erlang:monitor(process, whereis(avlizer_confluent)),
        application:stop(?APPLICATION),
        receive
          {'DOWN', Ref, process, _, shutdown} ->
            ok
        end,
        ok = avlizer_confluent:register_schema_with_fp(Name, Fp, Sc)
    end).

no_redownload_test_() ->
  with_meck(
    fun() ->
        Name = "name-6",
        Fp = 2,
        Sc = test_schema(),
        ok = avlizer_confluent:register_schema_with_fp(Name, Fp, Sc),
        ?assertMatch({ok, _},
          gen_server:call(avlizer_confluent, {download, {Name, Fp}})),
        ?assertMatch({ok, _},
          gen_server:call(avlizer_confluent, {download, {Name, Fp}})),
        %% expect 2 calls, one upload, one download
        ?assertEqual(2, meck_history:num_calls('_', httpc, request, '_'))
    end).

with_meck(RunTestFun) ->
  {setup, fun setup/0, fun cleanup/1, RunTestFun}.

setup() ->
  os:putenv("AVLIZER_CONFLUENT_SCHEMAREGISTRY_URL", "theurl"),
  application:ensure_all_started(?APPLICATION),
  meck:new(httpc, [passthrough]),
  meck:expect(httpc, request,
              fun(get, {"theurl" ++ Endpoint, _}, _, _) ->
                  Body = test_download(Endpoint),
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
test_download("/schemas/ids/" ++ _Id) ->
  SchemaJSON = test_schema(),
  jsone:encode(#{<<"schema">> => SchemaJSON});
test_download(_) ->
  SchemaJSON = test_schema(),
  jsone:encode(#{<<"schema">> => SchemaJSON, <<"id">> => <<"1">>}).

test_schema() ->
  avro:encode_schema(test_type()).

test_type() ->
  avro_primitive:int_type().

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
