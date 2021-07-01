-module(avlizer_confluent_tests).

-include_lib("eunit/include/eunit.hrl").

-define(ENV_SCHEMAREGISTRY_URL, "AVLIZER_CONFLUENT_SCHEMAREGISTRY_URL").
-define(ENV_SCHEMAREGISTRY_AUTH_MECHANISM, "AVLIZER_CONFLUENT_SCHEMAREGISTRY_AUTH_MECHANISM").
-define(ENV_SCHEMAREGISTRY_AUTH_USERNAME, "AVLIZER_CONFLUENT_SCHEMAREGISTRY_AUTH_USERNAME").
-define(ENV_SCHEMAREGISTRY_AUTH_PASSWORD, "AVLIZER_CONFLUENT_SCHEMAREGISTRY_AUTH_PASSWORD").

%% The simplest use case, the least performant though.
simple_test_() ->
  with_meck(
   fun() ->
       {ok, Id} = avlizer_confluent:register_schema("simple", test_type()),
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
        {ok, Id} = avlizer_confluent:register_schema("simple", test_schema()),
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

make_encoder_decoder_test_() ->
  with_meck(
    fun() ->
        {ok, Id} = avlizer_confluent:register_schema("simple", test_schema()),
        Encoder = avlizer_confluent:make_encoder(Id),
        Decoder = avlizer_confluent:make_decoder(Id),
        Bin = avlizer_confluent:encode(Encoder, 42),
        42 = avlizer_confluent:decode(Decoder, Bin)
    end).

make_encoder_decoder_with_fp_test_() ->
  with_meck(
    fun() ->
        Name = <<"name-4">>,
        Fp = <<"md5-hex">>,
        Sc = test_schema(),
        ok = avlizer_confluent:register_schema_with_fp(Name, Fp, Sc),
        Encoder = avlizer_confluent:make_encoder(Name, Fp),
        Decoder = avlizer_confluent:make_decoder(Name, Fp),
        Bin = avlizer_confluent:encode(Encoder, 42),
        42 = avlizer_confluent:decode(Decoder, Bin)
    end).

pass_codec_options_to_decoder_test_() ->
  with_meck(
    fun() ->
      {ok, Id} = avlizer_confluent:register_schema("complex", complex_test_schema()),
      Encoder = avlizer_confluent:make_encoder(Id),
      CodecOptions = [{map_type, map}, {record_type, map}, {encoding, avro_binary}],
      Decoder = avlizer_confluent:make_decoder(Id, CodecOptions),
      Bin = avlizer_confluent:encode(Encoder, #{f1 => #{a => 1}}),
      Decoded = avlizer_confluent:decode(Decoder, Bin),
      true = is_map(Decoded),
      true = is_map(maps:get(<<"f1">>, Decoded))
    end).

pass_codec_options_to_decode_test_() ->
  with_meck(
    fun() ->
      {ok, Id} = avlizer_confluent:register_schema("complex", complex_test_schema()),
      Encoder = avlizer_confluent:make_encoder(Id),
      CodecOptions = [{map_type, map}, {record_type, map}, {encoding, avro_binary}],
      Bin = avlizer_confluent:encode(Encoder, #{f1 => #{a => 1}}),
      Decoded = avlizer_confluent:decode(Id, Bin, CodecOptions),
      true = is_map(Decoded),
      true = is_map(maps:get(<<"f1">>, Decoded))
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

simple_auth_test_() ->
  with_meck_auth(
   fun() ->
       {ok, Id} = avlizer_confluent:register_schema("simple", test_type()),
       Bin = avlizer_confluent:encode(Id, 85),
       85 = avlizer_confluent:decode(Id, Bin)
   end).

simple_auth_file_test_() ->
  with_meck_auth_file(
   fun() ->
       {ok, Id} = avlizer_confluent:register_schema("simple", test_type()),
       Bin = avlizer_confluent:encode(Id, 3),
       3 = avlizer_confluent:decode(Id, Bin)
   end).

with_meck(RunTestFun) ->
  {setup, 
    fun () -> setup_url_env(), setup_app(), setup_meck() end, 
    fun (_) -> cleanup_url_env(), cleanup_meck(), cleanup_app() end, 
    RunTestFun}.

with_meck_auth(RunTestFun) ->
  {setup,
    fun () -> setup_url_env(), setup_auth_envs(basic), setup_app(), setup_meck() end, 
    fun (_) -> cleanup_url_env(), cleanup_auth_envs(), cleanup_meck(), cleanup_app() end, 
    RunTestFun}.

with_meck_auth_file(RunTestFun) ->
  {setup,
    fun () -> setup_url_with_auth_file(basic), setup_app(), setup_meck() end, 
    fun (_) -> cleanup_url_with_auth_file(), cleanup_meck(), cleanup_app() end, 
    RunTestFun}.

setup_meck() ->
  meck:new(httpc, [passthrough]),
  meck:expect(httpc, request,
              fun(get, {"theurl/schemas/ids/" ++ ID, _}, _, _) ->
                  Body = test_download(ID),
                  {ok, {{ignore, 200, "OK"}, headers, Body}};
                 (get, {"theurl" ++ _, _}, _, _) ->
                  Body = test_download(),
                  {ok, {{ignore, 200, "OK"}, headers, Body}};
                 (post, {"theurl/subjects/complex" ++ _, _, _, _}, _, _) ->
                  Body = <<"{\"id\": 2}">>,
                  {ok, {{ignore, 200, "OK"}, headers, Body}};
                 (post, {"theurl" ++ _, _, _, _}, _, _) ->
                  Body = <<"{\"id\": 1}">>,
                  {ok, {{ignore, 200, "OK"}, headers, Body}}
              end),
  ok.

cleanup_meck() -> 
  meck:unload(), 
  ok.

setup_app() -> 
  application:ensure_all_started(?APPLICATION), 
  ok.

cleanup_app() -> 
  application:stop(?APPLICATION), 
  ok.

setup_url_env() -> 
  os:putenv(?ENV_SCHEMAREGISTRY_URL, "theurl"), 
  ok.

cleanup_url_env() -> 
  os:unsetenv(?ENV_SCHEMAREGISTRY_URL), 
  ok.

setup_auth_envs(Mechanism) ->
  os:putenv(?ENV_SCHEMAREGISTRY_AUTH_MECHANISM, atom_to_list(Mechanism)),
  os:putenv(?ENV_SCHEMAREGISTRY_AUTH_USERNAME, "avlizer_username"),
  os:putenv(?ENV_SCHEMAREGISTRY_AUTH_PASSWORD, "avlizer_password"),
  ok.

cleanup_auth_envs() ->
  os:unsetenv(?ENV_SCHEMAREGISTRY_AUTH_MECHANISM),
  os:unsetenv(?ENV_SCHEMAREGISTRY_AUTH_USERNAME),
  os:unsetenv(?ENV_SCHEMAREGISTRY_AUTH_PASSWORD),
  ok.

setup_url_with_auth_file(Mechanism) ->
  Vars = #{
    schema_registry_url => "theurl", 
    schema_registry_auth => {Mechanism, "./priv/auth_test.txt"}},
  application:set_env(?APPLICATION, avlizer_confluent, Vars),
  ok.

cleanup_url_with_auth_file() ->
  application:set_env(?APPLICATION, avlizer_confluent, #{}).

%% make a fake JSON as if downloaded from schema registry
test_download() ->
  test_download("1").

test_download(Id) ->
  SchemaJSON = case Id of
    "1" -> test_schema();
    "2" -> complex_test_schema()
  end,
  jsone:encode(#{<<"schema">> => SchemaJSON}).

test_schema() ->
  avro:encode_schema(test_type()).

complex_test_schema() ->
  avro:encode_schema(test_complex_record()).

test_type() ->
  avro_primitive:int_type().

test_complex_record() ->
  avro_record:type(
      <<"MyRecord">>,
      [avro_record:define_field(f1, avro_map:type(avro_primitive:int_type()))],
      [{namespace, 'com.example'}]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
