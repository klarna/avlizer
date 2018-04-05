%%%-----------------------------------------------------------------------------
%%%
%%% Copyright (c) 2018 Klarna AB
%%%
%%% This file is provided to you under the Apache License,
%%% Version 2.0 (the "License"); you may not use this file
%%% except in compliance with the License.  You may obtain
%%% a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing,
%%% software distributed under the License is distributed on an
%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%% KIND, either express or implied.  See the License for the
%%% specific language governing permissions and limitations
%%% under the License.
%%%-----------------------------------------------------------------------------

%% @doc This module implements the confluent.io schema tagging convention.
%% http://docs.confluent.io/
%% current/schema-registry/docs/serializer-formatter.html
%%
%% That is: Payload is tagged with 5-byte schema ID.
%%          The first byte is currently always 0 (work as version)
%%          And the following 4 bytes is the schema registration ID
%%          retrieved from schema-registry

-module(avlizer_confluent).
-behaviour(gen_server).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-export([ start_link/0
        , stop/0
        ]).

-export([ decode/1
        , decode/2
        , encode/2
        , make_decoder/1
        , make_encoder/1
        , get_decoder/1
        , get_encoder/1
        , maybe_download/1
        , register_schema/2
        , tag_data/2
        , untag_data/1
        ]).


-export_type([ regid/0
             ]).

%% confluent.io convention magic version
-define(VSN, 0).
-define(SERVER, ?MODULE).
-define(CACHE, ?MODULE).
-define(HTTPC_TIMEOUT, 10000).

-define(IS_REGID(Id), is_integer(Id)).
-type regid() :: non_neg_integer().

-define(ASSIGNED_NAME, <<"_avlizer_assigned">>).
-define(LKUP(RegId), fun(?ASSIGNED_NAME) -> ?MODULE:maybe_download(RegId) end).

%%%_* APIs =====================================================================

%% @doc Start a gen_server which owns a ets cache for schema
%% registered in schema registry.
%% Schema registry URL is configured in app env.
-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Stop the gen_server, mostly used for tets.
-spec stop() -> ok.
stop() ->
  ok = gen_server:call(?SERVER, stop).

%% @doc Make an avro decoder from the given schema registration ID.
%% This call has an overhead of ets lookup (and maybe a `gen_server'
%% call to perform a http download) to fetch the avro schema, hence
%% the caller should keep the built decoder function and re-use it
%% for the same reg-id.
%% The decoder made by this API has better performance than the one
%% returned from `get_decoder/1' which has the lookup overhead included
%% in the decoder. It may not be performant when decoding a large
%% number (or a stream) of objects having the same registration ID.
-spec make_decoder(regid()) -> avro:simple_decoder().
make_decoder(RegId) ->
  Schema = maybe_download(RegId),
  avro:make_simple_decoder(Schema, []).

%% @doc Make an avro encoder from the given schema registration ID.
%% This call has an overhead of ets lookup (and maybe a `gen_server'
%% call to perform a http download) to fetch the avro schema, hence
%% the caller should keep the built decoder function and re-use it
%% for the same reg-id.
%% The encoder made by this API has better performance than the one
%% returned from `get_encoder/1' which has the lookup overhead included
%% in the encoder. It may not be performant when encoding a large
%% number of objects in a tight loop.
-spec make_encoder(regid()) -> avro:simple_encoder().
make_encoder(RegId) ->
  Schema = maybe_download(RegId),
  avro:make_simple_encoder(Schema, []).

%% @doc Get avro decoder by lookup already decoded avro schema
%% from cache, and make a decoder from it.
%% Schema lookup is performed inside the decoder function for each call,
%% use `make_enodcer/1' for better performance.
-spec get_decoder(regid()) -> avro:simple_decoder().
get_decoder(RegId) ->
  Decoder = avro:make_decoder(?LKUP(RegId), [{encoding, avro_binary}]),
  fun(Bin) -> Decoder(?ASSIGNED_NAME, Bin) end.

%% @doc Get avro decoder by lookup already decode avro schema
%% from cache, and make a encoder from it.
%% Schema lookup is performed inside the encoder function for each call,
%% use `make_enodcer/1' for better performance.
get_encoder(RegId) ->
  Encoder = avro:make_encoder(?LKUP(RegId), [{encoding, avro_binary}]),
  fun(Input) -> Encoder(?ASSIGNED_NAME, Input) end.

%% @doc Lookup cache for decoded schema, try to download if not found.
-spec maybe_download(regid()) -> avro:avro_type().
maybe_download(RegId) ->
  case lookup_cache(RegId) of
    false ->
      case download(RegId) of
        {ok, Type} ->
          Type;
        {error, Reason} ->
          erlang:error(Reason)
      end;
    {ok, Type} ->
      Type
  end.

%% @doc Register a schema.
-spec register_schema(string(), binary() | avro:type()) ->
        {ok, regid()} | {error, any()}.
register_schema(Subject, JSON) when is_binary(JSON) ->
  gen_server:call(?SERVER, {register, Subject, JSON}, infinity);
register_schema(Subject, Schema) ->
  JSON = avro:encode_schema(Schema),
  register_schema(Subject, JSON).

%% @doc Tag binary data with schema registration ID.
-spec tag_data(regid(), binary()) -> binary().
tag_data(SchemaId, AvroBinary) ->
  iolist_to_binary([<<?VSN:8, SchemaId:32/unsigned-integer>>, AvroBinary]).

%% @doc Get schema registeration ID and avro binary from tagged data.
-spec untag_data(binary()) -> {regid(), binary()}.
untag_data(<<?VSN:8, RegId:32/unsigned-integer, Body/binary>>) ->
  {RegId, Body}.

%% @doc Decode tagged payload
-spec decode(binary()) -> avro:out().
decode(Bin) ->
  {RegId, Payload} = untag_data(Bin),
  decode(RegId, Payload).

%% @doc Decode untagged payload.
-spec decode(regid() | avro:simple_decoder(), binary()) -> avro:out().
decode(RegId, Bin) when ?IS_REGID(RegId) ->
  decode(get_decoder(RegId), Bin);
decode(Decoder, Bin) when is_function(Decoder) ->
  Decoder(Bin).

%% @doc Encoded avro-binary with schema tag.
-spec encode(regid() | avro:simple_encoder(), avro:in()) -> binary().
encode(RegId, Input) when ?IS_REGID(RegId) ->
  encode(get_encoder(RegId), Input);
encode(Encoder, Input) when is_function(Encoder) ->
  iolist_to_binary(Encoder(Input)).

%%%_* gen_server callbacks =====================================================

init(_) ->
  ets:new(?CACHE, [named_table, protected, {read_concurrency, true}]),
  {ok, #{ schema_registry_url := URL
        }} = application:get_env(?APPLICATION, ?MODULE),
  {ok, #{schema_registry_url => URL}}.

handle_info(Info, State) ->
  error_logger:error_msg("Unknown info: ~p", [Info]),
  {noreply, State}.

handle_cast(Cast, State) ->
  error_logger:error_msg("Unknown cast: ~p", [Cast]),
  {noreply, State}.

handle_call(stop, _From, State) ->
  {stop, normal, State};
handle_call({register, Subject, JSON}, _From,
            #{schema_registry_url := URL} = State) ->
  Result = register_schema(URL, Subject, JSON),
  {reply, Result, State};
handle_call({download, RegId}, _From, State) ->
  Result = handle_download(RegId, State),
  {reply, Result, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

%%%_* Internals ================================================================

download(RegId) ->
  gen_server:call(?SERVER, {download, RegId}, infinity).

handle_download(RegId, #{schema_registry_url := URL}) ->
  case do_download(URL, RegId) of
    {ok, JSON} ->
      Schema = decode_and_insert_cache(RegId, JSON),
      {ok, Schema};
    Error ->
      Error
  end.

decode_and_insert_cache(RegId, JSON) ->
  AvroType = avro:decode_schema(JSON),
  Lkup = avro:make_lkup_fun(AvroType),
  Schema = avro:expand_type_bloated(AvroType, Lkup),
  ok = insert_cache(RegId, Schema),
  Schema.

insert_cache(RegId, Schema) ->
  ets:insert(?CACHE, {RegId, Schema}),
  ok.

lookup_cache(RegId) ->
  case ets:lookup(?CACHE, RegId) of
    [] -> false;
    [{RegId, Schema}] -> {ok, Schema}
  end.

-spec do_download(string(), regid()) -> {ok, binary()} | {error, any()}.
do_download(RegistryURL, RegId) ->
  URL = RegistryURL ++ "/schemas/ids/" ++ integer_to_list(RegId),
  case httpc:request(get, {URL, _Headers = []},
                     [{timeout, ?HTTPC_TIMEOUT}], []) of
    {ok, {{_, 200, "OK"}, _RspHeaders, RspBody}} ->
      #{<<"schema">> := SchemaJSON} =
        jsone:decode(iolist_to_binary(RspBody)),
      {ok, SchemaJSON};
    Other ->
      error_logger:error_msg("Failed to download schema from from ~s:\n~p",
                             [URL, Other]),
      Other
  end.

%% Equivalent cURL command:
%% curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
%%      --data '{"schema": "{\"type\": \"string\"}"}' \
%%      http://localhost:8081/subjects/com.klarna.kred.kevent.name/versions
%% @end
-spec register_schema(string(), string(), binary()) ->
        {ok, regid()} | {error, any()}.
register_schema(RegistryURL, Subject, SchemaJSON) ->
  URL = RegistryURL ++ "/subjects/" ++ Subject ++ "/versions",
  Headers = [],
  Body = make_schema_reg_req_body(SchemaJSON),
  Req = {URL, Headers, "application/vnd.schemaregistry.v1+json", Body},
  Result = httpc:request(post, Req, [{timeout, ?HTTPC_TIMEOUT}], []),
  case Result of
    {ok, {{_, 200, "OK"}, _RspHeaders, RspBody}} ->
      #{<<"id">> := Id} = jsone:decode(iolist_to_binary(RspBody)),
      {ok, Id};
    {error, Reason} ->
      error_logger:error_msg("Failed to register schema to ~s:\n~p",
                             [URL, Reason]),
      {error, Reason}
  end.

%% Make schema registry POST request body.
%% which is: the schema JSON is escaped and wrapped by another JSON object.
-spec make_schema_reg_req_body(binary()) -> binary().
make_schema_reg_req_body(SchemaJSON) ->
  jsone:encode(#{<<"schema">> => SchemaJSON}).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
