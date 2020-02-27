%%%-----------------------------------------------------------------------------
%%%
%%% Copyright (c) 2018-2019 Klarna Bank AB (publ).
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

-module(avlizer).

%% application callbacks
-export([ start/2
        , stop/1
        ]).

start(_StartType, _StartArgs) ->
  set_envs(),
  avlizer_sup:start_link().

stop(_State) -> ok.

set_envs() ->
  Vars0 = application:get_env(?APPLICATION, avlizer_confluent, #{}),
  Vars1 = maybe_put_schema_registry_url(Vars0),
  Vars2 = maybe_put_schema_registry_sasl(Vars1),
  application:set_env(?APPLICATION, avlizer_confluent, Vars2).

maybe_put_schema_registry_url(Map) ->
  SchemaRegistryURL = osenv("AVLIZER_CONFLUENT_SCHEMAREGISTRY_URL", false),
  case SchemaRegistryURL of
    false -> Map;
    URL -> maps:put(schema_registry_url, URL, Map)
  end.

maybe_put_schema_registry_sasl(Map) ->
  SchemaRegistrySASLMechanism = osenv("AVLIZER_CONFLUENT_SCHEMAREGISTRY_SASL_MECHANISM", false),
  SchemaRegistrySASLUsername = osenv("AVLIZER_CONFLUENT_SCHEMAREGISTRY_SASL_USERNAME", false),
  SchemaRegistrySASLPassword = osenv("AVLIZER_CONFLUENT_SCHEMAREGISTRY_SASL_PASSWORD", false),

  SASL = {maybe_string_to_atom(SchemaRegistrySASLMechanism), SchemaRegistrySASLUsername, SchemaRegistrySASLPassword},

  Map0 = case lists:member(false, tuple_to_list(SASL)) of
    true -> Map;
    false -> maps:put(schema_registry_sasl, SASL, Map)
  end,
  
  Map1 = maybe_read_sasl_file(Map0),

  maybe_closure_sasl_password(Map1).

maybe_string_to_atom(false) -> false;
maybe_string_to_atom(Env) -> list_to_atom(Env).

maybe_read_sasl_file(#{schema_registry_sasl := {Mechanism, File}} = Vars) ->
  case read_file(File, 2) of
    [Username, Password] -> 
      maps:put(schema_registry_sasl, {Mechanism, Username, Password}, Vars);
    _Any -> 
      error_logger:error_msg("Malformed SASL file ~s", [File]),
      Vars
  end;
maybe_read_sasl_file(Vars) -> Vars.

read_file(File, NumberOfLines) -> 
  {ok, Device} = file:open(File, [raw, read, read_ahead]),
  Lines = read_lines([], Device, NumberOfLines),
  file:close(Device),
  Lines.

read_lines(Lines, _Device, 0) -> lists:reverse(Lines);
read_lines(Lines, Device, LineNumber) ->
  case file:read_line(Device) of
    {ok, Line} ->
      StrippedLine = string:strip(Line, right, $\n),
      read_lines([StrippedLine|Lines], Device, LineNumber - 1);
    eof -> 
      read_lines(Lines, Device, 0)
  end.

maybe_closure_sasl_password(#{schema_registry_sasl := {Mechanism, Username, Password}} = Vars) -> 
  maps:put(schema_registry_sasl, {Mechanism, Username, fun() -> Password end}, Vars);
maybe_closure_sasl_password(Vars) -> Vars.

osenv(Name, Default) ->
  case os:getenv(Name) of
    "" -> Default; %% VAR=""
    false -> Default; %% not set
    Val -> Val
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
