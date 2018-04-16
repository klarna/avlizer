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

-module(avlizer).

%% application callbacks
-export([ start/2
        , stop/1
        ]).

start(_StartType, _StartArgs) ->
  case osenv("AVLIZER_CONFLUENT_SCHEMAREGISTRY_URL", false) of
    false -> ok;
    URL ->
      Vars0 = application:get_env(?APPLICATION, avlizer_confluent, #{}),
      Vars = Vars0#{schema_registry_url => URL},
      application:set_env(?APPLICATION, avlizer_confluent, Vars)
  end,
  avlizer_sup:start_link().

stop(_State) -> ok.

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
