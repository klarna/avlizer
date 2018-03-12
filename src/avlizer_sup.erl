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

-module(avlizer_sup).
-behaviour(supervisor).

-export([ start_link/0
        , init/1
        ]).

-define(SUP, ?MODULE).

start_link() ->
  supervisor:start_link({local, ?SUP}, ?MODULE, []).

init(_) ->
  Children = [avlizer_confluent()],
  {ok, {{one_for_one, 5, 10}, Children}}.

%%%_* Internals ================================================================

avlizer_confluent() ->
  { _Id       = avlizer_confluent
  , _Start    = {avlizer_confluent, start_link, []}
  , _Restart  = permanent
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [avlizer_confluent]
  }.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
