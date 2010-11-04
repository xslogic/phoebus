%% -------------------------------------------------------------------
%%
%% Phoebus: A distributed framework for large scale graph processing.
%%
%% Copyright (c) 2010 Arun Suresh. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY 
%% KIND, either express or implied.  See the License for the 
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(external_store).
-author('Arun Suresh <arun.suresh@gmail.com>').

-export([behaviour_info/1]).
-include("phoebus.hrl").
%% API
-export([init/1,
         partition_input/1,
         read_vertices/1,
         read_vertices/2,
         store_vertices/2,
         destroy/1,
         check_dir/1
        ]).

behaviour_info(callbacks) ->
  [{init, 1},
   {partition_input, 1},
   {read_vertices, 2},
   {store_vertices, 2},
   {destroy, 1}];
behaviour_info(_Other) ->
  undefined.

%%%===================================================================
%%% API
%%%===================================================================
init(URI) ->  
  RegisteredStores = 
    phoebus_utils:get_env(registered_stores, ["file", "hdfs"]),
  {StoreMod, State} =
    lists:foldl(
      fun(_, {SMod, _} = ModInfo) when is_atom(SMod) -> ModInfo;
         (SType, {0, _} = Acc) ->
          Mod = list_to_atom("external_store_" ++ SType),
          case Mod:init(URI) of
            {true, SState} -> {Mod, [{store_module, Mod}|SState]};
            _ -> Acc
          end
      end, {0, []}, RegisteredStores),
  case StoreMod of
    0 -> 
      ?ERROR("No registered store for URI", [{uri, URI}]),
      {error, enostores};
    _ -> {ok, State}
  end.

partition_input(StoreState) ->
  Mod = proplists:get_value(store_module, StoreState),
  Mod:partition_input(StoreState).

read_vertices(StoreState) ->
  read_vertices(StoreState, self()).

read_vertices(StoreState, Recvr) ->
  Mod = proplists:get_value(store_module, StoreState),
  Mod:read_vertices(StoreState, Recvr).

store_vertices(StoreState, Vertices) ->
  Mod = proplists:get_value(store_module, StoreState),
  Mod:store_vertices(StoreState, Vertices).

destroy(StoreState) ->
  Mod = proplists:get_value(store_module, StoreState),
  Mod:destroy(StoreState).
  

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
check_dir(URI) ->
  case lists:last(URI) of
    $/ -> true;
    _ -> false
  end.
