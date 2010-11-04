%% -------------------------------------------------------------------
%%
%% Phoebus: A distributed framework for large scale graph processing.
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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
-module(phoebus_utils).
-author('Arun Suresh <arun.suresh@gmail.com>').
-include("phoebus.hrl").
%% API
-export([vertex_owner/3, all_nodes/0, map_to_node/3, 
         map_to_node/2, job_id/0, get_env/2]).

%%%===================================================================
%%% API
%%%===================================================================

%% Returns {Node, {JobId, WorkerId}}
vertex_owner(JobId, VId, NumWorkers) ->  
  WorkerId = (erlang:phash2(VId) rem NumWorkers) + 1,  
  {map_to_node(JobId, WorkerId), WorkerId}.    

%% TODO : This has to be configurable...
map_to_node(_JobId, WorkerId, AllNodes) -> 
  NodeIdx = ((WorkerId - 1) rem length(AllNodes)) + 1,
  lists:nth(NodeIdx, AllNodes).

map_to_node(JobId, WorkerId) -> 
  [{JobId, AllNodes}] = ets:lookup(all_nodes, JobId),
  NodeIdx = ((WorkerId - 1) rem length(AllNodes)) + 1,
  lists:nth(NodeIdx, AllNodes).
  

all_nodes() ->
  NList = net_adm:world_list([list_to_atom(net_adm:localhost())]),
  lists:sort(NList).
  %% TODO : implement
  %% ['phoebus1@needplant-lm', 'phoebus2@needplant-lm'].
  %% [erlang:node()].

%% Returns "nodename_timestamp"
job_id() ->
  {Me, S, Mi} = erlang:now(),
  atom_to_list(erlang:node()) ++ "_" ++ 
    integer_to_list((Me * 1000000) + (S * 1000) + Mi).

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_env(Key, Def) ->
  case application:get_env(Key) of
    {ok, Val} -> Val;
    _ -> Def
  end.
      
