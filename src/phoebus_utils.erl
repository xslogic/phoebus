%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created : 21 Sep 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(phoebus_utils).
-include("phoebus.hrl").
%% API
-export([vertex_owner/3, all_nodes/0, map_to_node/2, job_id/0, get_env/2]).

%%%===================================================================
%%% API
%%%===================================================================

%% Returns {Node, {JobId, WorkerId}}
vertex_owner(JobId, VId, NumWorkers) ->  
  WorkerId = (erlang:phash2(VId) rem NumWorkers) + 1,  
  {map_to_node(JobId, WorkerId), WorkerId}.    

%% TODO : This has to be configurable...
map_to_node(_JobId, WorkerId) -> 
  AllNodes = all_nodes(),
  NodeIdx = ((WorkerId - 1) rem length(AllNodes)) + 1,
  lists:nth(NodeIdx, AllNodes).
  

%% Returns [Nodes]
all_nodes() ->
  %% TODO : implement
  ['phoebus1@needplant-lm', 'phoebus2@needplant-lm'].
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
      
