%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created : 22 Sep 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(worker_store).
-include("phoebus.hrl").

%% API
-export([init/2, store_vertex/4, 
         init_step_file/4, init_step_file/5, 
         close_step_file/1, 
         commit_step/3]).

%%%===================================================================
%%% API
%%%===================================================================
init(JobId, WId) ->
  ok = mkdir_p(?JOB_DIR(JobId, WId)),
  case file:open(?LAST_STEP_FILE(JobId, WId), [read]) of
    {error, enoent} -> {last_step, -1};
    {ok, F} -> 
      case file:read_line(F) of
        {ok, StrStep} when is_list(StrStep) -> 
          file:close(F),
          {last_step, list_to_integer(StrStep)};
        _ ->
          file:close(F),
          {last_step, -1}
      end
  end.

init_step_file(JobId, WId, Mode, Step) ->
  init_step_file(JobId, WId, Mode, Step, Step).

init_step_file(JobId, WId, Mode, Step, Idx) -> 
  ok = mkdir_p(?STEP_DIR(JobId, WId, Step)),
  file:open(?STEP_VETEX_DATA(JobId, WId, Step, Idx), Mode).

init_rstep_file(JobId, WId, RNode, RWId, Mode, Step) ->
  init_rstep_file(JobId, WId, RNode, RWId, Mode, Step, Step).

init_rstep_file(JobId, WId, RNode, RWId, Mode, Step, Idx) -> 
  ok = mkdir_p(?RSTEP_DIR(JobId, WId, Step, RNode, RWId)),
  file:open(?RSTEP_VETEX_DATA(JobId, WId, Step, RNode, RWId, Idx), Mode).
  
close_step_file(FD) ->
  file:close(FD).

commit_step(JobId, WId, Step) ->
  {ok, FD} = file:open(?LAST_STEP_FILE(JobId, WId), [write]),
  file:write(FD, integer_to_list(Step)),
  file:close(FD).

store_vertex(Vertex, {Node, {JobId, MyWId, WId}}, Step, FDs) ->
  {ok, FD} = 
    case WId of
      MyWId -> 
        case proplists:get_value(MyWId, FDs) of
          undefined -> init_step_file(JobId, MyWId, [write], Step);
          OldFD -> {ok, OldFD}
        end;
      _ ->
        case proplists:get_value(WId, FDs) of
          undefined -> 
            init_rstep_file(JobId, MyWId, Node, WId, [write], Step);
          OldFD -> {ok, OldFD}
        end
    end,
  VRec = construct_rec(Vertex),
  file:write(FD, VRec),
  TempFDs = lists:keydelete(WId, 1, FDs),
  [{WId, FD}|TempFDs].
  

%%--------------------------------------------------------------------
%% @doc`
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
construct_rec(#vertex{vertex_id = VId, vertex_state = VState, 
                      vertex_value = VVal, edge_list = EList}) ->
  lists:concat([integer_to_list(VId), "\t",
                atom_to_list(VState), "\t",
                VVal, "\t", construct_edge_rec(EList, [])]).

construct_edge_rec([], Done) ->
  Done ++ "\n";
construct_edge_rec([#edge{value = EVal, target_vid = VId}|Rest], Done) ->
  construct_edge_rec(Rest, 
                     lists:concat([EVal, "\t", 
                                   integer_to_list(VId), "\t", Done])).
  
mkdir_p(Loc) ->
  {_, FinalRetVal} = 
    lists:foldl(
      fun([], X) -> X; 
         (DirName, {Finished, _}) -> 
          Next = 
            case Finished of 
              "/" -> "/" ++ DirName; 
              _ -> Finished ++ "/" ++ DirName 
            end, 
          R = file:make_dir(Next), {Next, R} 
      end, {"/", init}, re:split(Loc, "/", [{return, list}])),
  case FinalRetVal of
    ok -> ok;
    {error, eexist} -> ok;
    E ->
      ?DEBUG("Could not Create Directory...", [{location, Loc}, {error, E}]),
      {error, E}
  end.
