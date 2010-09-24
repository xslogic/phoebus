%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created : 23 Sep 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(phoebus_source).
-include("phoebus.hrl").

%% API
-export([init/1, partition_input/1, read_vertices_start/1, destroy/1]).

%%%===================================================================
%%% API
%%%===================================================================
init(URI) ->
  case URI of
    [$h, $d, $f, $s, $:, $/, $/ | AbsPath] -> 
      {ok, check_dir(URI, [{uri, URI}, {abs_path, AbsPath}, {type, dets}])};
    [$f, $i, $l, $e, $:, $/, $/ | AbsPath] -> 
      {ok, check_dir(URI, [{uri, URI}, {abs_path, AbsPath}, {type, file}])}
  end.

partition_input(State) ->
  case proplists:get_value(is_dir, State) of
    true ->
      {ok, Files} = file:list_dir(proplists:get_value(abs_path, State)),
      Base = proplists:get_value(uri, State),
      {ok, [Base ++ F || F <- Files], State};
    _ ->
      {error, State}
  end.
             
read_vertices_start(State) ->
  case proplists:get_value(is_dir, State) of
    false ->
      start_reading(proplists:get_value(type, State), 
                    proplists:get_value(abs_path, State), State);
    _ ->
      {error, State}
  end.  

destroy(_) ->
  void.
 
%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
check_dir(URI, Conf) ->
  case lists:last(URI) of
    $/ -> [{is_dir, true} | Conf];
    _ -> [{is_dir, false} | Conf]
  end.
      
start_reading(file, File, State) ->
  MyPid = self(),
  RPid = spawn(fun() -> reader_loop({init, File}, MyPid, State) end),
  {ok, RPid, State}.


reader_loop({init, File}, Pid, State) ->
  {ok, FD} = file:open(File, [read, {read_ahead, 4096}]),
  reader_loop(FD, Pid, State);
reader_loop(FD, Pid, State) ->
  {Recs, IsDone} = 
    lists:foldl(
      fun(_, {Records, true}) -> {Records, true};
         (_, {Records, X}) ->
          case file:read_line(FD) of
            {ok, Line} -> 
              case convert_to_rec(Line) of
                #vertex{vertex_id = nil} -> {Records, X};
                V -> {[V|Records], X}
              end;
            eof -> 
              file:close(FD),
              {Records, true}
          end
      end, {[], false}, lists:seq(1, 10)),
  case IsDone of
    true -> Pid ! {vertices_done, Recs, self(), State};
    _ -> Pid ! {vertices, Recs, self(), State},
         reader_loop(FD, Pid, State)
  end.
      
  
%% vname \t vval \t [eval \t tvname \t].. \n
convert_to_rec(Line) ->
  convert_to_rec(Line, #vertex{}, [], [], vname).

convert_to_rec([$\n | _], V, EList, _, _) ->
  V#vertex{edge_list = EList};
convert_to_rec([$\t | Rest], V, EList, Buffer, vname) ->
  VName = lists:reverse(Buffer),
  VId = erlang:phash2(VName),
  convert_to_rec(Rest, V#vertex{vertex_id = VId}, EList, [], vval);
convert_to_rec([$\t | Rest], V, EList, Buffer, vval) ->
  convert_to_rec(Rest, V#vertex{vertex_value = lists:reverse(Buffer)}, 
                 EList, [], eval);
convert_to_rec([$\t | Rest], V, EList, Buffer, eval) ->
  convert_to_rec(Rest, V, EList, [], {tvname, lists:reverse(Buffer)});
convert_to_rec([$\t | Rest], V, EList, Buffer, {tvname, EVal}) ->
  VName = lists:reverse(Buffer),
  VId = erlang:phash2(VName),
  convert_to_rec(Rest, V, [#edge{value = EVal, target_vid = VId}|EList], 
                 [], eval);
convert_to_rec([X | Rest], V, EList, Buffer, Token) ->
  convert_to_rec(Rest, V, EList, [X|Buffer], Token).
