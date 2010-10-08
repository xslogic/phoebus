%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created :  4 Oct 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(phoebus_rw).
-include("phoebus.hrl").

%% API
-export([init/1, partition_input/1, 
         read_vertices_start/1, 
         store_vertices/2,
         destroy/1]).

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

store_vertices(State, Vertices) ->
  case proplists:get_value(is_dir, State) of
    false ->
      {FD, NewState} = 
        case proplists:get_value(open_file_ref, State) of
          undefined ->
            {ok, F} = 
              file:open(proplists:get_value(abs_path, State), [write]),
            {F, [{open_file_ref, F}|State]};
          F -> {F, State}
        end,
      lists:foreach(
        fun(V) -> file:write(FD, serialize(V)) end, Vertices),
      NewState;
    _ ->
      {error, State}
  end.  

destroy(State) ->
  case proplists:get_value(open_file_ref, State) of
    undefined -> ok;
    FD -> file:close(FD)
  end.
 
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
  RPid = spawn(fun() -> reader_loop({init, File}, MyPid, {State, []}) end),
  {ok, RPid, State}.


reader_loop({init, File}, Pid, State) ->
  {ok, FD} = file:open(File, [raw, read_ahead]),
  reader_loop(FD, Pid, State);
reader_loop(FD, Pid, {State, Buffer}) ->
  case file:read_line(FD) of
    {ok, Line} ->
      case deserialize(Line) of
        nil -> reader_loop(FD, Pid, {State, Buffer});
        V -> 
          case length(Buffer) > 100 of
            true ->
              gen_fsm:send_event(
                Pid, {vertices, [V|Buffer], self(), State}),
              reader_loop(FD, Pid, {State, []});
            _ ->
              reader_loop(FD, Pid, {State, [V|Buffer]})
          end
      end;
    eof ->
      gen_fsm:send_event(Pid, {vertices_done, Buffer, self(), State}),
      file:close(FD)
  end.
      

serialize(V) ->
  worker_store:serialize_rec(vertex, V).
  
%% {Vid, VName, VVal, VState, [{EVal, VName}]
%% vname \t vval \t [eval \t tvname \t].. \n
deserialize(Line) ->
  deserialize(Line, #vertex{}, [], [], vname).

deserialize([$\n | _], #vertex{vertex_id = nil}, _, _, _) -> nil;
deserialize([$\n | _], V, EList, _, _) ->
  {V#vertex.vertex_name, V#vertex.vertex_value, EList};
deserialize([$\t | Rest], V, EList, Buffer, vname) ->
  VName = lists:reverse(Buffer),
  VId = erlang:phash2(VName, 4294967296),
  deserialize(Rest, V#vertex{vertex_id = VId, 
                                vertex_name = VName}, EList, [], vval);
deserialize([$\t | Rest], V, EList, Buffer, vval) ->
  deserialize(Rest, V#vertex{vertex_value = lists:reverse(Buffer)}, 
                 EList, [], eval);
deserialize([$\t | Rest], V, EList, Buffer, eval) ->
  deserialize(Rest, V, EList, [], {tvname, lists:reverse(Buffer)});
deserialize([$\t | Rest], V, EList, Buffer, {tvname, EVal}) ->
  VName = lists:reverse(Buffer),
  deserialize(Rest, V, [{EVal, VName}|EList], [], eval);
deserialize([X | Rest], V, EList, Buffer, Token) ->
  deserialize(Rest, V, EList, [X|Buffer], Token).

