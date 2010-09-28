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
-export([purge/0, init/2, store_vertex/4, 
         init_step_file/5, init_step_file/6, 
         sync_table/3, close_step_file/1, 
         transfer_files/3,
         create_receiver/6,
         deserialize_rec/1,
         table_name/2,
         commit_step/3]).

%%%===================================================================
%%% API
%%%===================================================================
purge() ->
  os:cmd("rm -rf " ++ ?BASE_DIR()).  

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

init_step_file(Type, JobId, WId, Mode, Step) ->
  init_step_file(Type, JobId, WId, Mode, Step, Step).

init_step_file(Type, JobId, WId, _Mode, Step, Idx) -> 
  ok = mkdir_p(?STEP_DIR(JobId, WId, Step)),
  [{_, Table}] = ets:lookup(table_mapping, {JobId, WId}),
  TableName = table_name(Table, Type),
  ets:insert(worker_registry, {{TableName, sync}, 0}), 
  case dets:info(TableName) of
    undefined -> 
      ?DEBUG("Opening Table...", [{job, JobId}, {worker, WId}, {step, Step}]),
      dets:open_file(TableName, 
                     [{file, step_data(Type, JobId, WId, Step, Idx)}]);
    _ -> {ok, TableName}
  end.
        

init_rstep_file(Type, JobId, WId, RNode, RWId, Mode, Step) ->
  init_rstep_file(Type, JobId, WId, RNode, RWId, Mode, Step, Step).

init_rstep_file(Type, JobId, WId, RNode, RWId, Mode, Step, Idx) -> 
  ok = mkdir_p(?RSTEP_DIR(JobId, WId, Step, RNode, RWId)),
  file:open(rstep_data(Type, JobId, WId, Step, RNode, RWId, Idx), Mode).

sync_table(Table, Type, IsForce) ->
  TName = table_name(Table, Type),
  [{_, LastSync}] = ets:lookup(worker_registry, {TName, sync}), 
  CurrSize = dets:info(TName, size),
  case (((CurrSize - LastSync) > 1000) or IsForce) of
    true -> 
      ets:insert(worker_registry, {{TName, sync}, CurrSize}),
      FileName = dets:info(TName, filename),
      dets:close(TName),
      dets:open_file(TName, {file, FileName});
    _ -> ok
  end.      

close_step_file({_, []}) -> void;
close_step_file({Table, Buffer}) -> 
  %% dets:safe_fixtable(Table, true),
  table_insert(Table, Buffer);
  %% dets:sync(Table),
  %% dets:safe_fixtable(Table, false);
close_step_file(FD) ->
  file:close(FD).

commit_step(JobId, WId, Step) ->
  {ok, FD} = file:open(?LAST_STEP_FILE(JobId, WId), [write]),
  file:write(FD, integer_to_list(Step)),
  file:close(FD).

store_vertex_helper(Table, Buffer, Vertex, WId, FDs) ->
  NewBuffer = 
    case length(Buffer) > 9 of
      true -> 
        table_insert(Table, [Vertex|Buffer]), 
        [];
      _ -> [Vertex|Buffer]
    end,
  TempFDs = lists:keydelete(WId, 1, FDs),
  [{WId, {Table, NewBuffer}}|TempFDs].  

store_vertex(Vertex, {_, {JobId, MyWId, WId}}, Step, FDs) when WId =:= MyWId->
  {Table, Buffer} = 
    case proplists:get_value(MyWId, FDs) of
      undefined -> 
        {ok, T} = init_step_file(vertex, JobId, MyWId, [write], Step),
        {T, []};
      OldTD -> OldTD
    end,
  store_vertex_helper(Table, Buffer, Vertex, WId, FDs);
store_vertex(Vertex, {Node, {JobId, MyWId, WId}}, Step, FDs) ->
  case erlang:node() of
    Node ->
      {Table, Buffer} = 
        case proplists:get_value(WId, FDs) of
          undefined -> {get_other_worker_table(JobId, WId, vertex), []};
          TD -> TD
        end,
      store_vertex_helper(Table, Buffer, Vertex, WId, FDs);
    _ ->        
      VRec = serialize_rec(Vertex),
      {ok, FD} = 
        case proplists:get_value(WId, FDs) of
          undefined -> 
            init_rstep_file(vertex, JobId, MyWId, Node, WId, [write], Step);
          OldFD -> {ok, OldFD}
        end,
      file:write(FD, VRec),
      TempFDs = lists:keydelete(WId, 1, FDs),
      [{WId, FD}|TempFDs]
  end.



transfer_files(Node, {JobId, WId, OWid}, Step) ->  
  MyPid = self(),
  spawn(fun() -> transfer_loop(init, MyPid, Node, 
                               {JobId, WId, OWid}, Step) end).

create_receiver(Type, JobId, WId, Mode, Step, Idx) ->
  spawn(fun() ->
            recv_loop({init, Type}, JobId, WId, Mode, Step, Idx)
        end).

%%--------------------------------------------------------------------
%% @doc`
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
transfer_loop(init, _MyPid, Node, {JobId, WId, OWid}, Step) ->
  Dir = ?RSTEP_DIR(JobId, WId, Step, Node, OWid),
  case file:list_dir(Dir) of
    {ok, FList} ->
      lists:foreach(
        fun(FName) ->
            handle_transfer(file_type(FName), Node, Dir ++ FName, 
                            JobId, OWid, Step, WId)
        end, FList);
    _ -> void
  end.


handle_transfer(Type, Node, LocalFName, JobId, OWid, Step, WId) ->
  WriteFD = 
    rpc:call(Node, worker_store, create_receiver,
             [Type, JobId, OWid, [write, binary], Step, WId]),
  {ok, ReadFD} =
    file:open(LocalFName, [read, binary]),
  trans_loop(LocalFName, ReadFD, WriteFD).


trans_loop(LocalFName, ReadFD, WriteFD) ->
  %% io:format("~n ~n [~p] [~p] ~n ~n", 
  %% [self(), [LocalFName, ReadFD, WriteFD]]),
  case file:read(ReadFD, 16384) of
    {ok, Data} ->
      WriteFD ! {data, Data},
      trans_loop(LocalFName, ReadFD, WriteFD);
    eof ->
      file:close(ReadFD),
      file:delete(LocalFName),
      MRef = erlang:monitor(process, WriteFD),
      WriteFD ! {close, self()},
      receive
        {done, WriteFD} -> done;
        {'DOWN', MRef, _, _, _} -> done 
      end
  end.
  

recv_loop({init, Type}, JobId, WId, Mode, Step, Idx) ->
  Table = get_other_worker_table(JobId, WId, Type),
  %% {ok, FD} = init_step_file(Type, JobId, WId, Mode, Step, Idx),
  recv_loop({Table, <<>>, 0}, JobId, WId, Mode, Step, Idx);
recv_loop({WriteFD, Buffer, RCount}, JobId, WId, Mode, Step, Idx) ->
  receive
    {data, Data} -> 
      BinLines = re:split(Data, "\n"),
      %% NOTE : Extracted Recs must be [{vId, VRec}]
      {VRecs, Rem} = extract_records(Buffer, BinLines),
      table_insert(WriteFD, VRecs), 
      NewRCount = RCount + length(VRecs),
      case RCount > 750 of
        true -> 
          [{_, WPid}] = ets:lookup(worker_registry, {JobId, WId}),
          gen_fsm:send_all_state_event(WPid, sync_table);
        _ ->
          void
      end,
      recv_loop({WriteFD, Rem, NewRCount}, JobId, WId, Mode, Step, Idx);
    {close, WPid} -> WPid ! {done, self()}
      %% file:close(WriteFD)
  end.


wait_table_loop(_Type, _JobId, _OWid, Pid, 0) -> Pid ! {error, enoent};
wait_table_loop(Type, JobId, OWid, Pid, Counter) ->
  ?DEBUG("Checking for table...", 
         [{job, JobId}, {worker, OWid}, {counter, Counter}]),
  case ets:lookup(table_mapping, {JobId, OWid}) of
    [{_, Table}] ->
      case dets:info(table_name(Table, Type)) of
        undefined ->
          ?DEBUG("Table unopen... waiting..", 
                 [{job, JobId}, {worker, OWid}, {counter, Counter}]),
          timer:sleep(10000), 
          wait_table_loop(Type, JobId, OWid, Pid, Counter - 1);
        _ ->Pid ! {table, Table}
      end;
    _ -> 
      ?DEBUG("Table NOT acquired... waiting..", 
             [{job, JobId}, {worker, OWid}, {counter, Counter}]),
      timer:sleep(10000), 
      wait_table_loop(Type, JobId, OWid, Pid, Counter - 1)
  end.


get_other_worker_table(JobId, OWId, Type) ->
  Pid = self(),
  spawn(fun() -> wait_table_loop(Type, JobId, OWId, Pid, 30) end),
  Table = 
    receive
      {table, T} -> T;
      %% TODO : have to think of something...
      {error, enoent} -> backup_table
    end,
  table_name(Table, Type).

extract_records(Buffer, BinLines) ->
  lists:foldl(
    fun(BinLine, {AccRecs, Buff}) ->
        L = <<Buff/binary, BinLine/binary>>,
        case deserialize_rec(L) of
          null -> {AccRecs, <<>>};
          incomplete -> {AccRecs, L};
          Rec -> {[Rec|AccRecs], <<>>}
        end
    end, {[], Buffer}, BinLines).
  

deserialize_rec(<<>>) -> null;
deserialize_rec(Line) ->
  BSize = size(Line) - 1,
  <<_:BSize/binary, Last/binary>> = Line,
  case Last of
    <<$\r>> -> deserialize_rec(Line, #vertex{}, [], <<>>, vname);
    _ -> incomplete
  end.
  
%% vid \t vname \t vstate \t vval \t [eval \t tvid\t tvname \t].. \r\n
deserialize_rec(<<$\r, _/binary>>, V, EList, _, _) ->
  {V#vertex.vertex_name, V#vertex.vertex_value,
   V#vertex.vertex_state, EList};
%% deserialize_rec(<<$\t, Rest/binary>>, V, EList, Buffer, vid) ->
%%   VId = list_to_integer(binary_to_list(Buffer)),
%%   deserialize_rec(Rest, V#vertex{vertex_id = VId}, EList, <<>>, vname);
deserialize_rec(<<$\t, Rest/binary>>, V, EList, Buffer, vname) ->
  VName = binary_to_list(Buffer),
  deserialize_rec(Rest, V#vertex{vertex_name = VName}, EList, <<>>, vstate);
deserialize_rec(<<$\t, Rest/binary>>, V, EList, Buffer, vstate) ->
  VState = list_to_atom(binary_to_list(Buffer)),
  deserialize_rec(Rest, V#vertex{vertex_state = VState}, EList, <<>>, vval);
deserialize_rec(<<$\t, Rest/binary>>, V, EList, Buffer, vval) ->
  VVal = binary_to_list(Buffer),
  deserialize_rec(Rest, V#vertex{vertex_value = VVal}, EList, <<>>, eval);
deserialize_rec(<<$\t, Rest/binary>>, V, EList, Buffer, eval) ->
  deserialize_rec(Rest, V, EList, <<>>, {tvname, binary_to_list(Buffer)});
deserialize_rec(<<$\t, Rest/binary>>, V, EList, Buffer, {tvname, EVal}) ->
  deserialize_rec(Rest, V, [{EVal, binary_to_list(Buffer)}|EList], <<>>, 
                  eval);
deserialize_rec(<<X, Rest/binary>>, V, EList, Buffer, Token) ->
  deserialize_rec(Rest, V, EList, <<Buffer/binary, X>>, Token).  





serialize_rec({VName, VVal, VState, EList}) ->  
  lists:concat([VName, "\t",
                atom_to_list(VState), "\t",
                VVal, "\t", serialize_edge_rec(EList, [])]).

serialize_edge_rec([], Done) ->
  Done ++ "\r\n";
serialize_edge_rec([{EVal, VName}|Rest], Done) ->
  serialize_edge_rec(Rest, 
                     lists:concat([EVal, "\t", VName, "\t", Done])).
  
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


is_vfile(F) -> (re:run(F, ".*vertex_data", [anchored]) =/= nomatch).
is_qfile(F) -> (re:run(F, ".*msg_queue", [anchored]) =/= nomatch).

file_type(F) -> 
  case is_vfile(F) of
    true -> vertex;
    _ -> case is_qfile(F) of
           true -> msg;
           _ -> unknown
         end
  end.

step_data(vertex, JobId, WId, Step, Idx) ->
        ?STEP_VETEX_DATA(JobId, WId, Step, Idx);
step_data(msg, JobId, WId, Step, Idx) ->
        ?STEP_MSG_QUEUE(JobId, WId, Step, Idx).

rstep_data(vertex, JobId, WId, Step, RNode, RWId, Idx) ->
        ?RSTEP_VETEX_DATA(JobId, WId, Step, RNode, RWId, Idx);
rstep_data(msg, JobId, WId, Step, RNode, RWId, Idx) ->
        ?RSTEP_MSG_QUEUE(JobId, WId, Step, RNode, RWId, Idx).


table_name(Table, Type) ->
  list_to_atom(atom_to_list(Table) ++ "_" ++ atom_to_list(Type)).

table_insert(Table, X) ->
  try
    dets:insert(Table, X)
  catch
    E1:E2 ->
      ?DEBUG("Error while inserting into table..", 
             [{table, Table}, {error, E1, E2}]),
      table_insert(Table, X, 120)
  end.

table_insert(Table, X, Counter) ->
  case dets:info(Table) of
    undefined ->
      timer:sleep(1000),
      table_insert(Table, X, Counter - 1);
    _ ->
      table_insert(Table, X)
  end.
      
