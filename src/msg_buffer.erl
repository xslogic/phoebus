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
-module(msg_buffer).
-author('Arun Suresh <arun.suresh@gmail.com>').
-behaviour(gen_server).

%% API
-export([start_link/2, start_link/4, insert/2, 
         merge/1, merge_file/4, do_merge/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(FNAME, "out_msgs_"). 
-define(MERGE_FNAME, "msg_queue_"). 
-define(BUFFER_SIZE(), phoebus_utils:get_env(msg_buffer_size, 1000)). 

-record(state, {base_dir, index = 0, records, combine_fun, 
                return_pid, log_file}).

%%%===================================================================
%%% API
%%%===================================================================
insert(MBuffPid, {VName, Msg}) ->
  gen_server:cast(MBuffPid, {insert, VName, Msg}).

merge(MBuffPid) ->
  gen_server:cast(MBuffPid, merge_files).

merge_file(FName, RetPid, CombineFun, Dir) ->
  start_link(FName, RetPid, CombineFun, Dir).
  

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(FName, RetPid, CombineFun, Dir) ->
  gen_server:start_link(?MODULE, [FName, RetPid, CombineFun, Dir], []).

start_link(BaseDir, CombineFun) ->
  gen_server:start_link(?MODULE, [BaseDir, CombineFun], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([FName, RetPid, CombineFun, Dir]) ->
  worker_store:mkdir_p(Dir),
  {ok, FD} = file:open(FName, [raw, read_ahead, binary]),
  {ok, #state{base_dir = Dir, combine_fun = CombineFun, 
              return_pid = RetPid, log_file = {FName, FD},
              records = gb_sets:new()}, 0};
init([BaseDir, CombineFun]) ->
  worker_store:mkdir_p(BaseDir),
  {ok, #state{base_dir = BaseDir, combine_fun = CombineFun, 
              records = gb_sets:new()}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(merge_files, #state{index = I, base_dir = BD, 
                                return_pid = RetPid,
                                combine_fun = C} = State) ->
  flush_buffer(State),
  do_merge(BD, C),
  case RetPid of
    undefined -> void;
    _ -> gen_fsm:send_event(RetPid, {done_merge, self()})
  end,
  {stop, normal, #state{index = I, base_dir = BD}};

handle_cast({insert, VName, Msg}, #state{index = I, records = R} = State) ->
  case (gb_sets:size(R) > ?BUFFER_SIZE()) of
    true -> 
      flush_buffer(State),
      {noreply, State#state{index = I + 1, 
                            records = ins({VName, Msg}, gb_sets:new())}};
    _ -> 
      {noreply, State#state{records = ins({VName, Msg}, R)}}
  end;

handle_cast({records, IsDone, Msgs}, 
            #state{index = I, records = R} = State) ->
  NewState = 
    case (gb_sets:size(R) > ?BUFFER_SIZE()) of
      true -> 
        flush_buffer(State),
        State#state{index = I + 1, records = ins(Msgs, gb_sets:new())};
      _ -> 
        State#state{records = ins(Msgs, R)}
    end,
  case IsDone of
    false -> {noreply, NewState, 0};
    _ -> 
      gen_server:cast(self(), merge_files),
      {noreply, NewState}
  end.
         
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(timeout, #state{log_file = {FName, FD}} = State) ->
  {IsDone, Recs} = read_log_loop(FD, 100, []),
  gen_server:cast(self(), {records, IsDone, Recs}),
  NewFD = case IsDone of true -> done; _ -> FD end,
  {noreply, State#state{log_file = {FName, NewFD}}};

handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
read_log_loop(_, 0, Recs) -> {false, Recs};
read_log_loop(FD, Count, Recs) ->
  case file:read_line(FD) of
    {ok, Data} ->
      R = serde:deserialize_rec(msg, Data),
      read_log_loop(FD, Count - 1, [R|Recs]);
    eof -> 
      file:close(FD),
      {true, Recs}
  end.  

flush_buffer(#state{base_dir = BD, index = I, 
                    combine_fun = C, records = R}) ->
  %% io:format("~n~n HERE 1 ~n~n", []),
  FName = BD ++ ?FNAME ++ integer_to_list(I),
  %% io:format("~n~n HERE 2 [~p] ~n~n", [FName]),
  {ok, FD} = file:open(FName, [write]),
  %% io:format("~n~n HERE 3 [~p] ~n~n", [FD]),
  {LastVName, LVMsgs} =
    lists:foldl(
      fun({VName, Msg}, {start, Buffer}) -> {VName, [Msg|Buffer]};
         ({NewVName, Msg}, {VName, Buffer}) when VName =:= NewVName -> 
          {VName, [Msg|Buffer]};
         ({NewVName, Msg}, {VName, Buffer}) -> 
          file:write(FD, serde:serialize_rec(
                           msg, {VName, apply_combine(C, Buffer)})),
          {NewVName, [Msg]}
      end, {start, []}, gb_sets:to_list(R)),
  case LastVName of
    start -> void;
    _ -> 
      file:write(FD, serde:serialize_rec(
                       msg, {LastVName, apply_combine(C, LVMsgs)}))
  end,
  file:close(FD),
  case (I rem 5) of
    4 -> do_merge(BD, C);
    _ -> void
  end.
      

do_merge(BD, CombineFun) ->
  {ok, FList} = file:list_dir(BD),
  do_merge(0, CombineFun, self(), BD, FList).

wait_on({done, RetLeft}, {done, RetRight}) ->
  {RetLeft, RetRight};
wait_on({_, LPid} = LInfo, {_, RPid} = RInfo) ->
  receive
    {LPid, FinLeft} -> wait_on({done, FinLeft}, RInfo);
    {RPid, FinRight} -> wait_on(LInfo, {done, FinRight})
  end.


do_merge(Idx, CombineFun, RootPid, BD, Lst) ->          
  MyPid = self(),
  RetFName = BD ++ ?MERGE_FNAME ++ integer_to_list(Idx),
  case Lst of
    [SingleFile] -> 
      SrcFName = BD ++ SingleFile,
      os:cmd("mv " ++ SrcFName ++ " " ++ RetFName),
      RootPid ! {MyPid, RetFName};
    Lst ->
      {Left, Right} = lists:split(length(Lst) div 2, Lst),
      LPid = 
        spawn(fun() -> 
                  do_merge((Idx * 2) + 1, CombineFun, MyPid, BD, Left) end),
      RPid = 
        spawn(fun() -> 
                  do_merge((Idx * 2) + 2, CombineFun, MyPid, BD, Right) end),
      {LFile, RFile} = wait_on({wait, LPid}, {wait, RPid}),      
      {ok, WriteFD} = file:open(RetFName, [write, binary]),
      {ok, LReadFD} = file:open(LFile, [raw, read_ahead, binary]),
      {ok, RReadFD} = file:open(RFile, [raw, read_ahead, binary]),
      merge_files(WriteFD, CombineFun, {start, LReadFD}, {start, RReadFD}),
      file:delete(LFile),
      file:delete(RFile),
      RootPid ! {MyPid, RetFName}
  end.

merge_files(WriteFD, CombineFun, {start, LReadFD}, {start, RReadFD}) ->
  {ok, LLine} = file:read_line(LReadFD),
  {ok, RLine} = file:read_line(RReadFD),
  merge_files(WriteFD, CombineFun, {LLine, LReadFD}, {RLine, RReadFD});
merge_files(WriteFD, CombineFun, {LLine, LReadFD}, {start, RReadFD}) ->
  case file:read_line(RReadFD) of
    eof ->
      file:close(RReadFD),
      dump_rest(LLine, LReadFD, WriteFD);
    {ok, RLine} ->
      merge_files(WriteFD, CombineFun, {LLine, LReadFD}, {RLine, RReadFD})
  end;
merge_files(WriteFD, CombineFun, {start, LReadFD}, {RLine, RReadFD}) ->
  case file:read_line(LReadFD) of
    eof ->
      file:close(LReadFD),
      dump_rest(RLine, RReadFD, WriteFD);
    {ok, LLine} ->
      merge_files(WriteFD, CombineFun, {LLine, LReadFD}, {RLine, RReadFD})
  end;
merge_files(WriteFD, CombineFun, {LLine, LReadFD}, {RLine, RReadFD}) ->
  {RVName, RMsgs} = 
    case is_binary(RLine) of
      true -> serde:deserialize_rec(msg, RLine);
      _ -> RLine
    end,
  {LVName, LMsgs} =
    case is_binary(LLine) of
      true -> serde:deserialize_rec(msg, LLine);
      _ -> LLine
    end,
  case RVName of
    LVName -> 
      NMsgs = apply_combine(CombineFun, LMsgs ++ RMsgs),
      merge_files(WriteFD, CombineFun, {start, LReadFD}, 
                  {{RVName, NMsgs}, RReadFD});
    _ ->
      case (LVName < RVName) of
        true ->
          file:write(WriteFD, 
                     serde:serialize_rec(msg, {LVName, LMsgs})),
          merge_files(WriteFD, CombineFun, {start, LReadFD},
                      {{RVName, RMsgs}, RReadFD});
        _ ->
          file:write(WriteFD, 
                     serde:serialize_rec(msg, {RVName, RMsgs})),
          merge_files(WriteFD, CombineFun, {{LVName, LMsgs}, LReadFD},
                      {start, RReadFD})
      end
  end.
                     
dump_rest(Line, ReadFD, WriteFD) ->  
  case is_binary(Line) of
    true -> file:write(WriteFD, binary_to_list(Line));
    _ -> 
      file:write(WriteFD, serde:serialize_rec(msg, Line))
  end,
  loop_write(ReadFD, WriteFD).

loop_write(ReadFD, WriteFD) ->
  Line = file:read_line(ReadFD),
  case Line of
    eof ->
      file:close(ReadFD),
      file:close(WriteFD);
    {ok, Data} ->
      file:write(WriteFD, binary_to_list(Data)),
      loop_write(ReadFD, WriteFD)
  end.
                       
            
ins(Recs, R) when is_list(Recs) ->
  lists:foldl(fun(Rec, S) -> ins(Rec, S) end, R, Recs);  
ins(Rec, R) ->
  try gb_sets:insert(Rec, R) catch _:_ -> R end.
    

apply_combine(none, Msgs) -> Msgs;
apply_combine(_, [_] = Msgs) -> Msgs;
apply_combine(F, [M|Rest]) ->
  X = lists:foldl(fun(Msg, Acc) -> F(Msg, Acc) end, M, Rest),
  [X].
  
  
