%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created : 25 Sep 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(phoebus_worker).
-include("phoebus.hrl").
-behaviour(gen_fsm).

%% API
-export([start_link/7]).

%% gen_fsm callbacks
-export([init/1,          
         vsplit_phase1/2, 
         vsplit_phase2/2, 
         vsplit_phase3/2, 
         algo/2,
         await_buffer_merge/2,
         post_algo/2,
         store_result/2, 
         await_master/2, 
         state_name/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SOURCE_TIMEOUT(), phoebus_utils:get_env(source_timeout, 120000)).
-define(MASTER_TIMEOUT(), phoebus_utils:get_env(master_timeout, 300000)).
-define(STORE(Type, Table, Rec), 
        worker_store:table_insert(Type, Table, Rec)).


-record(state, {worker_info, num_workers, part_file,
                output_dir, master_info, sub_state, step,
                aggregate, table, algo_fun, combine_fun
               }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(WorkerInfo, NumWorkers, MInfo,
           Partition, OutputDir, AlgoFun, CombineFun) ->
  gen_fsm:start_link(?MODULE, [WorkerInfo, NumWorkers, 
                               MInfo, Partition, OutputDir, 
                               AlgoFun, CombineFun], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([{JobId, WId, Nodes}, NumWorkers, 
      {MNode, MPid}, Partition, OutputDir, AlgoFun, CombineFun]) ->
  MMonRef = erlang:monitor(process, MPid),
  erlang:group_leader(whereis(init), self()),
  ets:insert(all_nodes, {JobId, Nodes}),
  Table = acquire_table(JobId, WId),
  register_worker(JobId, WId),
  {last_step, LastStep} = worker_store:init(JobId, WId),
  {WorkerState, Step, Timeout} = 
    case (LastStep < 0) of
      true -> {vsplit_phase1, 0, 0};
      _ -> {await_master, LastStep, ?MASTER_TIMEOUT()}
    end,                
  {ok, WorkerState, 
   #state{master_info = {MNode, MPid, MMonRef}, worker_info = {JobId, WId},
          num_workers = NumWorkers, step = Step, sub_state = none,
          algo_fun = AlgoFun, combine_fun = CombineFun,
          output_dir = OutputDir, table = Table, 
          part_file = Partition}, Timeout}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------


%% ------------------------------------------------------------------------
%% vsplit_phase1 START
%% Description : Read the partition.. and save into stable storage
%% ------------------------------------------------------------------------
vsplit_phase1(timeout, #state{worker_info = {JobId, WId}, 
                              sub_state = none,
                              part_file = Partition} = State) ->
  ?DEBUG("Worker In State.. ", [{state, vsplit_phase1}, {job, JobId}, 
                                {worker, WId}]),
  {ok, SS} = phoebus_rw:init(Partition),
  {ok, RefPid, SS2} = phoebus_rw:read_vertices_start(SS),  
  %% notify_master({MNode, MPid}, {vsplit_phase1_done, WId, 0}),
  {next_state, vsplit_phase1, 
   State#state{sub_state = {reading_partition, {RefPid, SS2, []}}}};

vsplit_phase1(timeout, #state{worker_info = {JobId, WId}, 
                              sub_state = {reading_partition, _},
                              part_file = Partition} = State) ->
  ?DEBUG("No response from source.. Shutting down...", 
         [{state, vsplit_phase1}, {job, JobId}, 
          {worker, WId}, {partition, Partition}]),
  {stop, timeout_expired, State};

vsplit_phase1({vertices, Vertices, RefPid, SS}, 
              #state{master_info = {MNode, MPid, _}, 
                     worker_info = {JobId, WId} = WInfo, 
                     num_workers = NumWorkers,
                     part_file = Partition,
                     table = Table,
                     sub_state = {reading_partition, {RefPid, _, FDs}}} = 
                State) ->
  ?DEBUG("Worker In State.. ", [{state, vsplit_phase1}, 
                                {sub_state, {reading_partition, Partition}},
                                {job, JobId}, 
                                {worker, WId}]),
  NewFDs = handle_vertices(NumWorkers, WInfo, Vertices, 0, FDs),
  notify_master({MNode, MPid}, {vsplit_phase1_inter, WId, 
                                length(Vertices)}),
  worker_store:sync_table(Table, vertex, 0, false),
  {next_state, vsplit_phase1, 
   State#state{sub_state = {reading_partition, {RefPid, SS, NewFDs}}}, 
   ?SOURCE_TIMEOUT()};

vsplit_phase1({vertices_done, Vertices, RefPid, SS}, 
              #state{master_info = {MNode, MPid, _}, 
                     worker_info = {JobId, WId} = WInfo, 
                     num_workers = NumWorkers,
                     part_file = Partition,
                     table = Table,
                     sub_state = {reading_partition, {RefPid, _, FDs}}} = 
                State) ->
  ?DEBUG("Worker In State.. ", [{state, vsplit_phase1}, 
                                {sub_state, 
                                 {reading_partition_done, Partition}},
                                {job, JobId}, 
                                {worker, WId}]),
  NewFDs = handle_vertices(NumWorkers, WInfo, Vertices, 0, FDs),
  phoebus_rw:destroy(SS),
  worker_store:sync_table(Table, vertex, 0, true),
  lists:foreach(
    fun({_, FD}) -> worker_store:close_step_file(vertex, FD) end, NewFDs),
  notify_master({MNode, MPid}, {vsplit_phase1_inter, WId, 
                                length(Vertices)}),
  notify_master({MNode, MPid}, {vsplit_phase1_done, WId, 0}),
  ?DEBUG("Worker Exiting State.. ", [{state, vsplit_phase1}, 
                                     {job, JobId}, {worker, WId}]),
  {next_state, await_master, State#state{sub_state = none}, 
   ?MASTER_TIMEOUT()}.
%% ------------------------------------------------------------------------
%% vsplit_phase1 DONE
%% ------------------------------------------------------------------------
vsplit_phase2(timeout, #state{master_info = {MNode, MPid, _}, 
                              worker_info = {_JobId, WId} = WInfo, 
                              num_workers = NumWorkers,
                              sub_state = none} = State) ->
  ?DEBUG("Worker In State.. ", [{state, vsplit_phase2}, {worker, WId}]),
  transfer_files(NumWorkers, WInfo, 0),
  notify_master({MNode, MPid}, {vsplit_phase2_done, WId, 0}),
  {next_state, await_master, State#state{sub_state = none}, 
   ?MASTER_TIMEOUT()}.


%% ------------------------------------------------------------------------
%% vsplit_phase3 START
%% Description : Load active workers in flag table 
%% (Can possibly get rid of this step.. refactor..)
%% ------------------------------------------------------------------------
vsplit_phase3(timeout, #state{master_info = {MNode, MPid, _}, 
                              worker_info = {JobId, WId}, 
                              sub_state = none} = State) ->
  ?DEBUG("Worker In State.. ", [{state, vsplit_phase3}, {worker, WId}]),
  worker_store:load_active_vertices(JobId, WId),
  notify_master({MNode, MPid}, {vsplit_phase3_done, WId, 0}),
  {next_state, await_master, 
   State#state{sub_state = {step_to_be_committed, 0}}, ?MASTER_TIMEOUT()}.
%% ------------------------------------------------------------------------
%% vsplit_phase3 DONE
%% ------------------------------------------------------------------------



%% ------------------------------------------------------------------------
%% algo START
%% Description : Execute Algorithm on all nodes
%% ------------------------------------------------------------------------
algo(timeout, #state{master_info = {MNode, MPid, _}, 
                     worker_info = {JobId, WId},
                     num_workers = NumWorkers,
                     step = OldStep,
                     algo_fun = AlgoFun,
                     combine_fun = CombineFun,
                     table = Table} = State) ->
  NewStep = OldStep + 1,  
  ?DEBUG("Worker In State.. ", [{state, algo}, {job, JobId}, 
                                {old_step, OldStep},
                                {new_step, NewStep},
                                {worker, WId}]),
  %% Feed previous step data into Compute fun
  %% Store new stuff in current step file..
  {ok, PrevFTable} =
    worker_store:init_step_file(flag, JobId, WId, [read], OldStep), 
  NumActive = get_num_active(PrevFTable),
  {ok, PrevMTable} = 
    worker_store:init_step_file(msg, JobId, WId, [read], OldStep),

  worker_store:init_step_file(vertex, JobId, WId, [write], NewStep),
  worker_store:init_step_file(msg, JobId, WId, [write], NewStep),
  NumMessages = 
    dets:info(PrevMTable, size),  
  BuffPids = 
    case (NumActive < 1) of
      true -> 
        case (NumMessages < 1) of
          true -> [];
          _ ->
            run_algo({JobId, WId, NumWorkers}, {msg, Table}, 
                     AlgoFun, CombineFun, NewStep)
        end;
      _ -> 
        run_algo({JobId, WId, NumWorkers}, {vertex, Table}, 
                 AlgoFun, CombineFun, NewStep)
    end,
  {NextState, SubState} =
    case BuffPids of
      [] -> 
        notify_master({MNode, MPid}, 
                      {algo_done, WId, (NumMessages + NumActive)}),
        {await_master, none};
      _ -> {await_buffer_merge, {{buffer_pids, BuffPids},
                                 {num_actives, (NumMessages + NumActive)}}}
    end,
  {next_state, NextState, 
   State#state{step = NewStep, sub_state = SubState}}.
%% ------------------------------------------------------------------------
%% algo DONE
%% ------------------------------------------------------------------------

%% ------------------------------------------------------------------------
%% post_algo START
%% Description : wait for master.. and then commit the step..
%% ------------------------------------------------------------------------
await_buffer_merge({done_merge, Pid},
                   #state{master_info = {MNode, MPid, _}, 
                          sub_state = {{buffer_pids, BPids}, 
                                       {num_actives, NAct}},
                          worker_info = {JobId, WId}} = State) ->
  ?DEBUG("Worker In State.. ", [{state, await_buffer_merge}, {job, JobId}, 
                                {worker, WId}]),
  NewBPids =
    case lists:keyfind(Pid, 2, BPids) of
      false -> BPids;
      {OWid, Pid, MRef} -> 
        erlang:demonitor(MRef),
        lists:keydelete(OWid, 1, BPids)
    end,
  {NextState, SubState, Timeout} = 
    case NewBPids of
      [] ->
        notify_master({MNode, MPid}, {algo_done, WId, NAct}),
        {await_master, none, ?MASTER_TIMEOUT()};
      _ -> 
        {await_buffer_merge, 
         {{buffer_pids, NewBPids}, {num_actives, NAct}}, none}
    end,
  case Timeout of
    none -> {next_state, NextState, State#state{sub_state = SubState}};
    _ -> {next_state, NextState, State#state{sub_state = SubState},Timeout}
  end.
%% ------------------------------------------------------------------------
%% post_algo DONE
%% ------------------------------------------------------------------------



%% ------------------------------------------------------------------------
%% post_algo START
%% Description : wait for master.. and then commit the step..
%% ------------------------------------------------------------------------
post_algo(timeout, #state{master_info = {MNode, MPid, _},
                          num_workers = NumWorkers,
                          step = Step,
                          worker_info = {JobId, WId}} = State) ->
  ?DEBUG("Worker In State.. ", [{state, post_algo}, {job, JobId}, 
                                {worker, WId}]),
  transfer_files(NumWorkers, {JobId, WId}, Step),
  notify_master({MNode, MPid}, {post_algo_done, WId, 0}),
  {next_state, await_master, 
   State#state{sub_state = {step_to_be_committed, Step}}, 
   ?MASTER_TIMEOUT()}.
%% ------------------------------------------------------------------------
%% post_algo DONE
%% ------------------------------------------------------------------------


%% ------------------------------------------------------------------------
%% store_result START
%% Description : store result to outdir..
%% ------------------------------------------------------------------------
store_result(timeout, #state{output_dir = OutputDir, 
                             step = Step,
                             worker_info = {JobId, WId}} = State) ->
  ?DEBUG("Worker In State.. ", [{state, post_algo}, {job, JobId}, 
                                {worker, WId}, {output_dir, OutputDir}]),
  {ok, SS} = phoebus_rw:init(OutputDir ++ "part-" 
                             ++ integer_to_list(WId)),
  {ok, VTable} =
    worker_store:init_step_file(vertex, JobId, WId, [read], Step),
  SS2 = store_result_loop(SS, VTable, start),
  %% release_table(Table, JobId, WId),
  phoebus_rw:destroy(SS2),
  {next_state, await_master, State}.
%% ------------------------------------------------------------------------
%% store_result START
%% ------------------------------------------------------------------------


%% ------------------------------------------------------------------------
%% await_master START
%% Description : Recevive Command from Master for Next State
%% ------------------------------------------------------------------------
await_master(timeout, 
             #state{worker_info = {JobId, WId}} = State) ->
  ?DEBUG("No response from master.. Shutting down...", 
         [{state, vsplit_phase1}, {job, JobId}, {worker, WId}]),
  {stop, master_timeout, State};

await_master({goto_state, NState, EInfo}, 
             #state{worker_info = {JobId, WId}, 
                    step = CurrentStep,
                    sub_state = SubState} = State) ->
  ?DEBUG("Worker Received command.. ", 
         [{next_state, NState}, {worker, WId}, {extra_info, EInfo}]),
  NewStep = 
    case SubState of
      {step_to_be_committed, Step} ->
        worker_store:commit_step(JobId, WId, Step),
        Step;
      _ -> CurrentStep
    end,
  {next_state, NState, State#state{aggregate = EInfo, sub_state = none, 
                                   step = NewStep}, 0}.
%% ------------------------------------------------------------------------
%% await_master DONE
%% ------------------------------------------------------------------------


%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
state_name(_Event, _From, State) ->
  Reply = ok,
  {reply, Reply, state_name, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(sync_table, StateName, #state{table = Table, 
                                           step = Step} = State) ->
  worker_store:sync_table(Table, vertex, Step, false),
  {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
  Reply = ok,
  {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info({'DOWN', MMonRef, _, _, _}, StateName,
            #state{worker_info = {JobId, WId},
                   master_info = {_, _, MMonRef}} = State) ->
  ?DEBUG("Master Down... Shutting Down..", [{state_name, StateName}, 
                                             {job, JobId}, {worker, WId}]),
  {stop, monitor_down, State};  

handle_info(Info, StateName, #state{worker_info = {JobId, WId}} = State) ->
  ?DEBUG("Received Info....", [{info, Info}, {state_name, StateName}, 
                               {job, JobId}, {worker, WId}]),
  {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, #state{worker_info = {JobId, WId}}) ->
  %% TODO : might be a better idea for the master to do this..
  ets:delete(worker_registry, {JobId, WId}),
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
store_result_loop(RWState, VTable, start) ->
  case dets:select(VTable, [{{'$1', '$2', '$3'}, [], ['$_']}], 5) of
    {Sel, Cont} ->
      NewRWState = phoebus_rw:store_vertices(RWState,Sel),
      store_result_loop(NewRWState, VTable, Cont);
    '$end_of_table' -> RWState
  end;
store_result_loop(RWState, VTable, Cont) ->
  case dets:select(Cont) of
    {Sel, Cont2} ->
      NewRWState = phoebus_rw:store_vertices(RWState,Sel),
      store_result_loop(NewRWState, VTable, Cont2);
    '$end_of_table' -> RWState
  end.
      
      

handle_vertices(NumWorkers, {JobId, MyWId}, Vertices, Step, FDs) ->
  lists:foldl(
    fun({VName, _, _} = Vertex, OldFDs) ->
        {Node, WId} = phoebus_utils:vertex_owner(JobId, VName, NumWorkers),
        %% case VName of
        %%   "160" -> io:format("~n~n [~p] : [~p] ~n~n", [MyWId, WId]);
        %%   _ -> void
        %% end,                     
        worker_store:store_vertex(Vertex, {Node, {JobId, MyWId, WId}}, 
                                  Step, OldFDs)
    end, FDs, Vertices).  

transfer_files(NumWorkers, {JobId, WId}, Step) ->
  Refs = 
    lists:foldl(
      fun(W, Pids) when W =:= WId -> Pids;
         (OWid, MRefs) ->
          Node = phoebus_utils:map_to_node(JobId, OWid),
          Pid = worker_store:transfer_files(Node, {JobId, WId, OWid}, Step),
          MRef = erlang:monitor(process, Pid),
          [MRef|MRefs]
      end, [], lists:seq(1, NumWorkers)),
  wait_loop(Refs).

wait_loop([]) -> done;
wait_loop(Refs) ->
  receive
    {'DOWN', MRef, _, _, _} -> 
      NRefs = lists:delete(MRef, Refs),
      wait_loop(NRefs)
  end.

notify_master({MNode, MPid}, Notification) ->
  rpc:call(MNode, gen_fsm, send_event, [MPid, Notification]).


%% TODO : have to implemnt.. using a table manager..
acquire_table(JobId, WId) ->
  Table = table_manager:acquire_table(JobId, WId),
  io:format("~n Acquired table.. [~p] from [~p] ~n", [Table, WId]),
  %% Vtable = Worker_store:table_name(Table, vertex),
  %% MTable = worker_store:table_name(Table, msg),
  worker_store:init_step_file(vertex, JobId, WId, {table, Table}, 0),
  worker_store:init_step_file(flag, JobId, WId, {table, Table}, 0),
  worker_store:init_step_file(msg, JobId, WId, {table, Table}, 0),
  ets:insert(table_mapping, {{JobId, WId}, Table}), 
  Table.


register_worker(JobId, WId) ->
  ets:insert(worker_registry, {{JobId, WId}, self()}).

run_algo({JobId, WId, NumWorkers}, 
         {IterType, Table}, AlgoFun, CombineFun, Step) ->
  worker_store:sync_table(Table, vertex, Step, true),
  PrevFTable = worker_store:table_name(Table, flag, Step - 1),
  PrevMTable = worker_store:table_name(Table, msg, Step - 1),
  CurrFTable = worker_store:table_name(Table, flag, Step - 1),
  CurrMTable = worker_store:table_name(Table, msg, Step),
  VTable = worker_store:table_name(Table, vertex, Step),
  WriteFDs = 
    algo_loop({JobId, WId, NumWorkers, Step}, VTable, PrevFTable, 
              PrevMTable, CurrFTable, CurrMTable, 
              AlgoFun, CombineFun, {IterType, start}, []),
  MyPid = self(),
  %% io:format("~n~n~nFiles to Merge [~p]~n~n~n", [WriteFDs]),
  lists:foldl(
    fun({OWId, FD, FName}, BPids) -> 
        Dir = ?RSTEP_DIR(JobId, WId, Step, OWId),
        %% io:format("~n~n~nMerging [~p] to dir [~p]~n~n~n", [FName, Dir]),
        file:close(FD),        
        {ok, BPid} = 
          msg_buffer:merge_file(FName, MyPid, CombineFun, Dir),
        MRef = erlang:monitor(process, BPid),
        [{OWId, BPid, MRef}|BPids]
    end, [], WriteFDs).

algo_loop({JobId, WId, NumWorkers, Step}, VTable, PrevFTable, PrevMTable, 
          CurrFTable, CurrMTable, AlgoFun, 
          CombineFun, {vertex, start}, WriteFDs) ->
  case dets:select(PrevFTable, [{{'$1', active}, [], ['$_']}], 5) of
    {Sel, Cont} ->
      NewWriteFDs = 
        iterate_vertex({JobId, WId, NumWorkers, Step}, Sel, VTable, 
                       PrevMTable, CurrFTable, CurrMTable, 
                       AlgoFun, CombineFun, WriteFDs),
      algo_loop({JobId, WId, NumWorkers, Step}, VTable, PrevFTable, 
                PrevMTable, CurrFTable, CurrMTable, AlgoFun, 
                CombineFun, {vertex, Cont}, NewWriteFDs);
    '$end_of_table' ->
      algo_loop({JobId, WId, NumWorkers, Step}, VTable, PrevFTable, 
                PrevMTable, CurrFTable, CurrMTable, AlgoFun, 
                CombineFun, {msg, start}, WriteFDs)
  end;
algo_loop({JobId, WId, NumWorkers, Step}, VTable, PrevFTable, PrevMTable, 
          CurrFTable, CurrMTable, AlgoFun, 
          CombineFun, {vertex, Cont}, WriteFDs) ->
  case dets:select(Cont) of
    {Sel, Cont2} ->
      NewWriteFDs = 
        iterate_vertex({JobId, WId, NumWorkers, Step}, Sel, VTable, 
                       PrevMTable, CurrFTable, CurrMTable, 
                       AlgoFun, CombineFun, WriteFDs),
      algo_loop({JobId, WId, NumWorkers, Step}, VTable, PrevFTable, 
                PrevMTable, CurrFTable, CurrMTable, AlgoFun, 
                CombineFun, {vertex, Cont2}, NewWriteFDs);
    '$end_of_table' ->
      algo_loop({JobId, WId, NumWorkers, Step}, VTable, PrevFTable, 
                PrevMTable, CurrFTable, CurrMTable, AlgoFun, 
                CombineFun, {msg, start}, WriteFDs)
  end;
algo_loop({JobId, WId, NumWorkers, Step}, VTable, PrevFTable, PrevMTable, 
          CurrFTable, CurrMTable, AlgoFun, 
          CombineFun, {msg, start}, WriteFDs) ->
  iterate_msg({JobId, WId, NumWorkers, Step}, dets:first(PrevMTable), 
              VTable, PrevFTable, PrevMTable, CurrFTable, CurrMTable, 
              AlgoFun, CombineFun, WriteFDs).
      
iterate_msg(_, '$end_of_table', _, _, _, _, _, _, _, WriteFDs) -> WriteFDs;
iterate_msg({JobId, WId, NumWorkers, Step}, K, VTable, PrevFTable, 
            PrevMTable, CurrFTable, CurrMTable, 
            AlgoFun, CombineFun, WriteFDs) ->
  NewWriteFDs = 
    case dets:lookup(CurrFTable, K) of
      [] ->
        case dets:lookup(PrevMTable, K) of
          [{_, _M}|_Rest] = Lst ->
            InMsgs = apply_combine(CombineFun, Lst),
            case dets:lookup(VTable, K) of
              [{VName, _, _} = OldVInfo] ->
                {NewV, OutMsgs, VState} = AlgoFun(OldVInfo, InMsgs),
                ?STORE(flag, CurrFTable, {VName, VState}),
                ?STORE(vertex, VTable, NewV),
                handle_msgs({JobId, WId, NumWorkers, Step}, WriteFDs, 
                            CurrMTable, OutMsgs);
              [] -> 
                %% New vertex created...
                {NewV, OutMsgs, VState} = AlgoFun({K, K, []}, InMsgs),
                ?STORE(flag, CurrFTable, {K, VState}),
                ?STORE(vertex, VTable, NewV),
                handle_msgs({JobId, WId, NumWorkers, Step}, WriteFDs, 
                            CurrMTable, OutMsgs)
            end;
          _ -> WriteFDs
        end;
      _ -> WriteFDs
    end,
  iterate_msg({JobId, WId, NumWorkers, Step}, dets:next(PrevMTable, K), 
              VTable, PrevFTable, PrevMTable, CurrFTable, CurrMTable, 
              AlgoFun, CombineFun, NewWriteFDs).

                                          
iterate_vertex({JobId, WId, NumWorkers, Step}, Sel, VTable, PrevMTable, 
               CurrFTable, CurrMTable, AlgoFun, CombineFun, WriteFDs) ->
  lists:foldl(
    fun({VName, _}, FDs) ->
        [OldVInfo] =
          try
            case dets:lookup(VTable, VName) of
              [OV] -> [OV];
              {error, {premature_eof, _}} ->
                %% Weird "premature_eof" error from dets sometimes
                %% when size of partion is <500 and >=250
                io:format("~n~nGot Error 1 while looking up [~p, ~p]~n~n",
                          [VTable, VName]),
                worker_store:sync_table(VTable, true),
                dets:lookup(VTable, VName)
            end of
            X -> X
          catch
            Er1:Er2 ->
              io:format("~nNo Vertex foind [~p][~p][~p]~n", 
                        [VName, VTable, {Er1, Er2}]),
              throw(some_error)
          end,                
        InMsgs = apply_combine(CombineFun, dets:lookup(PrevMTable, VName)),
        {NewV, OutMsgs, VState} = AlgoFun(OldVInfo, InMsgs),
        ?STORE(flag, CurrFTable, {VName, VState}),
        try
          ?STORE(vertex, VTable, NewV)
        catch
          E1:E2 ->
            io:format("~n~nGot Error while writing [~p, ~p, ~p]~n~n",
                      [E1, E2, NewV]),
            worker_store:sync_table(VTable, true),
            ?STORE(vertex, VTable, NewV)
        end,
        handle_msgs({JobId, WId, NumWorkers, Step}, FDs, 
                    CurrMTable, OutMsgs)
    end, WriteFDs, Sel).
            
get_num_active(TName) ->
  length(dets:select(TName, [{{'$1', active}, [], ['$$']}])).

handle_msgs({JobId, WId, NumWorkers, Step}, WriteFDs, CurrMTable, Msgs) ->  
  lists:foldl(
    fun({VName, Msg} = _M, WFDs) ->
        {_Node, OWId} = 
          phoebus_utils:vertex_owner(JobId, VName, NumWorkers),
        case OWId of
          WId -> 
            ?STORE(msg, CurrMTable, {VName, [Msg]}), WFDs;
          _ -> 
            
            {NewWFDs, WriteFD, _File} = 
              case lists:keyfind(OWId, 1, WFDs) of
                false -> 
                  {NFD, FName} = open_fd(JobId, WId, OWId, Step),
                  {[{OWId, NFD, FName}|WFDs], NFD, FName};
                {OWId, FD, F} -> {WFDs, FD, F}
              end,
            %% io:format("~n~n~nWriting [~p] into [~p]~n~n~n", [M, File]),
            file:write(WriteFD, 
                          worker_store:serialize_rec(
                            msg, {VName, [Msg]})),
            NewWFDs
        end
    end, WriteFDs, Msgs).


open_fd(JobId, WId, OWId, Step) ->
  FName = ?MSG_TMP_FILE(JobId, WId, Step, OWId),
  %% io:format("~n~n~nOpening File [~p]~n~n~n", [FName]),
  {ok, FD} = file:open(FName, [write]),
  {FD, FName}.
   
apply_combine(_, []) -> [];
apply_combine(Fun, Msgs) ->
  NewLSt = [M || {_, M} <- Msgs],
  apply_combine2(Fun, NewLSt).
  
apply_combine2(none, Msgs) -> Msgs;
apply_combine2(_, [_] = Msgs) -> Msgs;
apply_combine2(F, [M|Rest]) ->
  X = lists:foldl(fun(Msg, Acc) -> F(Msg, Acc) end, M, Rest),
  [X].
