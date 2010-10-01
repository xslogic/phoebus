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
-export([start_link/6]).

%% gen_fsm callbacks
-export([init/1,          
         vsplit_phase1/2, 
         vsplit_phase2/2, 
         algo/2,
         post_algo/2,
         store_result/2, 
         await_master/2, 
         state_name/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SOURCE_TIMEOUT(), phoebus_utils:get_env(source_timeout, 120000)).
-define(MASTER_TIMEOUT(), phoebus_utils:get_env(master_timeout, 300000)).


-record(state, {worker_info, num_workers, part_file,
                master_info, sub_state, step,
                aggregate, table,
                algo_fun, combine_fun
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
start_link(WorkerInfo, NumWorkers, MInfo, Partition, AlgoFun, CombineFun) ->
  gen_fsm:start_link(?MODULE, [WorkerInfo, NumWorkers, 
                               MInfo, Partition, 
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
init([{JobId, WId} = WorkerInfo, NumWorkers, 
      {MNode, MPid}, Partition, AlgoFun, CombineFun]) ->
  MMonRef = erlang:monitor(process, MPid),
  Table = acquire_table(JobId, WId),
  register_worker(JobId, WId),
  {last_step, LastStep} = worker_store:init(JobId, WId),
  WorkerState = 
    case (LastStep < 0) of
      true -> vsplit_phase1;
      _ -> algo
    end,                
  {ok, WorkerState, 
   #state{master_info = {MNode, MPid, MMonRef}, worker_info = WorkerInfo,
          num_workers = NumWorkers, step = LastStep, sub_state = none,
          algo_fun = AlgoFun, combine_fun = CombineFun,
          table = Table, part_file = Partition}, 0}.

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
  {ok, SS} = phoebus_source:init(Partition),
  {ok, RefPid, SS2} = phoebus_source:read_vertices_start(SS),  
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
  phoebus_source:destroy(SS),
  worker_store:sync_table(Table, vertex, 0, true),
  lists:foreach(
    fun({_, FD}) -> worker_store:close_step_file(FD) end, NewFDs),
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



%% ------------------------------------------------------------------------
%% vsplit_phase2 START
%% Description : Transfer Partition data to other workers
%% ------------------------------------------------------------------------
vsplit_phase2(timeout, #state{master_info = {MNode, MPid, _}, 
                              worker_info = {_JobId, WId} = WInfo, 
                              num_workers = NumWorkers,
                              sub_state = none} = State) ->
  ?DEBUG("Worker In State.. ", [{state, vsplit_phase2}, {worker, WId}]),
  transfer_files(NumWorkers, WInfo, 0),
  notify_master({MNode, MPid}, {vsplit_phase2_done, WId, 0}),
  {next_state, await_master, 
   State#state{sub_state = {step_to_be_committed, 0}}, ?MASTER_TIMEOUT()}.
%% ------------------------------------------------------------------------
%% vsplit_phase2 DONE
%% ------------------------------------------------------------------------



%% ------------------------------------------------------------------------
%% algo START
%% Description : Execute Algorithm on all nodes
%% ------------------------------------------------------------------------
algo(_Event, #state{master_info = {MNode, MPid, _}, 
                    worker_info = {JobId, WId},
                    step = OldStep,
                    algo_fun = AlgoFun,
                    combine_fun = CombineFun,
                    table = Table} = State) ->
  NewStep = OldStep + 1,  
  ?DEBUG("nWorker In State.. ", [{state, algo}, {job, JobId}, 
                                {old_step, OldStep},
                                {new_step, NewStep},
                                {worker, WId}]),
  %% Feed previous step data into Compute fun
  %% Store new stuff in current step file..
  {ok, PrevVTable} =
    worker_store:init_step_file(vertex, JobId, WId, [read], OldStep), 
  NumActive = get_num_active(PrevVTable),
  {ok, PrevMTable} = 
    worker_store:init_step_file(msg, JobId, WId, [read], OldStep),
  worker_store:init_step_file(vertex, JobId, WId, [write], NewStep),
  worker_store:init_step_file(msg, JobId, WId, [write], NewStep),
  NumMessages = 
    dets:info(PrevMTable, size),  
  case (NumActive < 1) of
    true -> 
      case (NumMessages < 1) of
        true -> void;
        _ ->
          run_algo(JobId, WId, {msg, Table}, 
                   AlgoFun, CombineFun, NewStep)
      end;
    _ -> 
      run_algo(JobId, WId, {vertex, Table}, AlgoFun, CombineFun, NewStep)
  end,  
  dets:close(PrevVTable),
  dets:close(PrevMTable),
  notify_master({MNode, MPid}, {algo_done, WId, (NumMessages + NumActive)}),
  {next_state, await_master, State#state{step = NewStep}}.
%% ------------------------------------------------------------------------
%% algo DONE
%% ------------------------------------------------------------------------

post_algo(_Event, #state{master_info = {MNode, MPid, _}, 
                         worker_info = {JobId, WId}} = State) ->
  ?DEBUG("Worker In State.. ", [{state, post_algo}, {job, JobId}, 
                                {worker, WId}]),
  notify_master({MNode, MPid}, {post_algo_done, WId, 0}),
  {next_state, await_master, State}.

store_result(_Event, State) ->
  {next_state, await_master, State}.


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

handle_info(_Info, StateName, State) ->
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
handle_vertices(NumWorkers, {JobId, MyWId}, Vertices, Step, FDs) ->
  lists:foldl(
    fun({VName, _, _, _} = Vertex, OldFDs) ->
        {Node, WId} = phoebus_utils:vertex_owner(JobId, VName, NumWorkers),
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
  Table = list_to_atom("test_table_" ++ integer_to_list(WId)),
  %% Vtable = Worker_store:table_name(Table, vertex),
  %% MTable = worker_store:table_name(Table, msg),
  ets:insert(table_mapping, {{JobId, WId}, Table}), 
  worker_store:init_step_file(vertex, JobId, WId, [write], 0),
  worker_store:init_step_file(msg, JobId, WId, [write], 0),
  Table.

register_worker(JobId, WId) ->
  ets:insert(worker_registry, {{JobId, WId}, self()}).

run_algo(JobId, _WId, {IterType, Table}, AlgoFun, CombineFun, Step) ->
  PrevVTable = worker_store:table_name(Table, vertex, Step - 1),
  PrevMTable = worker_store:table_name(Table, msg, Step - 1),
  CurrVTable = worker_store:table_name(Table, vertex, Step),
  CurrMTable = worker_store:table_name(Table, msg, Step),
  algo_loop(JobId, PrevVTable, PrevMTable, CurrVTable, CurrMTable, 
            AlgoFun, CombineFun, {IterType, start}).

algo_loop(JobId, PrevVTable, PrevMTable, CurrVTable, CurrMTable, 
          AlgoFun, CombineFun, {vertex, start}) ->
  case dets:select(PrevVTable, 
                   [{{'_', '_', active, '_'}, [], ['$_']}], 5) of
    {Sel, Cont} ->
      iterate_vertex(JobId, Sel, PrevMTable, CurrVTable, CurrMTable, 
                     AlgoFun, CombineFun),
      algo_loop(JobId, PrevVTable, PrevMTable, CurrVTable, CurrMTable,
                AlgoFun, CombineFun, {vertex, Cont});
    '$end_of_table' ->
      algo_loop(JobId, PrevVTable, PrevMTable, CurrVTable, CurrMTable,
                AlgoFun, CombineFun, {msg, start})
  end;
algo_loop(JobId, PrevVTable, PrevMTable, CurrVTable, CurrMTable, 
          AlgoFun, CombineFun, {vertex, Cont}) ->
  case dets:select(Cont) of
    {Sel, Cont2} ->
      iterate_vertex(JobId, Sel, PrevMTable, CurrVTable, CurrMTable, 
                     AlgoFun, CombineFun),
      algo_loop(JobId, PrevVTable, PrevMTable, CurrVTable, CurrMTable,
                AlgoFun, CombineFun, {vertex, Cont2});
    '$end_of_table' ->
      algo_loop(JobId, PrevVTable, PrevMTable, CurrVTable, CurrMTable,
                AlgoFun, CombineFun, {msg, start})
  end;
algo_loop(JobId, PrevVTable, PrevMTable, CurrVTable, CurrMTable, 
          AlgoFun, CombineFun, {msg, start}) ->
  iterate_msg(JobId, dets:first(PrevMTable), PrevVTable, 
              PrevMTable, CurrVTable, CurrMTable, AlgoFun, CombineFun).
      
iterate_msg(_, '$end_of_table', _, _, _, _, _, _) -> void;
iterate_msg(JobId, K, PrevVTable, PrevMTable, CurrVTable, CurrMTable, 
            AlgoFun, CombineFun) ->
  case dets:lookup(CurrVTable, K) of
    [] ->
      case dets:lookup(PrevMTable, K) of
        [{_, M}|Rest] ->
          InMsg = 
            lists:foldl(fun({_, Msg}, Acc) -> CombineFun(Msg, Acc) end,
              M, Rest),
          case dets:lookup(PrevVTable, K) of
            [OldVInfo] ->
              {NewV, OutMsgs} = AlgoFun(OldVInfo, [InMsg]),
              dets:insert(CurrVTable, NewV),
              %% TODO Find out which worker these messages 
              %% are meant for first..
              dets:insert(CurrMTable, OutMsgs);
            [] -> 
              %% New vertex created...
              dets:insert(CurrVTable, {K, "new", active, []})
          end;
        _ -> void
      end;
    _ -> void
  end,
  iterate_msg(JobId, dets:next(PrevMTable, K), PrevVTable, 
              PrevMTable, CurrVTable, CurrMTable, AlgoFun, CombineFun).

                                          
iterate_vertex(_JobId, Sel, PrevMTable, CurrVTable, CurrMTable, 
               AlgoFun, CombineFun) ->
  lists:foreach(
    fun({VName, _, _, _} = OldVInfo) ->
        InMsg = 
          case dets:lookup(PrevMTable, VName) of
            [] -> [];
            [{_, M}|Rest] ->
              lists:foldl(
                fun({_, Msg}, Acc) -> CombineFun(Msg, Acc) end,
                M, Rest)
          end,
        {NewV, OutMsgs} = AlgoFun(OldVInfo, [InMsg]),
        dets:insert(CurrVTable, NewV),
        %% TODO Find out which worker these messages are meant for first..
        dets:insert(CurrMTable, OutMsgs)
    end, Sel).
            
get_num_active(TName) ->
  Lst = dets:select(TName, [{{'$1', '_', active, '_'}, [], ['$$']}]),
  length(Lst).
