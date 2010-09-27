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
                num_active, table,
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
                     sub_state = {reading_partition, {RefPid, _, FDs}}} = 
                State) ->
  ?DEBUG("Worker In State.. ", [{state, vsplit_phase1}, 
                                {sub_state, {reading_partition, Partition}},
                                {job, JobId}, 
                                {worker, WId}]),
  NewFDs = handle_vertices(NumWorkers, WInfo, Vertices, 0, FDs),
  notify_master({MNode, MPid}, {vsplit_phase1_inter, WId, 
                                length(Vertices)}),
  {next_state, vsplit_phase1, 
   State#state{sub_state = {reading_partition, {RefPid, SS, NewFDs}}}, 
   ?SOURCE_TIMEOUT()};

vsplit_phase1({vertices_done, Vertices, RefPid, SS}, 
              #state{master_info = {MNode, MPid, _}, 
                     worker_info = {JobId, WId} = WInfo, 
                     num_workers = NumWorkers,
                     part_file = Partition,
                     sub_state = {reading_partition, {RefPid, _, FDs}}} = 
                State) ->
  ?DEBUG("Worker In State.. ", [{state, vsplit_phase1}, 
                                {sub_state, 
                                 {reading_partition_done, Partition}},
                                {job, JobId}, 
                                {worker, WId}]),
  NewFDs = handle_vertices(NumWorkers, WInfo, Vertices, 0, FDs),
  phoebus_source:destroy(SS),
  lists:foreach(
    fun({_, FD}) -> worker_store:close_step_file(FD) end, NewFDs),
  notify_master({MNode, MPid}, {vsplit_phase1_done, WId, length(Vertices)}),
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
  {next_state, await_master, State, ?MASTER_TIMEOUT()}.
%% ------------------------------------------------------------------------
%% vsplit_phase2 DONE
%% ------------------------------------------------------------------------



%% ------------------------------------------------------------------------
%% algo START
%% Description : Execute Algorithm on all nodes
%% ------------------------------------------------------------------------
algo(_Event, #state{master_info = {MNode, MPid, _}, 
                    worker_info = {JobId, WId}} = State) ->
  ?DEBUG("Worker In State.. ", [{state, algo}, {job, JobId}, 
                                {worker, WId}]),
  notify_master({MNode, MPid}, {algo_done, WId, 0}),
  {next_state, await_master, State}.
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
  {stop, normal, State}.


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
             #state{worker_info = WId} = State) ->
  ?DEBUG("Worker Received command.. ", 
         [{next_state, NState}, {worker, WId}, {extra_info, EInfo}]),
  {next_state, NState, State, 0}.
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
handle_event(_Event, StateName, State) ->
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
terminate(_Reason, _StateName, _State) ->
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
    fun(#vertex{vertex_id = VId} = Vertex, OldFDs) ->
        {Node, WId} = phoebus_utils:vertex_owner(JobId, VId, NumWorkers),
        worker_store:store_vertex(Vertex, {Node, {JobId, MyWId, WId}}, 
                                  Step, OldFDs)
    end, FDs, Vertices).  

transfer_files(NumWorkers, {JobId, WId}, Step) ->
  lists:foldl(
    fun(W, Pids) when W =:= WId -> Pids;
       (OWid, Pids) ->
        Node = phoebus_utils:map_to_node(JobId, OWid),
        Pid = worker_store:transfer_files(Node, {JobId, WId, OWid}, Step),
        [Pid|Pids]
    end, [], lists:seq(1, NumWorkers)).

notify_master({MNode, MPid}, Notification) ->
  rpc:call(MNode, gen_fsm, send_event, [MPid, Notification]).


%% TODO : have to implemnt.. using a table manager..
acquire_table(JobId, WId) ->
  Table = list_to_atom("test_table_" ++ integer_to_list(WId)),
  ets:insert(table_mapping, {{JobId, WId}, Table}), 
  Table.
