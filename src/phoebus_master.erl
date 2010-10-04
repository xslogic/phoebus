%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created : 25 Sep 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(phoebus_master).
-include("phoebus.hrl").
-behaviour(gen_fsm).

%% API
-export([start_link/1]).

%% gen_fsm callbacks
-export([init/1, 
         vsplit_phase1/2, 
         vsplit_phase2/2, 
         vsplit_phase3/2, 
         algo/2,
         post_algo/2,
         check_algo_finish/2,
         store_result/2,
         end_state/2,
         state_name/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {step = 0, max_steps, vertices = 0, job_id,
                conf, workers = {[], []}, algo_sub_state = 0}).

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
start_link(Conf) ->
  Name = name(proplists:get_value(name, Conf, noname)),
  gen_fsm:start_link(?MODULE, [[{name, Name}|Conf]], []).

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
init([Conf]) ->
  {ok, SS} = phoebus_source:init(proplists:get_value(input_dir, Conf)),
  {ok, Partitions, SS2} = phoebus_source:partition_input(SS),
  phoebus_source:destroy(SS2),
  JobId = phoebus_utils:job_id(),
  %% NOTE: Workers must be of the form [{Node, wId, wPid, wMonRef, wState}]
  DefAlgoFun = 
    fun({VName, VValStr, EList}, InMsgs) -> 
        io:format("~n[~p]Recvd msgs : ~p ~n", [VName, InMsgs]),
        ToGo = 
          lists:foldl(fun(M, Acc) -> Acc ++ "||" ++ M end, 
                      VValStr, InMsgs),
        Msgs = 
          lists:foldl(
            fun({_EValStr, TVName}, MsgAcc) ->
                [{TVName, ToGo}|MsgAcc]
            end, [], EList),
        io:format("[~p]Sending msgs : ~p ~n", [VName, Msgs]),
        {{VName, ToGo, EList}, Msgs, hold}
    end,
  DefCombineFun = none,
  %% DefCombineFun = fun(Msg1, Msg2) -> Msg1 ++ "||" ++ Msg2 end,
  Workers = start_workers(JobId, {erlang:node(), self()}, 
                          Partitions, 
                          proplists:get_value(algo_fun, 
                                              Conf, DefAlgoFun),
                          proplists:get_value(combine_fun, 
                                              Conf, DefCombineFun)),
  {ok, vsplit_phase1, 
   #state{max_steps = proplists:get_value(max_steps, Conf, 100000),
          job_id = JobId,
          workers = {Workers, []}, conf = Conf}}.

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
%% Description : Get vertex loading progress from Wrokers
%% ------------------------------------------------------------------------
vsplit_phase1({vsplit_phase1_inter, WId, Vertices}, 
              #state{job_id = JobId, 
                     conf = Conf,
                     vertices = OldVertices} = State) ->
  NewVertices = OldVertices + Vertices,
  ?DEBUG("Vertices Uncovered..", [{job, JobId}, {workers, WId},
                                  {name, proplists:get_value(name, Conf)}, 
                                  {num, NewVertices}]),
  {next_state, vsplit_phase1, State#state{vertices = NewVertices}};

vsplit_phase1({vsplit_phase1_done, WId, Vertices}, 
              #state{job_id = JobId, 
                     conf = Conf,
                     vertices = OldVertices,
                     workers = Workers} = State) ->
  NewVertices = OldVertices + Vertices,
  ?DEBUG("Vertices Uncovered..", [{job, JobId}, {workers, WId},
                                  {name, proplists:get_value(name, Conf)}, 
                                  {num, NewVertices}]),
  {NewWorkers, NextState} = 
    update_workers(vsplit_phase1, vsplit_phase2, none, Workers, WId),  
  {next_state, NextState, State#state{workers = NewWorkers}}.
%% ------------------------------------------------------------------------
%% vsplit_phase1 DONE
%% ------------------------------------------------------------------------


%% ------------------------------------------------------------------------
%% vsplit_phase2 START
%% Description : Wait for Workers to finish transferring files..
%% ------------------------------------------------------------------------
vsplit_phase2({vsplit_phase2_done, WId, _WData}, 
              #state{workers = Workers} = State) ->
  {NewWorkers, NextState} = 
    update_workers(vsplit_phase2, vsplit_phase3, bla, Workers, WId),  
  {next_state, NextState, State#state{workers = NewWorkers}}.
%% ------------------------------------------------------------------------
%% vsplit_phase2 DONE
%% ------------------------------------------------------------------------


%% ------------------------------------------------------------------------
%% vsplit_phase3 START
%% Description : copy vertex data to new dir..
%% ------------------------------------------------------------------------
vsplit_phase3({vsplit_phase3_done, WId, _WData}, 
              #state{workers = Workers} = State) ->
  {NewWorkers, NextState} = 
    update_workers(vsplit_phase3, algo, bla, Workers, WId),  
  {next_state, NextState, State#state{workers = NewWorkers}}.
%% ------------------------------------------------------------------------
%% vsplit_phase3 DONE
%% ------------------------------------------------------------------------

algo({algo_done, WId, NumMsgsActive}, 
              #state{step = Step, workers = Workers, 
                     algo_sub_state = A} = State) ->
  {NewWorkers, NextState} = 
    update_workers(algo, post_algo, Step, Workers, WId),  
  {next_state, NextState, State#state{workers = NewWorkers, 
                                      algo_sub_state = A + NumMsgsActive}}.


post_algo({post_algo_done, WId, _WData}, 
              #state{step = Step, workers = Workers} = State) ->
  {NewWorkers, NextState} = 
    update_workers(post_algo, check_algo_finish, Step, Workers, WId),    
  case NextState of
    check_algo_finish ->
      {next_state, check_algo_finish, 
       State#state{workers = NewWorkers}, 0};
    _ ->
      {next_state, NextState, State#state{workers = NewWorkers}}
  end.


check_algo_finish(timeout, #state{step = Step, max_steps = MaxSteps, 
                                  algo_sub_state = A,
                                  workers = {Workers, []}} = State) ->
  {NextState, NextStep} = 
    case Step < MaxSteps of
      true -> 
        case A > 0 of
          true -> {algo, Step + 1};
          _ -> {store_result, Step}
        end;
      _ -> {store_result, Step}
    end,
  NewWorkers = notify_workers2(Workers, NextState, NextStep),
  {next_state, NextState, State#state{workers = {NewWorkers, []}, 
                                      algo_sub_state = 0,
                                      step = NextStep}}.


store_result({store_result_done, WId, _WData}, 
              #state{workers = Workers} = State) ->
  {NewWorkers, NextState} = 
    update_workers(store_result, end_state, bla, Workers, WId),
  {next_state, NextState, State#state{workers = NewWorkers}}.

end_state(_Event, State) ->
  {next_state, end_state, State}.

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
update_workers(CurrentState, NextState, ExtraInfo, 
               {Waiting, Finished}, WId) ->
  ?DEBUG("~nMaster Recvd Event...", [{current, CurrentState}, 
                                     {next, NextState}, 
                                     {workers, {Waiting, Finished}},
                                     {worker, WId}]),
  {NewWaiting, NewFinished} = 
    lists:foldl(
      fun({N, W, P, R, _}, {Wait, Done}) when W =:= WId -> 
          {Wait, [{N, WId, P, R, NextState}|Done]};
         (WInfo, {Wait, Done}) -> {[WInfo|Wait], Done}
      end, {[], Finished}, Waiting),
  ?DEBUG("Master New State...", [{current, CurrentState}, 
                                     {next, NextState}, 
                                     {workers, {NewWaiting, NewFinished}},
                                     {worker, WId}]),
  case NewWaiting of
    [] -> 
      ?DEBUG("~nMaster Shifting States...", [{current, CurrentState}, 
                                             {next, NextState}]),
      notify_workers(NewFinished, NextState, ExtraInfo),
      {{NewFinished, NewWaiting}, NextState};
    _ -> {{NewWaiting, NewFinished}, CurrentState}
  end.
      

notify_workers(_, check_algo_finish, _) -> void;
notify_workers(Workers, NextState, ExtraInfo) ->
  lists:foreach(
    fun({Node, _, WPid, _, _}) -> 
        rpc:call(Node, gen_fsm, send_event, 
                 [WPid, {goto_state, NextState, ExtraInfo}])
    end, Workers).  


notify_workers2(Workers, NextState, ExtraInfo) ->
  lists:foldl(
    fun({Node, WId, WPid, R, _}, Ws) -> 
        rpc:call(Node, gen_fsm, send_event, 
                 [WPid, {goto_state, NextState, ExtraInfo}]),
        [{Node, WId, WPid, R, NextState}|Ws]
    end, [], Workers).  


name(StrName) ->
  list_to_atom("master_" ++ StrName). 

start_workers(JobId, MasterInfo, Partitions, AlgoFun, CombineFun) ->
  PartLen = length(Partitions),
  lists:foldl(
    fun(Part, Workers) ->
        WId = length(Workers) + 1,
        Node = phoebus_utils:map_to_node(JobId, WId),
        %% TODO : Make Async
        %% [{Node, wId, wPid, wMonRef}]
        {ok, WPid} = 
          rpc:call(Node, phoebus_worker, start_link, 
                   [{JobId, WId}, PartLen, MasterInfo, Part, 
                    AlgoFun, CombineFun]),
        MRef = erlang:monitor(process, WPid),
        [{Node, WId, WPid, MRef, vsplit_phase1}|Workers]
    end, [], Partitions).
