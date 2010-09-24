%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created : 22 Sep 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(phoebus_worker).

-behaviour(gen_server).
-include("phoebus.hrl").
%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 
-define(SOURCE_TIMEOUT(), phoebus_utils:get_env(source_timeout, 120000)).
-define(CONDUCTOR_TIMEOUT(), phoebus_utils:get_env(conductor_timeout, 300000)).

-record(state, {worker_info, num_workers, conductor_info, state, 
                vnode_state, vnode_list = [], step = 0}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(WorkerInfo, NumWorkers, CInfo, InputChunk) ->
  gen_server:start_link(?MODULE, [WorkerInfo, NumWorkers,
                                  CInfo, InputChunk], []).

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
init([{JobId, WId} = WorkerInfo, NumWorkers, {CNode, CPid}, Partition]) ->
  CMonRef = erlang:monitor(process, CPid),
  {last_step, LastStep} = worker_store:init(JobId, WId),
  WorkerState = 
  case (LastStep < 0) of
    true -> {init, Partition};
    _ -> {restore_state, LastStep}
  end,
  {ok, #state{worker_info = WorkerInfo, num_workers = NumWorkers,
              conductor_info = {CNode, CPid, CMonRef}, 
              state = WorkerState}, 0}.

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
handle_cast(_Msg, State) ->
  {noreply, State}.

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
handle_info(timeout, #state{state = {init, Partition}} = State) ->
  {ok, SS} = phoebus_source:init(Partition),
  {ok, RefPid, SS2} = phoebus_source:read_vertices_start(SS),
  %% Something should happen in 2 mins..
  {noreply, State#state{state = {reading_partition, {RefPid, SS2, []}}}, 
   ?SOURCE_TIMEOUT()};

handle_info(timeout, #state{worker_info = {JobId, WId},
                            conductor_info = {_, CPid, _},
                            state = {restore_state, LastStep}} = State) ->
  case LastStep of
    0 ->
      CPid ! {awaiting_conductor, -1, self()},
      {noreply, State#state{state = awaiting_conductor}, ?CONDUCTOR_TIMEOUT()};
    _ ->
      {MRef2StateDict, V2PidList} = start_workers(JobId, WId, LastStep),
      CPid ! {awaiting_conductor, length(V2PidList), self()},
      {noreply, State#state{state = awaiting_conductor, 
                            vnode_state = MRef2StateDict,
                            step = LastStep,
                            vnode_list = V2PidList}, ?CONDUCTOR_TIMEOUT()}
  end;
      

handle_info(timeout, #state{worker_info = WInfo,
                            state = {reading_partition, _}} = State) ->
  ?DEBUG("No response from source... Shutting Down...", 
         [{worker_info, WInfo},
          {source_timeout, ?SOURCE_TIMEOUT()}]),
  {stop, timeout_expired, State};

handle_info(timeout, #state{worker_info = WInfo, 
                            state = awaiting_conductor} = State) ->
  ?DEBUG("No response from conductor... Shutting Down...", 
         [{worker_info, WInfo},
          {conductor_timeout, ?CONDUCTOR_TIMEOUT()}]),
  {stop, timeout_expired, State};


%% TODO : make sure source monitors this process..
handle_info({'DOWN', CMRef, _, _, _}, 
            #state{worker_info = WInfo,
                   conductor_info = {_, _, CMRef}} = State) ->
  ?DEBUG("Conductor Down...", [{worker_info, WInfo}]),
  {stop, conductor_down, State};  


handle_info({vertices, Vertices, RefPid, SS}, 
            #state{worker_info = WInfo, 
                   num_workers = NumWorkers,
                   conductor_info = {_, CPid, _},
                   state = {reading_partition, {RefPid, _, FDs}}} = State) ->
  NewFDs = handle_vertices(NumWorkers, WInfo, Vertices, 0, FDs),
  CPid ! {vertices_read, length(Vertices), self()},
  {noreply, State#state{state = {reading_partition, {RefPid, SS, NewFDs}}}, 
   ?SOURCE_TIMEOUT()};


handle_info({vertices_done, Vertices, RefPid, SS}, 
            #state{worker_info = {JobId, WId} = WInfo, 
                   num_workers = NumWorkers,
                   conductor_info = {_, CPid, _},
                   state = {reading_partition, {RefPid, SS, FDs}}} = State) ->
  NewFDs = handle_vertices(NumWorkers, WInfo, Vertices, 0, FDs),
  CPid ! {awaiting_conductor, length(Vertices), self()},
  phoebus_source:destroy(SS),
  lists:foreach(
    fun({_, FD}) -> worker_store:close_step_file(FD) end, NewFDs),
  {noreply, State#state{state = awaiting_conductor}, ?CONDUCTOR_TIMEOUT()};

handle_info(Info, #state{worker_info = WInfo} = State) ->
  ?DEBUG("Recieved unknown event...", [{message, Info}, {worker_info, WInfo}]),
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

handle_vertices(NumWorkers, {JobId, MyWId}, Vertices, Step, FDs) ->
  lists:foldl(
    fun(#vertex{vertex_id = VId} = Vertex) ->
        {Node, WId} = phoebus_utils:vertex_owner(JobId, VId, NumWorkers),
        worker_store:store_vertex(Vertex, {Node, {JobId, MyWId, WId}}, Step, FDs)
    end, FDs, Vertices).  

%% TODO : implement
start_workers(_JobId, _WId, _Step) ->
  void.

start_worker(_JobId, _WId, _VId, _Step, _FPid) ->
  void.
