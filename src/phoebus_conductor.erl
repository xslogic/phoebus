%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created : 21 Sep 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(phoebus_conductor).
-include("phoebus.hrl").
-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {current_step = 0, state = setup, workers = [], 
                vertices = 0, conf}).

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
start_link(Conf) ->
  %% TODO : handle no name..
  Name = name(proplists:get_value(name, Conf, noname)),
  gen_server:start_link(?MODULE, [[{conductor_name, Name}|Conf]], []).

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
init([Conf]) ->  
  {ok, SS} = phoebus_source:init(proplists:get_value(input_dir, Conf)),
  {ok, Partitions, SS2} = phoebus_source:partition_input(SS),
  phoebus_source:destroy(SS2),
  JobId = phoebus_utils:job_id(),
  %% NOTE: Workers must be of the form [{Node, wId, wPid, wMonRef, wStep}]
  %% Mapping from VertexId to WorkerId
  Workers = start_workers(JobId, {erlang:node(), self()}, Partitions),
  ?DEBUG("Conductor started..", [{conf, Conf}, {node, erlang:node()}]),
  {ok, #state{state = awaiting_worker_init, 
              workers = Workers, conf = Conf}}.

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
handle_info({vertices_read, Vs, _From}, #state{vertices = NV} = State) ->
  ?DEBUG("Vertices Uncovered..", [{num, NV + Vs}]),
  {noreply, State#state{vertices = NV + Vs}};

handle_info({awaiting_transfer_order, Vs, {WId, WPid}}, 
            #state{vertices = NV, 
                   workers = Workers} = State) ->
  ?DEBUG("Vertices Uncovered..", [{num, NV + Vs}]),
  ?DEBUG("Worker finished init..", [{worker_id, WId}, {worker_pid, WPid}]),
  NewWorkers = 
    case lists:keytake(WId, 2, Workers) of
      {value, {Node, WId, WPid, MRef, -2}, W2} ->
        [{Node, WId, WPid, MRef, -1}|W2];
      _ -> Workers
    end,
  {noreply, State#state{vertices = NV + Vs, workers = NewWorkers}, 1000};

handle_info(execute_file_transfers, 
            #state{workers = Workers} = State) ->
  ?DEBUG("Asking Workers to transfer files..", [{workers, Workers}]),
  lists:foreach(
    fun({_Node, _WId, WPid, _MRef, _WStep}) ->
        WPid ! transfer_files
    end, Workers),
  {noreply, State#state{state = awaiting_finish_file_transfer}};

handle_info(timeout, #state{state = setup, workers = Workers} = State) ->
  case lists:keyfind(-2, 5, Workers) of
    false -> 
      self() ! start_transfers,
      {noreply, State};
    {Node, WId, _, _, _} -> 
      ?DEBUG("Asking Worker for finish loading..", 
             [{node, Node}, {worker, WId}]),
      {noreply, State, 1000}
  end;


handle_info(Msg, State) ->
  ?DEBUG("Got Msg..", [{msg, Msg}]),
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
name(StrName) ->
  list_to_atom("conductor_" ++ StrName). 

start_workers(JobId, CRef, Partitions) ->
  PartLen = length(Partitions),
  lists:foldl(
    fun(Part, Workers) ->
        WId = length(Workers) + 1,
        Node = phoebus_utils:map_to_node(JobId, WId),
        %% TODO : Make Async
        %% [{Node, wId, wPid, wMonRef}]
        {ok, WPid} = 
          rpc:call(Node, phoebus_worker, start_link, 
                   [{JobId, WId}, PartLen, CRef, Part]),
        MRef = erlang:monitor(process, WPid),
        [{Node, WId, WPid, MRef, -2}|Workers]
    end, [], Partitions).

  
