%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created :  6 Oct 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(table_manager).

-behaviour(gen_server).

%% API
-export([start_link/1, acquire_table/2, 
         release_table/1, lookup_table/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

%%%===================================================================
%%% API
%%%===================================================================
acquire_table(JobId, WId) ->
  gen_server:call(?SERVER, {acquire, JobId, WId}).

release_table(Table) ->
  gen_server:cast(?SERVER, {release, Table}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(PoolSize) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [PoolSize], []).

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
init([PoolSize]) ->
  ets:new(table_pool, [named_table, public]),
  lists:foreach(
    fun(X) ->
        Name = list_to_atom("worker_table_" ++ integer_to_list(X)),
        ets:insert(table_pool, {Name, none, none})
    end, lists:seq(1, PoolSize)),
  {ok, []}.

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
handle_call({acquire, JobId, WId}, {Pid, _} = From, State) ->
  MRef = erlang:monitor(process, Pid),
  case ets:select(table_pool, [{{'$1', none, '_'}, [], ['$$']}], 1) of
    {[[T]], _} ->
      ets:insert(table_pool, {T, {JobId, WId}, MRef}),
      {reply, T, State};
    _ ->
      {noreply, [{From, {JobId, WId}, MRef}|State]}
  end.

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
handle_cast({release, Table}, State) ->
  case State of
    [] -> 
      case ets:lookup(table_pool, Table) of
        [{Table, CInfo, MRef}] ->
          erlang:demonitor(MRef),
          ets:insert(table_pool, {Table, none, none}),
          ets:delete(table_mapping, CInfo);
        _ -> void
      end,
      {noreply, []};
    _ -> 
      [{From, CInfo, MRef}|Rest] = lists:reverse(State),
      ets:insert(table_pool, {Table, CInfo, MRef}),
      gen_server:reply(From, Table),
      {noreply, lists:reverse(Rest)}
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
handle_info({'DOWN', MRef, _, _, _}, State) ->
  case ets:select(table_pool, [{{'$1', '_', MRef}, [], ['$_']}]) of
    [{Table, CInfo, MRef}] ->
      case State of
        [] ->
          erlang:demonitor(MRef),
          ets:insert(table_pool, {Table, none, none}),
          ets:delete(table_mapping, CInfo),
          {noreply, []};
        _ ->
          [{From, CInfo, MRef2}|Rest] = lists:reverse(State),
          ets:insert(table_pool, {Table, CInfo, MRef2}),
          gen_server:reply(From, Table),
          {noreply, lists:reverse(Rest)}
      end;
    _ -> {noreply, State}
  end;

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
lookup_table(JobId, WId) ->
  case ets:lookup(table_mapping, {JobId, WId}) of
    [{_, T}] -> {table, T};
    _ -> none
  end.
      
