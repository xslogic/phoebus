%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created : 20 Sep 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(phoebus_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application is started using
%% application:start/[1,2], and should start the processes of the
%% application. If the application is structured according to the OTP
%% design principles as a supervision tree, this means starting the
%% top supervisor of the tree.
%%
%% @spec start(StartType, StartArgs) -> {ok, Pid} |
%%                                      {ok, Pid, State} |
%%                                      {error, Reason}
%%      StartType = normal | {takeover, Node} | {failover, Node}
%%      StartArgs = term()
%% @end
%%--------------------------------------------------------------------
start(_StartType, _StartArgs) ->
  error_logger:tty(false),
  BaseDir = get_log_base(),
  error_logger:logfile({open, BaseDir ++ 
                          atom_to_list(erlang:node()) ++ ".log"}),
  ets:new(table_mapping, [named_table, public]),
  ets:new(worker_registry, [named_table, public]),
  ets:new(all_nodes, [named_table, public]),
  case phoebus_sup:start_link() of
    {ok, Pid} ->
      {ok, Pid};
    Error ->
      Error
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application has stopped. It
%% is intended to be the opposite of Module:start/2 and should do
%% any necessary cleaning up. The return value is ignored.
%%
%% @spec stop(State) -> void()
%% @end
%%--------------------------------------------------------------------
stop(_State) ->
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_log_base() ->
  BaseDir = phoebus_utils:get_env(log_base, "/tmp/phoebus_logs/"),
  worker_store:mkdir_p(BaseDir),
  case lists:last(BaseDir) of
    $/ -> BaseDir;
    _ -> BaseDir ++ "/"
  end.
