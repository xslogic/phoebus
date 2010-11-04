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
-module(external_store_hdfs).
-author('Arun Suresh <arun.suresh@gmail.com>').

-behaviour(external_store).
-include("phoebus.hrl").
-include("thriftHadoopFileSystem_thrift.hrl").

%% API
-export([init/1,
         partition_input/1,
         read_vertices/2,
         store_vertices/2,
         destroy/1]).

%%%===================================================================
%%% API
%%%===================================================================
init([$h, $d, $f, $s, $:, $/, $/ | Rest] = URI) ->
  IsDir = external_store:check_dir(URI),  
  {ok, TFactory} = 
    thrift_socket_transport:new_transport_factory(
      phoebus_utils:get_env(thriftfs_hostname, "localhost"),
      phoebus_utils:get_env(thriftfs_port, 8899), []),
  {ok, PFactory} = 
    thrift_binary_protocol:new_protocol_factory(TFactory, []),
  {ok, Protocol} = PFactory(),
  {ok, Client} = 
    thrift_client:new(Protocol, thriftHadoopFileSystem_thrift),
  {NameNode, Port, AbsPath} = parse_path(Rest),    
  %% TODO : check if there is a registered thrift server for this NN
  {true, [{uri, URI}, {namenode, NameNode}, {port, Port}, 
          {abs_path, AbsPath}, {type, hdfs}, {is_dir, IsDir}, 
          {thrift_client, Client}]};
init(_) -> {false, []}.

  
partition_input(State) ->
  case proplists:get_value(is_dir, State) of
    true ->
      Client = proplists:get_value(thrift_client, State),
      AbsPath = proplists:get_value(abs_path, State),
      {_, {ok, FSs}} = thrift_client:call(Client, listStatus, 
                         [#pathname{pathname = AbsPath}]),      
      Files = 
        lists:foldl(
          fun(#fileStatus{isdir = false, path = P}, Acc) 
              when is_binary(P) ->
              [binary_to_list(P)|Acc];
             (#fileStatus{isdir = false, path = P}, Acc) 
              when is_list(P) ->
              [P|Acc];
             (_, Acc) -> Acc
          end, [], FSs),
      {ok, Files, State};
    _ ->
      ?ERROR("URI not a directory", 
             [{uri, proplists:get_value(uri, State)}]),
      destroy(State),
      {error, enotdir}
  end.

read_vertices(State, Recvr) ->
  case proplists:get_value(is_dir, State) of
    false ->
      start_reading(proplists:get_value(type, State), 
                    proplists:get_value(abs_path, State), Recvr, State);
    _ ->
      ?ERROR("URI is a directory", [{uri, proplists:get_value(uri, State)}]),
      destroy(State),
      {error, eisdir}
  end.  

store_vertices(State, Vertices) ->
  case proplists:get_value(is_dir, State) of
    false ->
      {TC, TH, NewState} = 
        case proplists:get_value(thrift_handle, State) of
          undefined ->
            Client = proplists:get_value(thrift_client, State),
            AbsPath = proplists:get_value(abs_path, State),
            thrift_client:call(Client, rm, 
                               [#pathname{pathname = AbsPath}, false]),
            {_, {ok, THandle}} = 
              thrift_client:call(Client, create, 
                                 [#pathname{pathname = AbsPath}]),
            {Client, THandle, [{thrift_handle, THandle}|State]};
          THandle -> 
            {proplists:get_value(thrift_client, State), THandle, State}
        end,
      lists:foreach(
        fun(V) -> 
            thrift_client:call(TC, write, 
                               [TH, serde:serialize_rec(vertex, V)]) 
        end, Vertices),
      NewState;
    _ ->
      ?ERROR("URI is a directory", [{uri, proplists:get_value(uri, State)}]),
      destroy(State),
      {error, eisdir}
  end.  

destroy(State) ->
  case proplists:get_value(thrift_client, State) of
    undefined -> ok;
    Client -> 
      case proplists:get_value(thrift_handle, State) of
        undefined -> void;
        THandle ->
          try
            thrift_client:call(Client, close, [THandle])
          catch
            E1:E2 ->
              ?WARN("Error while closing hdfs file handle store..", 
                    [{error, E1}, {reason, E2}]),
              ok
          end
      end,
      try
        thrift_client:close(Client)
      catch
        E11:E21 ->
          ?WARN("Error while closing hdfs store..", 
                 [{error, E11}, {reason, E21}]),
          ok
      end
  end.

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_reading(hdfs, File, Recvr, StoreState) ->
  RPid = spawn(fun() -> reader_loop({init, File}, Recvr, 
                                    {StoreState, <<>>, []}) end),
  {ok, RPid, StoreState}.


reader_loop({init, AbsPath}, Pid, {StoreState, X, Y}) ->
  Client = proplists:get_value(thrift_client, StoreState),
  {_, {ok, THandle}} = 
    thrift_client:call(Client, open, [#pathname{pathname = AbsPath}]),
  NewSState = 
    [{thrift_handle, THandle} | StoreState],
  reader_loop({Client, THandle, 0}, Pid, {NewSState, X, Y});
reader_loop({Client, THandle, OffSet}, Pid, {StoreState, Rem, Buffer}) ->
  ByteSize = phoebus_utils:get_env(hdfs_read_size, 16384),
  {_, {ok, Data}} = 
    thrift_client:call(Client, read, [THandle, OffSet, ByteSize]),
  {Vs, NewRem} = serde:deserialize_stream(vertex, Rem, Data),
  NewBuffer = Vs ++ Buffer,
  case size(Data) < ByteSize of
    true ->
      gen_fsm:send_event(Pid, {vertices_done, NewBuffer, self(), StoreState}),
      thrift_client:close(Client, close, [THandle]);
    _ ->    
      case length(NewBuffer) > 100 of
        true ->
          gen_fsm:send_event(
            Pid, {vertices, NewBuffer, self(), StoreState}),
          reader_loop({Client, THandle, OffSet + ByteSize}, Pid, 
                      {StoreState, NewRem, []});
        _ ->
          reader_loop({Client, THandle, OffSet + ByteSize}, Pid, 
                      {StoreState, NewRem, NewBuffer})
      end
  end.

parse_path(Path) ->
  {match, [NN, P, AbsPath]} = 
    re:run(Path, "([^:]*):([^/]*)(.*)", [{capture, [1, 2, 3], list}]),
  {NN, list_to_integer(P), AbsPath}.
  
