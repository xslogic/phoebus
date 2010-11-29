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
-module(algos).
-author('Arun Suresh <arun.suresh@gmail.com>').

%% API
-export([shortest_path/3, create_binary_tree/3]).

%%%===================================================================
%%% API
%%%===================================================================  
%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------
shortest_path({VName, VValStr, EList}, LargestTillNow, InMsgs) ->
  io:format("~n[~p]Recvd msgs : ~p : Aggregate : ~p ~n", 
            [VName, InMsgs, LargestTillNow]),
  LTN = list_to_integer(LargestTillNow),
  NewLTN = 
    lists:foldl(
      fun({_, TV}, OldLTN) ->
          TestLTN = list_to_integer(TV),
          case TestLTN > OldLTN of
            true -> TestLTN;
            _ -> OldLTN
          end
      end, LTN, [{0, VName}|EList]),
  {VInfo, O, NAgg, S} = 
    case VName of
      "1" ->
        OutMsgs = [{TV, VName} || {_, TV} <- EList],
        {{VName, "_", EList}, OutMsgs, integer_to_list(NewLTN), hold};
      _ ->
        case InMsgs of
          [] -> {{VName, "inf", EList}, [], LargestTillNow, hold};
          _ ->
            StartVal = case VValStr of VName -> "inf"; _ -> VValStr end,
            Shortest =
              lists:foldl(
                fun(Msg, CurrSh) ->
                    Split = re:split(Msg, ":", [{return, list}]),
                    case (["inf"] =:= CurrSh) or 
                      (length(Split) < length(CurrSh)) of
                      true -> Split;
                      _ -> CurrSh
                    end
                end, re:split(StartVal, ":", [{return, list}]),
                InMsgs),
            NewVVal = string:join(Shortest, ":"),
            OutMsgs =
              [{TV, VName ++ ":" ++ NewVVal} || {_, TV} <- EList],
            {{VName, NewVVal, EList}, OutMsgs, integer_to_list(NewLTN), hold}
        end
    end,                            
  io:format("[~p]Sending msgs : ~p ~n", [VName, O]),
  {VInfo, O, NAgg, S}.


create_binary_tree(Dir, NumFiles, NumRecs) ->
  TargetDir = 
    case lists:last(Dir) of
      $/ -> ok = worker_store:mkdir_p(Dir), Dir;
      _ -> ok = worker_store:mkdir_p(Dir ++ "/"), Dir ++ "/"
    end,    
  FDs = 
    lists:foldl(
      fun(X, AFDs) ->
          {ok, FD} =
            file:open(TargetDir ++ "infile" ++ integer_to_list(X),
                      [write]),
          [FD|AFDs]
      end, [], lists:seq(1, NumFiles)),
  Fn = fun(N) -> integer_to_list(N) end,
  lists:foldl(
    fun(N, [F|Rest]) ->
        Line = 
          lists:concat(
            [Fn(N),"\t",Fn(N),"\t", 
             "1\t",Fn(N*2),"\t1\t",Fn((N*2)+1),"\t\r\n"]),
        file:write(F, Line),
        Rest ++ [F]
    end, FDs, lists:seq(1, NumRecs)),
  lists:foreach(fun(FD) -> file:close(FD) end, FDs).          


%%%===================================================================
%%% Internal functions
%%%===================================================================
