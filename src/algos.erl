%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created :  5 Oct 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(algos).

%% API
-export([shortest_path/2]).

%%%===================================================================
%%% API
%%%===================================================================  
%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------
shortest_path({VName, VValStr, EList}, InMsgs) ->
  io:format("~n[~p]Recvd msgs : ~p ~n", [VName, InMsgs]),
  {VInfo, O, S} = 
    case VName of
      "1" ->
        OutMsgs = [{TV, VName} || {_, TV} <- EList],
        {{VName, "_", EList}, OutMsgs, hold};
      _ ->
        case InMsgs of
          [] -> {{VName, "inf", EList}, [], hold};
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
            {{VName, NewVVal, EList}, OutMsgs, hold}
        end
    end,                            
  io:format("[~p]Sending msgs : ~p ~n", [VName, O]),
  {VInfo, O, S}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
