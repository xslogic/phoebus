%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created : 30 Nov 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(path_find).

%% API
-export([generate_input/6, compute_fun/3, aggregate_fun/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% NumBlocks = Blocks in a city
%% Density = People per block
%% Locs = Locs per block edge
%%--------------------------------------------------------------------
generate_input(Dir, NumFiles, NumBlocks, Density, Locs, {Min, Max}) ->
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
  People = [[[Pre|integer_to_list(Suf)] || Suf <- lists:seq(1, Density)]
            || Pre <- lists:seq($A, $A + NumBlocks - 1)],
  Fn = fun(N) -> integer_to_list(N) end,
  lists:foldl(
    fun(P, [F|Rest]) ->
        Num = random:uniform(Max - Min + 1) + (Min - 1),
        %% PLocs = [[$L, $O, $C | integer_to_list(random:uniform(Locs))] 
        %%      || _N <- lists:seq(1, Num)],
        Line = 
          lists:concat(
            [P,"\t",P,"\t"|[[$1,$\t,$L,$O,$C|Fn(random:uniform(Locs))] ++ "\t" 
                            || _N <- lists:seq(1, Num)]] ++ ["\t\r\n"]),
        file:write(F, Line),
        Rest ++ [F]
    end, FDs, lists:concat(People)),
  lists:foreach(fun(FD) -> file:close(FD) end, FDs).
  
  


aggregate_fun("done", _) -> "done";
aggregate_fun(_, "done") -> "done";
aggregate_fun(X, _) -> X.
  

compute_fun({VName, VName, []}, Agg, InMsgs) ->
  NewEdges = 
    lists:map(
      fun(M) -> [_, N] = re:split(M, ":", [{return, list}]), {"1", N} end, 
      InMsgs),
  NewVVal = "from_src=;from_dest=",
  {{VName, NewVVal, NewEdges}, [], Agg, hold};
compute_fun({VName, VName, EList}, Agg, _) ->
  OutMsgs = [{TV, "discover:" ++ VName} || {_, TV} <- EList],
  NewVVal = "from_src=;from_dest=",
  {{VName, NewVVal, EList}, OutMsgs, Agg, active};
compute_fun(CurrState, "done" = Agg, _) ->
  %% io:format("~n Got done [~p] ~n", [CurrState]),
  {CurrState, [], Agg, hold};
compute_fun({VName, VVal, EList} = _CurrState, Agg, []) ->
  %% io:format("~n Am here 1 [~p] ~n", [CurrState]),
  [Src, Dest] = re:split(Agg, ":", [{return, list}]),
  OutMsgs = 
    case (Src =:= VName) orelse (Dest =:= VName) of
      true ->
        case VName of
          Src ->
            [{TV, "src_path=" ++ VName} || {_, TV} <- EList];
          _ ->
            [{TV, "dest_path=" ++ VName} || {_, TV} <- EList]
        end;
      _ -> []
    end,
  %% case OutMsgs of
  %%   [] -> void;
  %%   _ -> io:format("[~p] Sending First Messages : ~p ~n", [VName, OutMsgs])
  %% end,
  {{VName, VVal, EList}, OutMsgs, Agg, hold};
compute_fun({VName, VVal, EList} = _CurrState, Agg, InMsgs) ->
  %% io:format("~n Am here 2 [~p] ~n", [CurrState]),
  [TempSrcPath, TempDestPath] = re:split(VVal, ";", [{return, list}]),
  SplitFun = 
    fun(P) -> 
        [_, X] = re:split(P, "=", [{return, list}]),
        case X of
          [] -> "inf";
          _ -> re:split(X, ":", [{return, list}])
        end
    end,  
  {CurrSrcPath, CurrDestPath} = {SplitFun(TempSrcPath), SplitFun(TempDestPath)},
  {UpdatedSrcPath, UpdatedDestPath, NumS, NumD} =
    lists:foldl(
      fun([$s, $r, $c, $_, $p, $a, $t, $h, $= | Path], 
          {SSPath, SDPath, NS, ND}) ->
          IPathSplit = re:split(Path, ":", [{return, list}]),
          case (SSPath =:= "inf") orelse (length(IPathSplit) < SSPath) of
            true -> {IPathSplit, SDPath, NS + 1, ND};
            _ -> {SSPath, SDPath, NS + 1, ND}
          end;
         ([$d, $e, $s, $t, $_, $p, $a, $t, $h, $= | Path], 
          {SSPath, SDPath, NS, ND}) ->
          IPathSplit = re:split(Path, ":", [{return, list}]),
          case (SDPath =:= "inf") orelse (length(IPathSplit) < SDPath) of
            true -> {SSPath, IPathSplit, NS, ND + 1};
            _ -> {SSPath, SDPath, NS, ND + 1}
          end
      end, {CurrSrcPath, CurrDestPath, 0, 0}, InMsgs),
  [Src, Dest] = re:split(Agg, ":", [{return, list}]),
  CleanFun = fun("inf") -> []; (P) -> string:join(P, ":") end,
  {USPStr, UDPStr} = {CleanFun(UpdatedSrcPath), CleanFun(UpdatedDestPath)},
  case (Src =:= VName) andalso (NumD > 0) of
    true -> 
      {{VName, "full_path=" ++ VName ++ ":" ++ UDPStr, EList}, [], "done", hold};
    _ ->
      case (Dest =:= VName) andalso (NumS > 0) of
        true ->
          {{VName, "full_path=" ++ USPStr ++ ":" ++ VName, EList}, 
           [], "done", hold};
        _ ->
          case (UDPStr =/= []) andalso (USPStr =/= []) of
            true ->
              {{VName, 
                "full_path=" ++ USPStr ++ ":" ++ VName ++ ":" ++ UDPStr, EList}, 
               [], "done", hold};
            _ ->              
              NewVVal = "from_src=" ++ USPStr ++ ";from_dest=" ++ UDPStr, 
              DestMsgs = 
                case UpdatedDestPath of
                  CurrDestPath -> [];
                  _ -> 
                    lists:foldl(
                      fun({_, TV}, Acc) ->
                          [{TV, "dest_path=" ++ VName ++ ":" ++ UDPStr}|Acc]
                      end, [], EList)
                end,
              OutMsgs = 
                case UpdatedSrcPath of
                  CurrSrcPath -> DestMsgs;
                  _ -> 
                    lists:foldl(
                      fun({_, TV}, Acc) ->
                          [{TV, "src_path=" ++ USPStr ++ ":" ++ VName}|Acc]
                      end, DestMsgs, EList)
                end,
              {{VName, NewVVal, EList}, OutMsgs, Agg, hold}
          end
      end
  end.
            
%%%===================================================================
%%% Internal functions
%%%===================================================================
