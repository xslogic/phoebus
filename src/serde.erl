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
-module(serde).
-author('Arun Suresh <arun.suresh@gmail.com>').
-include("phoebus.hrl").

-export([serialize_rec/2, 
         deserialize_rec/2,
         deserialize_stream/3]).


deserialize_rec(_, <<>>) -> null;
deserialize_rec(Type, Line) ->
  BSize = size(Line) - 1,
  <<Rest:BSize/binary, Last/binary>> = Line,
  case Last of
    <<$\r>> -> 
      case Type of
        vertex ->
          deserialize_rec(vertex, Line, #vertex{}, [], <<>>, vname);
        msg ->
          deserialize_rec(msg, Line, {null, null}, [], <<>>, vname)
      end;
    <<$\n>> -> deserialize_rec(Type, <<Rest/binary, $\r>>);
    _ -> incomplete
  end.


deserialize_stream(Type, Prefix, Binary) ->
  lists:foldl(
    fun(BinLine, {AccRecs, Buff}) ->
        L = <<Buff/binary, BinLine/binary>>,
        case deserialize_rec(Type, L) of
          null -> {AccRecs, <<>>};
          incomplete -> {AccRecs, L};
          Rec -> {[Rec|AccRecs], <<>>}
        end
    end, {[], Prefix}, re:split(Binary, "\n")).


serialize_rec(vertex, {VName, VVal, EList}) ->  
  lists:concat([VName, "\t",
                VVal, "\t", serialize_edge_rec(EList, [])]);
serialize_rec(msg, {VName, Msgs}) ->
  VName ++ "\t" ++ serialize_msgs(Msgs) ++ "\r\n".


%%%===================================================================
%%% Internal functions
%%%===================================================================
serialize_edge_rec([], Done) ->
  Done ++ "\r\n";
serialize_edge_rec([{EVal, VName}|Rest], Done) ->
  serialize_edge_rec(Rest, 
                     lists:concat([EVal, "\t", VName, "\t", Done])).

serialize_msgs(Msgs) ->
  lists:concat([M ++ "\t" || M <- Msgs]).

  
%% vid \t vname \t vstate \t vval \t [eval \t tvid\t tvname \t].. \r\n
deserialize_rec(vertex, <<$\r, _/binary>>, V, EList, _, _) ->
  {V#vertex.vertex_name, V#vertex.vertex_value, EList};
deserialize_rec(vertex, <<$\t, Rest/binary>>, V, EList, Buffer, vname) ->
  VName = binary_to_list(Buffer),
  deserialize_rec(vertex, Rest, V#vertex{vertex_name = VName}, 
                  EList, <<>>, vval);
deserialize_rec(vertex, <<$\t, Rest/binary>>, V, EList, Buffer, vval) ->
  VVal = binary_to_list(Buffer),
  deserialize_rec(vertex, Rest, V#vertex{vertex_value = VVal}, 
                  EList, <<>>, eval);
deserialize_rec(vertex, <<$\t, Rest/binary>>, V, EList, Buffer, eval) ->
  deserialize_rec(vertex, Rest, V, EList, <<>>, 
                  {tvname, binary_to_list(Buffer)});
deserialize_rec(vertex, <<$\t, Rest/binary>>, V, EList, 
                Buffer, {tvname, EVal}) ->
  deserialize_rec(vertex, Rest, V, [{EVal, binary_to_list(Buffer)}|EList], 
                  <<>>, eval);
deserialize_rec(vertex, <<X, Rest/binary>>, V, EList, Buffer, Token) ->
  deserialize_rec(vertex, Rest, V, EList, <<Buffer/binary, X>>, Token);

deserialize_rec(msg, <<$\r, _/binary>>, Msg, _, _, _) -> Msg;
deserialize_rec(msg, <<$\t, Rest/binary>>, {_, _}, [], Buffer, vname) ->
  VName = binary_to_list(Buffer),
  deserialize_rec(msg, Rest, {VName, []}, [], <<>>, vmsg);
deserialize_rec(msg, <<$\t, Rest/binary>>, {VN, L}, [], Buffer, vmsg) ->
  Msg = binary_to_list(Buffer),
  deserialize_rec(msg, Rest, {VN, [Msg|L]}, [], <<>>, vmsg);
deserialize_rec(msg, <<X, Rest/binary>>, V, [], Buffer, Token) ->
  deserialize_rec(msg, Rest, V, [], <<Buffer/binary, X>>, Token).
