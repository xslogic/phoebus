%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2010, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created : 22 Sep 2010 by Arun Suresh <>
%%%-------------------------------------------------------------------
-author('arun.suresh@gmail.com').

%% -define(DEBUG(Str, Args), io:format(Str ++ ":~n~p~n", [Args])).
%% -define(DEBUG(Str, Args), 
%%         error_logger:info_msg(Str ++ ":~n~p~n", [Args])).
-define(DEBUG(Str, Args), error_logger:info_report([Str|Args])).

-define(BASE_DIR(), phoebus_utils:get_env(store_dir, "/tmp/phoebus/")).
-define(JOB_DIR(JobId, WId), 
        ?BASE_DIR() ++ atom_to_list(erlang:node()) ++ "/" 
        ++ JobId ++ "/" 
        ++ integer_to_list(WId) ++ "/").

-define(LAST_STEP_FILE(JobId, WId), 
        ?JOB_DIR(JobId, WId) ++ "last_step").

-define(STEP_DIR(JobId, WId, Step), 
        ?JOB_DIR(JobId, WId) ++ integer_to_list(Step) ++ "/").
-define(STEP_VETEX_DATA(JobId, WId, Step, Idx), 
        ?STEP_DIR(JobId, WId, Step) ++ "vertex_data_" 
        ++ integer_to_list(Idx)).
-define(STEP_FLAG_DATA(JobId, WId, Step, Idx), 
        ?STEP_DIR(JobId, WId, Step) ++ "flag_" 
        ++ integer_to_list(Idx)).
-define(STEP_MSG_QUEUE(JobId, WId, Step, Idx), 
        ?STEP_DIR(JobId, WId, Step) ++ "msg_queue_"
        ++ integer_to_list(Idx)).


%% -define(EXT_MSG_DIR(JobId, WId, OWid, Step),
%%         ?STEP_DIR(JobId, WId, Step) ++ "not_mine/" ++ OWid ++ "/").

-define(MSG_TMP_FILE(JobId, WId, Step, OWid),
        ?STEP_DIR(JobId, WId, Step) ++ "ext_msgs_temp_" 
        ++ integer_to_list(OWid)).

%% -define(ESTEP_VETEX_DATA(JobId, WId, Step, Idx), 
%%         ?STEP_DIR(JobId, WId, Step) ++ "vertex_data_e" 
%%         ++ integer_to_list(Idx)).
%% -define(ESTEP_MSG_QUEUE(JobId, WId, Step, Idx), 
%%         ?STEP_DIR(JobId, WId, Step) ++ "msg_queue_e"
%%         ++ integer_to_list(Idx)).

-define(RSTEP_DIR(JobId, WId, Step, RWId), 
        ?JOB_DIR(JobId, WId) 
        ++ integer_to_list(Step) ++ "/"
        ++ "not_mine/"
        ++ integer_to_list(RWId) ++ "/").
-define(RSTEP_VETEX_DATA(JobId, WId, Step, RWId, Idx), 
        ?RSTEP_DIR(JobId, WId, Step, RWId) ++ "vertex_data_" 
        ++ integer_to_list(Idx)).
-define(RSTEP_MSG_QUEUE(JobId, WId, Step, RWId, Idx), 
        ?RSTEP_DIR(JobId, WId, Step, RWId) ++ "msg_queue_"
        ++ integer_to_list(Idx)).
                                                                          
 
-record(vertex, {vertex_id = nil, 
                 vertex_name, 
                 vertex_state = active, 
                 vertex_value, 
                 edge_list = []}).
