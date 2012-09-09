-module(yokozuna).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(INDEX_S, "fruit").
-define(INDEX_B, <<"fruit">>).
-define(NUM_KEYS, 10000).
-define(SUCCESS, 0).

confirm() ->
    random:seed(now()),
    Nodes = rt:deploy_nodes(4),
    Cluster = join_three(Nodes),
    wait_for_joins(Cluster),
    setup_indexing(Cluster),
    load_data(Cluster),
    Ref = async_query(Cluster),
    Cluster2 = join_rest(Cluster, Nodes),
    check_status(wait_for(Ref)),
    KeysDeleted = delete_some_data(Cluster2, reap_sleep()),
    verify_deletes(Cluster2, KeysDeleted),
    pass.

async_query(Cluster) ->
    lager:info("Run async query against cluster ~p", [Cluster]),
    Hosts = host_entries(rt:connection_info(Cluster)),
    Concurrent = length(Hosts),
    Expect = [{"apple", ?NUM_KEYS}],
    Operations = [{{search,E},1} || E <- Expect],
    Cfg = [{mode, {rate,8}},
           {duration, 2},
           {concurrent, Concurrent},
           {driver, basho_bench_driver_http_raw},
           {operations, Operations},
           {http_raw_ips, Hosts},
           {http_solr_path, "/search/" ++ ?INDEX_S},
           {http_raw_path, "/riak/" ++ ?INDEX_S},
           {shutdown_on_error, true}],

    File = "bb-query-fruit-" ++ ?INDEX_S,
    rt_utils:write_terms(File, Cfg),
    run_bb(async, File).

check_status({Status,_}) ->
    ?assertEqual(?SUCCESS, Status).

create_index(Node, Index) ->
    lager:info("Creating index ~s [~p]", [Index, Node]),
    rpc:call(Node, yz_index, create, [Index]).

delete_key(Cluster, Key) ->
    Node = select_random(Cluster),
    lager:info("Deleting key ~s", [Key]),
    {ok, C} = riak:client_connect(Node),
    C:delete(?INDEX_B, list_to_binary(Key)).

delete_some_data(Cluster, ReapSleep) ->
    Num = random:uniform(100),
    lager:info("Deleting ~p keys", [Num]),
    Keys = [integer_to_list(random:uniform(?NUM_KEYS))
            || _ <- lists:seq(1, Num)],
    [delete_key(Cluster, K) || K <- Keys],
    lager:info("Sleeping ~ps to allow for reap", [ReapSleep]),
    timer:sleep(timer:seconds(ReapSleep)),
    Keys.

host_entries(ClusterConnInfo) ->
    [proplists:get_value(http, I) || {_,I} <- ClusterConnInfo].

install_hook(Node, Index) ->
    lager:info("Install index hook on bucket ~s [~p]", [Index, Node]),
    rpc:call(Node, yz_kv, install_hook, [Index]).

join_three(Nodes) ->
    [NodeA|Others] = All = lists:sublist(Nodes, 3),
    [rt:join(Node, NodeA) || Node <- Others],
    All.

join_rest([NodeA|_]=Cluster, Nodes) ->
    ToJoin = Nodes -- Cluster,
    [begin rt:join(Node, NodeA) end || Node <- ToJoin],
    Nodes.

load_data(Cluster) ->
    lager:info("Load data onto cluster ~p", [Cluster]),
    Hosts = host_entries(rt:connection_info(Cluster)),
    Cfg = [{mode,max},
           {duration,5},
           {concurrent, 3},
           {driver, rs_bb_driver},
           {rs_index_path, "/riak/fruit"},
           {rs_ports, Hosts},
           {pb_ports, []},
           {key_generator, {function, rs_bb_driver, fruit_key_val_gen, [?NUM_KEYS]}},
           {operations, [{load_fruit, 1}]}],
    File = "bb-load-fruit-" ++ ?INDEX_S,
    rt_utils:write_terms(File, Cfg),
    run_bb(sync, File).

reap_sleep() ->
    %% NOTE: This is hardcoded to 5s now but if this test ever allows
    %%       configuation of deletion policy then this should be
    %%       calculated.
    10.

run_bb(Method, File) ->
    Fun = case Method of
              sync -> cmd;
              async -> spawn_cmd
          end,
    rt:Fun("$BASHO_BENCH/basho_bench " ++ File).

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

setup_indexing(Cluster) ->
    Node = select_random(Cluster),
    ok = create_index(Node, ?INDEX_S),
    ok = install_hook(Node, ?INDEX_B),
    %% Give Solr time to build index
    timer:sleep(5000).

verify_deletes(Cluster, KeysDeleted) ->
    Hosts = host_entries(rt:connection_info(Cluster)),
    Concurrent = length(Hosts),
    Expect = [{"apple", ?NUM_KEYS - length(KeysDeleted)}],
    Operations = [{{search,E},1} || E <- Expect],
    Cfg = [{mode, max},
           {duration, 1},
           {concurrent, Concurrent},
           {driver, basho_bench_driver_http_raw},
           {operations, Operations},
           {http_raw_ips, Hosts},
           {http_solr_path, "/search/" ++ ?INDEX_S},
           {http_raw_path, "/riak/" ++ ?INDEX_S},
           {shutdown_on_error, true}],

    File = "bb-verify-deletes-" ++ ?INDEX_S,
    rt_utils:write_terms(File, Cfg),
    check_status(run_bb(sync, File)).

wait_for(Ref) ->
    rt:wait_for_cmd(Ref).

wait_for_joins(Cluster) ->
    rt:wait_until_nodes_ready(Cluster),
    rt:wait_until_no_pending_changes(Cluster).
