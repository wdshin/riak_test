-module(yokozuna).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(INDEX_S, "fruit").
-define(INDEX_B, <<"fruit">>).
-define(SUCCESS, 0).

confirm() ->
    Nodes = rt:deploy_nodes(4),
    Cluster = join_three(Nodes),
    timer:sleep(20000),
    setup_indexing(Cluster),
    load_data(Cluster),
    Ref1 = async_query(Cluster),
    Cluster2 = join_rest(Cluster, Nodes),
    Ref2 = async_query(Cluster2),
    Results = wait_for([Ref1, Ref2]),
    verify_results(Results),
    %% KeysDeleted = delete_some_data(Cluster2),
    %% verify_deletes(Cluster2),
    pass.

async_query(Cluster) ->
    lager:info("Run async query against cluster ~p", [Cluster]),
    Hosts = host_entries(rt:connection_info(Cluster)),
    Concurrent = length(Hosts),
    %% Cfg = [{mode,{rate,1}},
    %%        {duration, 2},
    %%        {concurrent, Concurrent},
    %%        {driver, rs_bb_driver},
    %%        {rs_search_path, "/search/fruit"},
    %%        {rs_ports, Hosts},
    %%        {pb_ports, []},
    %%        {key_generator, {function, rs_bb_driver, fruit_key_val_gen, []}},
    %%        {operations, [{{exact_search, "apple", "id"}, 1}]}],
    Expect = [{"apple", 100000}],
    Operations = [{{search,E},1} || E <- Expect],
    Cfg = [{mode, max},
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

create_index(Node, Index) ->
    lager:info("Creating index ~s [~p]", [Index, Node]),
    rpc:call(Node, yz_index, create, [Index]).

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
           {key_generator, {function, rs_bb_driver, fruit_key_val_gen, []}},
           {operations, [{load_fruit, 1}]}],
    File = "bb-load-fruit-" ++ ?INDEX_S,
    rt_utils:write_terms(File, Cfg),
    run_bb(sync, File).

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
    ok = install_hook(Node, ?INDEX_B).

verify_results(Results) ->
    [?assertEqual(?SUCCESS, Status) || Status <- Results].

wait_for(Refs) when is_list(Refs) ->
    [wait_for(R) || R <- Refs];
wait_for(Ref) ->
    {Status, Output} = rt:wait_for_cmd(Ref),
    lager:info("Saw status ~p with output ~p", [Status, Output]),
    Status.
