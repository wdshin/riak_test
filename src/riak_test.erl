%% @private
-module(riak_test).
-export([main/1]).

main([]) ->
    X = io:get_chars("[Y/n]", 1),
    io:format("got ~p~n", [X]),
    ok.